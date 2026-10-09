package main

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-sqlite/wmsqlitemodernc"
	"github.com/ThreeDotsLabs/watermill/components/forwarder"
	"github.com/ThreeDotsLabs/watermill/message"
)

func testDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := openDB(filepath.Join(t.TempDir(), "store.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	return db
}
func count(t *testing.T, db *sql.DB, query string) int {
	t.Helper()
	var value int
	if err := db.QueryRow(query).Scan(&value); err != nil {
		t.Fatal(err)
	}
	return value
}

func TestProducerCommitsOrRollsBackBusinessAndOutbox(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	if err := initializeProducer(ctx, db); err != nil {
		t.Fatal(err)
	}
	event := OrderEvent{"event-1", "order-1", "synthetic rice", 2}
	if err := persistDataAndPublishEventInTransaction(ctx, db, event, true); err == nil {
		t.Fatal("expected rollback")
	}
	if count(t, db, "SELECT COUNT(*) FROM orders") != 0 || count(t, db, "SELECT COUNT(*) FROM watermill_purchase_outbox") != 0 {
		t.Fatal("rollback left a partial write")
	}
	if err := persistDataAndPublishEventInTransaction(ctx, db, event, false); err != nil {
		t.Fatal(err)
	}
	if count(t, db, "SELECT COUNT(*) FROM orders") != 1 || count(t, db, "SELECT COUNT(*) FROM watermill_purchase_outbox") != 1 {
		t.Fatal("commit did not retain both writes")
	}
	if err := persistDataAndPublishEventInTransaction(ctx, db, event, false); err == nil {
		t.Fatal("duplicate business identity should fail")
	}
	if count(t, db, "SELECT COUNT(*) FROM watermill_purchase_outbox") != 1 {
		t.Fatal("failed duplicate changed outbox")
	}
	event.EventID = "invalid\nheader"
	if err := persistDataAndPublishEventInTransaction(ctx, db, event, false); err == nil {
		t.Fatal("event IDs must be safe HTTP header values")
	}
}

func TestReceiverDeduplicatesAndRefusesConflictingIdentity(t *testing.T) {
	db := testDB(t)
	if err := initializeReceiver(db); err != nil {
		t.Fatal(err)
	}
	event := OrderEvent{"event-1", "order-1", "synthetic rice", 2}
	payload, _ := json.Marshal(event)
	for i := 0; i < 2; i++ {
		duplicate, err := receive(context.Background(), db, event, payload)
		if err != nil || duplicate != (i == 1) {
			t.Fatalf("delivery %d: duplicate=%v error=%v", i, duplicate, err)
		}
	}
	event.Quantity = 3
	conflict, _ := json.Marshal(event)
	if _, err := receive(context.Background(), db, event, conflict); !errors.Is(err, errConflictingID) {
		t.Fatalf("wanted conflict, got %v", err)
	}
	if count(t, db, "SELECT COUNT(*) FROM fulfillments") != 1 || count(t, db, "SELECT SUM(deliveries) FROM receipts") != 2 {
		t.Fatal("deduplication counts differ")
	}
	// A second event ID for an existing order must roll back its inserted receipt.
	event.EventID = "event-2"
	payload, _ = json.Marshal(event)
	if _, err := receive(context.Background(), db, event, payload); err == nil {
		t.Fatal("expected order identity conflict")
	}
	if count(t, db, "SELECT COUNT(*) FROM receipts") != 1 {
		t.Fatal("failed effect left its receipt")
	}
}

func TestReceiverValidatesRequestAndPayload(t *testing.T) {
	db := testDB(t)
	if err := initializeReceiver(db); err != nil {
		t.Fatal(err)
	}
	handler := receiverHandler(db)
	cases := []struct {
		body, id, topic string
		want            int
	}{
		{`{"event_id":"event-1","order_id":"order-1","item":"rice","quantity":0}`, "event-1", orderTopic, 400},
		{`{"event_id":"event-1","order_id":"order-1","item":"rice","quantity":1}`, "different", orderTopic, 400},
		{`{"event_id":"event-1","order_id":"order-1","item":"rice","quantity":1,"extra":1}`, "event-1", orderTopic, 400},
		{`{} {}`, "event-1", orderTopic, 400},
		{`{}`, "event-1", "unknown", 400},
		{string(bytes.Repeat([]byte("x"), 4097)), "event-1", orderTopic, 400},
	}
	for _, test := range cases {
		req := httptest.NewRequest("POST", "/orders", bytes.NewBufferString(test.body))
		req.Header.Set("X-Event-ID", test.id)
		req.Header.Set("X-Topic", test.topic)
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, req)
		if response.Code != test.want {
			t.Fatalf("status %d wanted %d", response.Code, test.want)
		}
	}
	if count(t, db, "SELECT COUNT(*) FROM receipts") != 0 {
		t.Fatal("invalid request created receipt")
	}
}

// Lose the first response after the receiver has committed. The retry travels
// through the unchanged copied Forwarder and SQLite subscriber, not a test relay.
type loseFirstResponse struct {
	mu   sync.Mutex
	lost bool
	base http.RoundTripper
}

func (transport *loseFirstResponse) RoundTrip(req *http.Request) (*http.Response, error) {
	response, err := transport.base.RoundTrip(req)
	if err != nil {
		return response, err
	}
	transport.mu.Lock()
	defer transport.mu.Unlock()
	if !transport.lost {
		transport.lost = true
		io.Copy(io.Discard, response.Body)
		response.Body.Close()
		return nil, errors.New("synthetic response lost after receiver commit")
	}
	return response, nil
}

func TestForwarderLostAcknowledgmentRepeatsReceiptWithoutRepeatingEffect(t *testing.T) {
	producer := testDB(t)
	receiver := testDB(t)
	ctx := context.Background()
	if err := initializeProducer(ctx, producer); err != nil {
		t.Fatal(err)
	}
	if err := initializeReceiver(receiver); err != nil {
		t.Fatal(err)
	}
	event := OrderEvent{"event-1", "order-1", "synthetic rice", 2}
	if err := persistDataAndPublishEventInTransaction(ctx, producer, event, false); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(receiverHandler(receiver))
	defer server.Close()
	transport := &loseFirstResponse{base: http.DefaultTransport.(*http.Transport).Clone()}
	publisher := &httpPublisher{target: server.URL + "/orders", client: &http.Client{Timeout: time.Second, Transport: transport}}
	sub, err := newTestSubscriber(producer)
	if err != nil {
		t.Fatal(err)
	}
	fwd, err := newTestForwarder(sub, publisher)
	if err != nil {
		t.Fatal(err)
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- fwd.Run(runCtx) }()
	select {
	case <-fwd.Running():
	case <-time.After(3 * time.Second):
		t.Fatal("forwarder did not start")
	}
	deadline := time.Now().Add(5 * time.Second)
	for count(t, producer, "SELECT offset_acked FROM watermill_offsets_purchase_outbox WHERE consumer_group='default'") < 1 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if err := fwd.Close(); err != nil {
		t.Fatal(err)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if count(t, producer, "SELECT offset_acked FROM watermill_offsets_purchase_outbox WHERE consumer_group='default'") != 1 {
		t.Fatal("outbox not acknowledged")
	}
	if count(t, receiver, "SELECT COUNT(*) FROM fulfillments") != 1 || count(t, receiver, "SELECT SUM(deliveries) FROM receipts") != 2 {
		t.Fatal("lost response did not produce two receipts and one effect")
	}
}

func newTestSubscriber(db *sql.DB) (message.Subscriber, error) {
	return wmsqlitemodernc.NewSubscriber(db, wmsqlitemodernc.SubscriberOptions{InitializeSchema: true, BatchSize: 1, PollInterval: 20 * time.Millisecond, LockTimeout: time.Second})
}
func newTestForwarder(sub message.Subscriber, pub message.Publisher) (*forwarder.Forwarder, error) {
	return forwarder.NewForwarder(sub, pub, watermill.NopLogger{}, forwarder.Config{ForwarderTopic: forwarderTopic, CloseTimeout: 3 * time.Second})
}

func TestReceiverRunDrainsActiveRequestOnShutdown(t *testing.T) {
	reservation, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := reservation.Addr().String()
	reservation.Close()
	dbPath := filepath.Join(t.TempDir(), "receiver.sqlite")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- run(ctx, []string{"receiver", "--db", dbPath, "--listen", address}) }()
	var connection net.Conn
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		connection, err = net.DialTimeout("tcp", address, 20*time.Millisecond)
		if err == nil {
			break
		}
		select {
		case err := <-done:
			t.Fatalf("receiver stopped before request: %v", err)
		default:
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	connection.SetDeadline(time.Now().Add(4 * time.Second))
	payload, _ := json.Marshal(OrderEvent{"event-drain", "order-drain", "synthetic rice", 2})
	// A 100 Continue response proves the real handler has started reading its body.
	request := "POST /orders HTTP/1.1\r\nHost: " + address + "\r\nX-Topic: " + orderTopic + "\r\nX-Event-ID: event-drain\r\nExpect: 100-continue\r\nContent-Length: " + strconv.Itoa(len(payload)) + "\r\n\r\n"
	if _, err = io.WriteString(connection, request); err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(connection)
	interim, err := http.ReadResponse(reader, &http.Request{Method: http.MethodPost})
	if err != nil {
		t.Fatal(err)
	}
	interim.Body.Close()
	if interim.StatusCode != http.StatusContinue {
		t.Fatalf("handler did not request the body: %d", interim.StatusCode)
	}
	cancel()
	// Observe listener closure, so the negative completion check occurs during drain.
	deadline = time.Now().Add(time.Second)
	for {
		probe, dialErr := net.DialTimeout("tcp", address, 20*time.Millisecond)
		if dialErr != nil {
			break
		}
		probe.Close()
		if time.Now().After(deadline) {
			t.Fatal("shutdown did not close the listener")
		}
		time.Sleep(10 * time.Millisecond)
	}
	select {
	case err := <-done:
		t.Fatalf("run returned while an active request was incomplete: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	if _, err = connection.Write(payload); err != nil {
		t.Fatal(err)
	}
	response, err := http.ReadResponse(reader, &http.Request{Method: http.MethodPost})
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("active request did not complete: %d", response.StatusCode)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("run did not finish after the request drained")
	}
	db, err := openDB(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if count(t, db, "SELECT COUNT(*) FROM receipts") != 1 || count(t, db, "SELECT COUNT(*) FROM fulfillments") != 1 {
		t.Fatal("drained request did not commit its receipt and effect")
	}
}
