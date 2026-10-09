// Adapted from Watermill's MIT transactional-events-forwarder example.
// Attribution and the copied/authored boundary: ../PROVENANCE.md.
package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-sqlite/wmsqlitemodernc"
	"github.com/ThreeDotsLabs/watermill/components/forwarder"
	"github.com/ThreeDotsLabs/watermill/message"
	_ "modernc.org/sqlite"
)

const forwarderTopic = "purchase_outbox"
const orderTopic = "purchase_accepted"

type OrderEvent struct {
	EventID  string `json:"event_id"`
	OrderID  string `json:"order_id"`
	Item     string `json:"item"`
	Quantity int    `json:"quantity"`
}

func (e OrderEvent) validate() error {
	if strings.TrimSpace(e.EventID) == "" || len(e.EventID) > 80 || strings.IndexFunc(e.EventID, func(r rune) bool { return r < 33 || r > 126 }) >= 0 || strings.TrimSpace(e.OrderID) == "" || len(e.OrderID) > 80 || strings.TrimSpace(e.Item) == "" || len(e.Item) > 200 || e.Quantity < 1 || e.Quantity > 1000000 {
		return errors.New("event/order IDs, item and quantity must be valid bounded values")
	}
	return nil
}

// openDB adapts the copied ModernC example's one-connection setup.
func openDB(path string) (*sql.DB, error) {
	if path == "" {
		return nil, errors.New("a database file path is required")
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	// minimalism: one connection and one active writer per role; no multiwriter claim.
	db.SetMaxOpenConns(1)
	if _, err = db.Exec("PRAGMA busy_timeout=5000"); err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

func initializeProducer(ctx context.Context, db *sql.DB) error {
	if _, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS orders (order_id TEXT PRIMARY KEY, event_id TEXT NOT NULL UNIQUE, item TEXT NOT NULL, quantity INTEGER NOT NULL)`); err != nil {
		return err
	}
	// The copied subscriber initializes outbox/offset tables outside the business transaction.
	sub, err := wmsqlitemodernc.NewSubscriber(db, wmsqlitemodernc.SubscriberOptions{InitializeSchema: true})
	if err != nil {
		return err
	}
	setup, cancel := context.WithCancel(ctx)
	_, err = sub.Subscribe(setup, forwarderTopic)
	cancel()
	return errors.Join(err, sub.Close())
}

// Adapted from persistDataAndPublishEventInTransaction in the copied example.
// SQLite writes on the same *sql.Tx; the copied Publisher owns the envelope.
func persistDataAndPublishEventInTransaction(ctx context.Context, db *sql.DB, event OrderEvent, rollback bool) (err error) {
	if err = event.validate(); err != nil {
		return err
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		if cleanup := tx.Rollback(); cleanup != nil && !errors.Is(cleanup, sql.ErrTxDone) {
			err = errors.Join(err, cleanup)
		}
	}()
	_, err = tx.ExecContext(ctx, `INSERT INTO orders (order_id,event_id,item,quantity) VALUES (?,?,?,?)`, event.OrderID, event.EventID, event.Item, event.Quantity)
	if err != nil {
		return err
	}
	var publisher message.Publisher
	publisher, err = wmsqlitemodernc.NewPublisher(tx, wmsqlitemodernc.PublisherOptions{})
	if err != nil {
		return err
	}
	defer publisher.Close()
	publisher = forwarder.NewPublisher(publisher, forwarder.PublisherConfig{ForwarderTopic: forwarderTopic})
	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}
	msg := message.NewMessage(event.EventID, payload)
	msg.SetContext(ctx)
	if err = publisher.Publish(orderTopic, msg); err != nil {
		return err
	}
	if rollback {
		return errors.New("requested rollback after both transaction writes")
	}
	// Necessary correction: the source example discards its deferred Commit error.
	return tx.Commit()
}

func initializeReceiver(db *sql.DB) error {
	_, err := db.Exec(`CREATE TABLE IF NOT EXISTS receipts (event_id TEXT PRIMARY KEY,payload BLOB NOT NULL,deliveries INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS fulfillments (order_id TEXT PRIMARY KEY,event_id TEXT NOT NULL UNIQUE,item TEXT NOT NULL,quantity INTEGER NOT NULL)`)
	return err
}

var errConflictingID = errors.New("event ID already has a different payload")

// The receipt and business effect commit together. Byte-identical repeat deliveries
// increment their count; reuse of an event ID with different bytes is refused.
func receive(ctx context.Context, db *sql.DB, event OrderEvent, payload []byte) (duplicate bool, err error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer func() {
		if cleanup := tx.Rollback(); cleanup != nil && !errors.Is(cleanup, sql.ErrTxDone) {
			err = errors.Join(err, cleanup)
		}
	}()
	var prior []byte
	err = tx.QueryRowContext(ctx, "SELECT payload FROM receipts WHERE event_id=?", event.EventID).Scan(&prior)
	if err == nil {
		if !bytes.Equal(prior, payload) {
			return false, errConflictingID
		}
		if _, err = tx.ExecContext(ctx, "UPDATE receipts SET deliveries=deliveries+1 WHERE event_id=?", event.EventID); err != nil {
			return false, err
		}
		return true, tx.Commit()
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return false, err
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO receipts (event_id,payload,deliveries) VALUES (?,?,1)", event.EventID, payload); err != nil {
		return false, err
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO fulfillments (order_id,event_id,item,quantity) VALUES (?,?,?,?)", event.OrderID, event.EventID, event.Item, event.Quantity); err != nil {
		return false, err
	}
	return false, tx.Commit()
}

func receiverHandler(db *sql.DB) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/orders" {
			http.Error(w, "POST /orders required", http.StatusMethodNotAllowed)
			return
		}
		if r.Header.Get("X-Topic") != orderTopic {
			http.Error(w, "unsupported topic", http.StatusBadRequest)
			return
		}
		payload, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 4096))
		if err != nil {
			http.Error(w, "invalid bounded payload", http.StatusBadRequest)
			return
		}
		var event OrderEvent
		decoder := json.NewDecoder(bytes.NewReader(payload))
		decoder.DisallowUnknownFields()
		if err = decoder.Decode(&event); err != nil {
			http.Error(w, "invalid event JSON", http.StatusBadRequest)
			return
		}
		if decoder.Decode(new(any)) != io.EOF || event.validate() != nil || r.Header.Get("X-Event-ID") != event.EventID {
			http.Error(w, "invalid event identity or values", http.StatusBadRequest)
			return
		}
		duplicate, err := receive(r.Context(), db, event, payload)
		if errors.Is(err, errConflictingID) {
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}
		if err != nil {
			http.Error(w, "receipt transaction failed", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]bool{"duplicate": duplicate})
	})
}

// Destination glue only. The copied Forwarder owns delivery/retry/ack behavior.
type httpPublisher struct {
	target string
	client *http.Client
}

func (p *httpPublisher) Publish(topic string, messages ...*message.Message) error {
	for _, msg := range messages {
		req, err := http.NewRequestWithContext(msg.Context(), http.MethodPost, p.target, bytes.NewReader(msg.Payload))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Event-ID", msg.UUID)
		req.Header.Set("X-Topic", topic)
		response, err := p.client.Do(req)
		if err != nil {
			return err
		}
		_, readErr := io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
		closeErr := response.Body.Close()
		if response.StatusCode != http.StatusOK {
			return fmt.Errorf("receiver returned %d", response.StatusCode)
		}
		if err = errors.Join(readErr, closeErr); err != nil {
			return err
		}
	}
	return nil
}
func (p *httpPublisher) Close() error { p.client.CloseIdleConnections(); return nil }

func loopbackAddress(address string) error {
	host, _, err := net.SplitHostPort(address)
	if err != nil || net.ParseIP(host) == nil || !net.ParseIP(host).IsLoopback() {
		return errors.New("a numeric loopback address and port are required")
	}
	return nil
}

func runRelay(ctx context.Context, db *sql.DB, target string) error {
	u, err := url.Parse(target)
	if err != nil || u.Scheme != "http" || u.User != nil || u.Path != "/orders" || u.RawQuery != "" || u.Fragment != "" {
		return errors.New("target must be an HTTP loopback /orders URL")
	}
	if err = loopbackAddress(u.Host); err != nil {
		return err
	}
	sub, err := wmsqlitemodernc.NewSubscriber(db, wmsqlitemodernc.SubscriberOptions{InitializeSchema: true, BatchSize: 1, PollInterval: 100 * time.Millisecond, LockTimeout: time.Second})
	if err != nil {
		return err
	}
	defer sub.Close()
	publisher := &httpPublisher{target: target, client: &http.Client{Timeout: 2 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return errors.New("redirect refused") }}}
	fwd, err := forwarder.NewForwarder(sub, publisher, watermill.NopLogger{}, forwarder.Config{ForwarderTopic: forwarderTopic, CloseTimeout: 3 * time.Second})
	if err != nil {
		return err
	}
	go func() { <-ctx.Done(); fwd.Close() }()
	return fwd.Run(ctx)
}

func run(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return errors.New("command required: init, produce, receiver, relay, inspect")
	}
	flags := flag.NewFlagSet(args[0], flag.ContinueOnError)
	dbPath := flags.String("db", "", "local SQLite database file")
	listen := flags.String("listen", "127.0.0.1:8099", "receiver loopback address")
	target := flags.String("target", "http://127.0.0.1:8099/orders", "receiver loopback URL")
	eventID := flags.String("event", "", "event ID")
	orderID := flags.String("order", "", "order ID")
	item := flags.String("item", "", "item")
	quantity := flags.Int("quantity", 1, "quantity")
	rollback := flags.Bool("rollback", false, "rollback after writing record and outbox")
	role := flags.String("role", "producer", "inspection role: producer or receiver")
	if err := flags.Parse(args[1:]); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("unexpected positional arguments")
	}
	db, err := openDB(*dbPath)
	if err != nil {
		return err
	}
	defer db.Close()
	switch args[0] {
	case "init":
		return initializeProducer(ctx, db)
	case "produce":
		return persistDataAndPublishEventInTransaction(ctx, db, OrderEvent{*eventID, *orderID, *item, *quantity}, *rollback)
	case "relay":
		return runRelay(ctx, db, *target)
	case "receiver":
		if err = loopbackAddress(*listen); err != nil {
			return err
		}
		if err = initializeReceiver(db); err != nil {
			return err
		}
		server := &http.Server{Addr: *listen, Handler: receiverHandler(db), ReadHeaderTimeout: 2 * time.Second, ReadTimeout: 3 * time.Second, WriteTimeout: 3 * time.Second}
		shutdownDone := make(chan error, 1)
		go func() {
			<-ctx.Done()
			shutdown, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			shutdownDone <- server.Shutdown(shutdown)
		}()
		err = server.ListenAndServe()
		if errors.Is(err, http.ErrServerClosed) {
			return <-shutdownDone
		}
		return err
	case "inspect":
		queries := map[string]string{}
		if *role == "producer" {
			queries = map[string]string{"orders": "SELECT COUNT(*) FROM orders", "outbox": "SELECT COUNT(*) FROM watermill_purchase_outbox", "acked": "SELECT offset_acked FROM watermill_offsets_purchase_outbox WHERE consumer_group='default'"}
		} else if *role == "receiver" {
			queries = map[string]string{"receipts": "SELECT COUNT(*) FROM receipts", "effects": "SELECT COUNT(*) FROM fulfillments", "deliveries": "SELECT COALESCE(SUM(deliveries),0) FROM receipts"}
		} else {
			return errors.New("role must be producer or receiver")
		}
		result := map[string]int{}
		for name, query := range queries {
			var value int
			if err = db.QueryRowContext(ctx, query).Scan(&value); err != nil {
				return err
			}
			result[name] = value
		}
		return json.NewEncoder(os.Stdout).Encode(result)
	default:
		return errors.New("unknown command")
	}
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := run(ctx, os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
