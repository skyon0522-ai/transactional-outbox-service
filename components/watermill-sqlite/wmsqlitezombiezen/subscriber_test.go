package wmsqlitezombiezen

import (
	"context"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/google/uuid"
)

func TestBasicSendRecieve(t *testing.T) {
	topic := "test-basic-publish-subscribe"
	pub, sub := NewEphemeralDB(t)(t, DefaultConsumerGroupName)
	t.Run("publish 20 messages", func(t *testing.T) {
		t.Parallel()

		for i := 0; i < 10; i++ {
			msg := message.NewMessage(uuid.New().String(), []byte("test"))
			msg2 := message.NewMessage(uuid.New().String(), []byte("test"))
			if err := pub.Publish(topic, msg, msg2); err != nil {
				t.Fatal(err)
			}
		}
	})

	t.Run("collect 20 messages", func(t *testing.T) {
		t.Parallel()
		// TODO: replace with t.Context() after Watermill bumps to Golang 1.24
		ctx, cancel := context.WithTimeout(context.TODO(), time.Second*5)
		defer cancel()

		msgs, err := sub.Subscribe(ctx, topic)
		if err != nil {
			t.Fatal(err)
		}
		if msgs == nil {
			t.Fatal("messages is nil")
		}

		select {
		case <-time.After(time.Second):
			t.Fatal("first message took longer than a second")
		case msg := <-msgs:
			if msg == nil {
				t.Fatal("message is nil")
			}
			msg.Ack()
			if string(msg.Payload) != "test" {
				t.Fatalf("unexpected message payload: %s", string(msg.Payload))
			}
		}

		end := time.After(time.Second * 4)
		processed := 0

	loop:
		for {
			select {
			case <-end:
				break loop
			case msg := <-msgs:
				if msg == nil {
					t.Fatal("message is nil")
				}
				msg.Ack()
				if string(msg.Payload) != "test" {
					t.Fatalf("unexpected message payload: %s", string(msg.Payload))
				}
				processed++
			}
		}
		if processed != 19 {
			t.Fatalf("expected 19 messages, got %d", processed)
		}
	})
}
