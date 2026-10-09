package wmsqlitemodernc

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/google/uuid"
)

func TestExpiringKeyRepository(t *testing.T) {
	// TODO: replace with t.Context() after Watermill bumps to Golang 1.24
	ctx, cancel := context.WithCancel(context.TODO())
	t.Cleanup(cancel)

	db := newTestConnection(t, "file:"+uuid.New().String()+"?mode=memory&journal_mode=WAL&busy_timeout=1000&secure_delete=true&foreign_keys=true&cache=shared")
	r, err := NewExpiringKeyRepository(ctx, ExpiringKeyRepositoryConfiguration{
		Database:      db,
		CleanUpLogger: watermill.NewSlogLogger(slog.Default()),
	})
	if err != nil {
		t.Fatal(err)
	}

	isDuplicate, err := r.IsDuplicate(ctx, "test_key")
	if err != nil {
		t.Fatal(err)
	}
	if isDuplicate {
		t.Fatal("key should not be duplicate")
	}
	isDuplicate, err = r.IsDuplicate(ctx, "test_key")
	if err != nil {
		t.Fatal(err)
	}
	if !isDuplicate {
		t.Fatal("key should be duplicate")
	}

	if err = r.(interface {
		CleanUp(context.Context, time.Time) error
	}).CleanUp(ctx, time.Time{}); err != nil {
		t.Fatal("clean up routine failed:", err)
	}
}
