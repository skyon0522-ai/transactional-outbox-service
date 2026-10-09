/*
Package wmsqlitemodernc provides a SQLite Pub/Sub driver for Watermill compatible with the Golang standard library SQL package.

It works without CGO.

SQLite3 does not support querying `FOR UPDATE`, which is used for row locking when subscribers in the same consumer group read an event batch in official Watermill SQL PubSub implementations. Current architectural decision is to lock a consumer group offset using `unixepoch()+lockTimeout` time stamp. While one consumed message is processing per group, the offset lock time is extended by `lockTimeout` periodically by `time.Ticker`. If the subscriber is unable to finish the consumer group batch, other subscribers will take over the lock as soon as the grace period runs out. A time lock fulfills the role of a traditional database network timeout that terminates transactions when its client disconnects.

All the normal SQLite limitations apply to Watermill. The connections are file handles. Create new connections for concurrent processing. If you must share a connection, protect it with a mutual exclusion lock. If you are writing within a transaction, create a connection for that transaction only.
*/
package wmsqlitemodernc

import (
	"context"
	"database/sql"

	"github.com/ThreeDotsLabs/watermill"
	_ "modernc.org/sqlite"
)

var defaultLogger = watermill.NopLogger{}

// SQLiteDatabase is an interface that represents an SQLite database.
type SQLiteDatabase interface {
	SQLiteConnection
	BeginTx(context.Context, *sql.TxOptions) (*sql.Tx, error)
}

// SQLiteDatabase is an SQLite database connection or a transaction.
type SQLiteConnection interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	// Close() error
}

// TableNameGenerator creates a table name for a given topic either for
// a topic table or for offsets table.
type TableNameGenerator func(topic string) string

// TableNameGenerators is a struct that holds two functions for generating topic and offsets table names.
// A [Publisher] and a [Subscriber] must use identical generators for topic and offsets tables in order
// to communicate with each other.
type TableNameGenerators struct {
	Topic   TableNameGenerator
	Offsets TableNameGenerator
}

// WithDefaultGeneratorsInsteadOfNils returns a TableNameGenerators with default generators for topic and offsets tables
// if they were left nil.
func (t TableNameGenerators) WithDefaultGeneratorsInsteadOfNils() TableNameGenerators {
	if t.Topic == nil {
		t.Topic = func(topic string) string {
			return "watermill_" + topic
		}
	}
	if t.Offsets == nil {
		t.Offsets = func(topic string) string {
			return "watermill_offsets_" + topic
		}
	}
	return t
}
