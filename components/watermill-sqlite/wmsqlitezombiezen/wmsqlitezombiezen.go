/*
Package wmsqlitezombiezen provides a high-performance SQLite Pub/Sub driver for Watermill.

The ZombieZen driver abandons the standard Golang library SQL conventions in favor of [the more orthogonal API and higher performance potential](https://crawshaw.io/blog/go-and-sqlite). Under the hood, it uses ModernC SQLite3 implementation and does not rely on CGO. Advanced SQLite users might prefer this driver.

It is about **9 times faster** than the ModernC variant when publishing messages. It is currently more stable due to lower level control. It is faster than even the CGO SQLite variants on standard library interfaces, and with some tuning should become the absolute speed champion of persistent message brokers over time. Tuned SQLite is [~35% faster](https://sqlite.org/fasterthanfs.html) than the Linux file system.

SQLite3 does not support querying `FOR UPDATE`, which is used for row locking when subscribers in the same consumer group read an event batch in official Watermill SQL PubSub implementations. Current architectural decision is to lock a consumer group offset using `unixepoch()+lockTimeout` time stamp. While one consumed message is processing per group, the offset lock time is extended by `lockTimeout` periodically by `time.Ticker`. If the subscriber is unable to finish the consumer group batch, other subscribers will take over the lock as soon as the grace period runs out. A time lock fulfills the role of a traditional database network timeout that terminates transactions when its client disconnects.

All the normal SQLite limitations apply to Watermill. The connections are file handles. Create new connections for concurrent processing. If you must share a connection, protect it with a mutual exclusion lock. If you are writing within a transaction, create a connection for that transaction only.
*/
package wmsqlitezombiezen

import "github.com/ThreeDotsLabs/watermill"

var defaultLogger = watermill.NopLogger{}

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
