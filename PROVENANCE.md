# Source analysis, duplication and necessary adaptation

| Copied OSS component | Exact source revision | License | Executed responsibility |
| --- | --- | --- | --- |
| Watermill | `080d4b4e7fe6105ab880d1becd5cf91af0235d8c` | MIT, Copyright (c) 2019 Three Dots Labs | Forwarder router, envelope Publisher, message acknowledgment and UUID preservation |
| Watermill SQLite | `a2a915319684f030296ccb15403b37f292640ad2` | MIT, Copyright (c) 2025 Dmitry Kotik | Transaction-scoped publishing, topic schema, persisted message log, subscription locks/offsets and ack/nack redelivery |

The full source trees are in `components/watermill` and `components/watermill-sqlite`; their LICENSE files are retained unchanged. `source-copy-manifest.json` records the archive/file provenance. The 549 Watermill files and 48 SQLite files, including their module manifests, were kept byte-identical to the pinned archives. Local module `replace` directives make these duplicated implementations part of the actual running application.

Inspected references:

- Watermill `_examples/real-world-examples/transactional-events-forwarder/main.go`: its `persistDataAndPublishEventInTransaction` writes a business record and publishes an enveloped event through a transaction-scoped database publisher. The original example uses MySQL, drops a table during setup and requires Google Cloud; that entire example was not executed.
- Watermill `components/forwarder/forwarder.go`, `publisher.go` and envelope tests: these own the forwarding path and transport acknowledgment. The app does not implement a replacement relay core.
- Watermill SQLite `DEVELOPMENT.md` ModernC Go example: `database/sql`, SQLite registration, `SetMaxOpenConns(1)`, Publisher/Subscriber construction and schema initialization. Its `wmsqlitemodernc/transaction_test.go` was inspected and executed, including the same-transaction publishing test. There is no `sample/main.go` file in this pinned SQLite source tree; the reference is its actual documentation example.
- SQLite `publisher.go`, `subscriber.go`, `subscription.go` and `topic.go`: initialization must occur outside the producer transaction; stored offsets and ack/nack behavior are reused directly.

Before adaptation, the complete copied Watermill example `main.go` was copied byte-for-byte into `app/main.go`; [adaptation.json](adaptation.json) records matching SHA-256 values from the initial local copy receipt. It was then adapted into one application file. The exact original-example → final-application textual diff is retained in [adaptation.diff](adaptation.diff), with the final source and diff hashes recorded in the adaptation receipt.

Retained/adapted application sequence: begin transaction → insert the business row → construct the copied publisher on the same transaction → decorate it with the copied Forwarder Publisher → marshal/publish the event → commit. The specific correction is to return the Commit error explicitly; the source example's deferred commit discarded its error. The SQLite publisher is created with schema initialization disabled inside this transaction, as its existing implementation requires.

Authored application glue is limited to the purchase data fields, command interface, local SQLite file setup, loopback HTTP destination adapter, bounded request validation, inspection output, graceful process shutdown, and the receiver's receipt/fulfillment transaction. The authored tests and tools execute copied contracts and the assembled path; the loopback lost-response injector is test-only. No copied Forwarder, envelope, SQLite schema, storage or subscriber implementation was changed.

Independent finding `OUTBOX-SHUTDOWN-1` led to a narrow correction in that shutdown glue: the receiver now waits for `Server.Shutdown` to finish and returns its result, retaining the existing three-second shutdown context. The original review, before-fix hashes and regression evidence remain preserved; current adaptation and verification hashes identify the corrected source.

The root LICENSE carries Watermill's MIT notice for the adapted application source. Watermill SQLite's independent MIT notice remains in `components/watermill-sqlite/LICENSE`. Dependency archives retain their own licenses and are Go-checksum verified. Task-local caches, original comparison trees, binaries and test databases are not source components to publish.

## Research background

Pat Helland, [Life beyond Distributed Transactions: an Apostate's Opinion](https://www.cidrdb.org/cidr2007/papers/cidr07p15.pdf), CIDR 2007, sections on at-least-once messaging and received-message state. This position paper discusses the failure window between a durable update and its acknowledgment, and remembering received messages to make retries harmless. The local example demonstrates that boundary; it does not measure scalability or implement a distributed transaction protocol.
