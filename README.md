# Local transactional outbox for synthetic purchase requests

Commit a purchase record and its event in one SQLite transaction, then forward the event from a separate process to a durable, deduplicating local receiver. This is a bounded integration example assembled from actual copied Watermill and Watermill SQLite implementations.

The producer never sends a purchase directly to the receiver. The copied transaction-scoped SQLite Publisher stores the event in the same transaction as `orders`; the copied Watermill Publisher supplies the forwarding envelope; the copied Forwarder and SQLite Subscriber deliver and acknowledge it. The receiver commits its receipt and fulfillment record in a second local transaction.

## Repository map

| Start here | Purpose |
| --- | --- |
| [app/main.go](app/main.go) | Adapted producer, forwarder and local receiver |
| [app/main_test.go](app/main_test.go), [source check](tools/verify_app.py) and [restart check](tools/verify_durable_restart.py) | Contract, source identity and durable restart checks |
| [components/watermill/](components/watermill/) and [components/watermill-sqlite/](components/watermill-sqlite/) | Unchanged running message/SQLite implementations |
| [PROVENANCE.md](PROVENANCE.md) and [adaptation.diff](adaptation.diff) | Source analysis, licenses and exact application changes |
| [VERIFICATION.md](VERIFICATION.md) and [verification-summary.json](verification-summary.json) | Observed results and limits |

The copied Watermill documentation includes site-built and archived routes that do not resolve as ordinary GitHub file links. For complete upstream material, use the pinned [Watermill repository](https://github.com/ThreeDotsLabs/watermill/tree/080d4b4e7fe6105ab880d1becd5cf91af0235d8c) and [Watermill SQLite repository](https://github.com/ThreeDotsLabs/watermill-sqlite/tree/a2a915319684f030296ccb15403b37f292640ad2). The copied source remains unchanged; this root README documents the local assembly.

## Run the local example

Use Go 1.27.2 with the checked-in module manifests and local component replacements. Run the producer sequentially; this example makes no concurrent-writer claim. Commands below assume a POSIX shell from the repository directory.

```sh
cd app
GOTOOLCHAIN=local GOFLAGS=-mod=readonly go test -count=1 -timeout=60s .
GOTOOLCHAIN=local GOFLAGS=-mod=readonly go build -o ../outbox .
cd ..
./outbox init --db producer.sqlite
./outbox produce --db producer.sqlite --event event-1 --order order-1 --item 'synthetic rice' --quantity 2
```

In a second terminal, start the receiver. In a third terminal, start the relay. All HTTP addresses must be numeric loopback addresses.

```sh
./outbox receiver --db receiver.sqlite --listen 127.0.0.1:8099
./outbox relay --db producer.sqlite --target http://127.0.0.1:8099/orders
```

Inspect the files after stopping the processes with Ctrl+C:

```sh
./outbox inspect --db producer.sqlite
./outbox inspect --db receiver.sqlite --role receiver
```

On Ctrl+C or SIGTERM, the receiver closes its listener and waits up to three seconds for active requests to finish before returning. A shutdown error, including an expired drain deadline, is returned to the command instead of being discarded.

For one accepted event, expect producer `orders=1,outbox=1,acked=1` and receiver `receipts=1,effects=1,deliveries=1`. `outbox` is the retained message-log row count; acknowledged rows are not deleted. `acked` is the copied subscriber's durable acknowledged offset for its single default consumer group.

Use a new event/order ID with `--rollback` to request a failure after both writes. The command exits with an error and neither new record remains. Event IDs are 1–80 printable ASCII bytes with no spaces; order IDs are at most 80 bytes, items at most 200 bytes, and quantities are 1–1,000,000. The receiver accepts at most 4,096 bytes and validates the event, topic and matching header identity.

## What is verified

See [VERIFICATION.md](VERIFICATION.md) for observed original → unchanged duplicate → assembled comparisons and durable process evidence. [PROVENANCE.md](PROVENANCE.md) identifies the exact copied implementations, their MIT licenses, the initial copied application source, and the necessary adaptation.

## Limits

This is local synthetic data, with no authentication, external broker, account, cloud service, deployment or load test. It demonstrates at-least-once delivery with a receiver transaction that deduplicates byte-identical repeats. It does not promise exactly-once transport.

Watermill SQLite's upstream README calls the library beta and says it has not been widely tested in production. Its `TestConcurrentTransactions` is explicitly skipped because the one-connection case deadlocks. The app retains `SetMaxOpenConns(1)` and tests one active relay and one producer invocation at a time. These checks do not establish multiwriter, concurrent transaction, high availability, power-loss or production safety.

Conflicting event-ID payloads return HTTP 409 and remain unacknowledged at the relay. There is no automatic correction or dead-letter queue. An unavailable receiver remains retryable; the copied SQLite Subscriber's nack path may retry rapidly. Raw byte equality is deliberate: differently serialized bytes under an existing event ID are a conflict. The append-only message/receipt history has no retention policy.
