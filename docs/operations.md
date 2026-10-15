# Manual operation on Linux

Build `./outbox` using the root README. These commands use fresh synthetic
database paths from the repository directory. Unlike the demo runner, the
individual commands can open an existing database: choose those paths deliberately.

```sh
mkdir manual-run
./outbox init --db manual-run/producer.sqlite
./outbox produce --db manual-run/producer.sqlite --event event-1 --order order-1 --item 'synthetic rice' --quantity 2
```

In a second terminal, start the receiver. In a third terminal, start the relay.
All HTTP addresses must be numeric loopback addresses.

```sh
./outbox receiver --db manual-run/receiver.sqlite --listen 127.0.0.1:8099
./outbox relay --db manual-run/producer.sqlite --target http://127.0.0.1:8099/orders
```

After delivery, stop both long-running processes with Ctrl+C and inspect:

```sh
./outbox inspect --db manual-run/producer.sqlite
./outbox inspect --db manual-run/receiver.sqlite --role receiver
```

Expect producer `orders=1,outbox=1,acked=1` and receiver
`receipts=1,effects=1,deliveries=1`. `outbox` counts retained message-log rows;
acknowledged rows are not deleted. `acked` is the copied subscriber's durable
acknowledged offset for its single default consumer group.

To try rollback, use new event/order IDs and add `--rollback` to `produce`.
That command exits 1 after both writes; neither new record remains.

## Inputs and shutdown

Event IDs are 1–80 printable ASCII bytes with no spaces; order IDs are at most
80 bytes, items at most 200 bytes, and quantities are 1–1,000,000. The receiver
accepts at most 4,096 bytes and validates event, topic and matching header identity.

On Ctrl+C or SIGTERM, the receiver closes its listener and waits up to three
seconds for active requests. Shutdown errors, including an expired drain
deadline, are returned by the command. The demo waits for owned children and
records a failed run if a child needs forced termination or exits unexpectedly.

## If a demo fails

- **Missing executable:** build from `app`, or pass `--binary /path/to/outbox`.
- **Output directory already exists:** choose a fresh `--output-dir`; the runner
  never overwrites, deletes or resumes an earlier run.
- **Process exit or timeout:** read `result.json`, then the named `*.stderr` or
  `*.log`. An ephemeral loopback-port bind race fails the run; repeat in a new
  directory. Ambient HTTP proxies are disabled for the runner's replay/readiness
  requests.
- **Interrupted run:** keep the output for inspection. SIGINT and SIGTERM trigger
  owned-child cleanup; an uncatchable SIGKILL or host failure cannot guarantee it.
  Check the recorded child PIDs before stopping anything manually. A missing
  receipt or `running`/`failed` status is incomplete even if HTML exists.

The runner does not remove its output. Keep or delete your chosen run directory
yourself after reviewing it. Never infer success from an HTML file alone.
