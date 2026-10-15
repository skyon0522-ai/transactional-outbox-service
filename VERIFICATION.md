# Observed verification

## Linux demo candidate: 2026-10-15

A fresh native Linux working copy outside the staged checkout passed the public
build and demo commands with Go 1.27.2 and Python 3. The Go build cache started
empty; the existing module-download cache was reused. `-count=1` disabled Go
test-result reuse, and `go mod verify` passed. This run does not demonstrate a
fresh dependency download. No global runtime was installed.

| Executed check | Observed result |
| --- | --- |
| `python3 -B -m unittest discover -s tools -p 'test_run_demo.py' -v` | 7 runner safety tests passed |
| `python3 -B tools/verify_components.py native-components` | 3 top-level Forwarder tests with 3 subtests, and 2 selected SQLite tests passed |
| `python3 -B tools/verify_app.py native-app` | 5 app integration tests, module verification, dependency paths and build passed |
| README build, then `python3 -B tools/run_demo.py --output-dir demo-runs/first` | All 6 checks passed; relay and receiver exited 0 without forced termination |
| Repeat the same demo output path | Exit 1; all 22 retained files unchanged |
| Demo with `/bin/false` as its executable | Exit 1; retained `failed` receipt and incomplete report |
| `python3 -B tools/verify_durable_restart.py native-app native-durable` | Durable restart, lost response, duplicate receipt and conflicting-ID checks passed |
| Source preservation before and after | All 549 Watermill and 48 SQLite files unchanged; app source, tests and module manifests unchanged |

The runner safety tests cover an existing directory and symlink, failure after
starting an owned child, an unexpected clean child exit, report-writing failure,
an incomplete final receipt write and HTML escaping. The final JSON completion
marker is written only after observed checks, owned-process cleanup and report
creation. A `running`, `failed` or missing receipt remains incomplete even if a
report file exists.

The generated [example report](examples/demo/report.html) and
[result](examples/demo/result.json) retain this synthetic run without personal
paths. Full local command logs, databases, hashes and cache evidence are kept
outside the public tree. The copied implementation and existing restart probe
are unchanged. This candidate's updated workflow has not run remotely; visual
and independent review are recorded separately from the execution checks.

## Earlier application evidence: 2026-10-09

The recorded local run used Go 1.27.2 from its official hash-verified archive. The component and app checks used `GOTOOLCHAIN=local`, `GOFLAGS=-mod=readonly`, task-local module/build caches, and `GOSUMDB=sum.golang.org`. Only the authored application's `go.mod`/`go.sum` were provisioned by an explicit `go mod tidy`; upstream manifests stayed unchanged.

## Original → unchanged duplicate → assembled application

| Stage | Executed contract | Observed result |
| --- | --- | --- |
| Original Watermill | `go test -json -count=1 -timeout=120s ./components/forwarder` | Passed: 3 top-level tests plus 3 passing subtest events |
| Duplicated Watermill | Same command on `components/watermill` | Same passing contracts; copied source unchanged |
| Original SQLite | `go test -json -count=1 -timeout=120s -run '^(TestPublishingInTransaction\|TestTopicTableCreation)$' .` | Both selected tests passed |
| Duplicated SQLite | Same command on `components/watermill-sqlite/wmsqlitemodernc` | Both selected tests passed; copied source unchanged |
| Final assembled app | `go test -json -count=1 -timeout=60s .` from `app` | 5 top-level integration tests passed, including the active-request shutdown regression |
| Final assembled modules | `go mod verify` | `all modules verified` |
| Final executable | `go build -o <task-local binary> .` | Exit code 0 |

Component comparisons used separate workspaces containing the original or duplicate Watermill, ModernC SQLite and SQLite test modules. The compact recorded comparison is in `verification-summary.json`; machine-specific command logs are excluded from this repository. The original and duplicate preservation checks returned no changed file paths for either source component.

The recorded application results and source hashes are in `verification-summary.json`. `go list -json -deps .` confirms that the running Forwarder package comes from `components/watermill/components/forwarder`, and the running SQLite package from `components/watermill-sqlite/wmsqlitemodernc`. These are actual executed source copies. The app does not merely reference an upstream import while running a replacement relay implementation.

The five assembled tests verify:

1. The order row and copied-publisher outbox event both commit, both roll back on requested failure, and duplicate business identity does not append another event. Invalid event-ID header values are refused before writes.
2. Repeated identical receipts create one fulfillment; a conflicting event-ID payload is refused. A second event ID for an existing order rolls back its newly inserted receipt when fulfillment insertion fails.
3. Invalid quantity, mismatched event ID, unknown fields, trailing JSON, unsupported topic and oversized payload create no receipt.
4. The actual copied Forwarder and SQLite subscriber retry after the first HTTP response is lost after the receiver commits: two deliveries, one receipt identity, one fulfillment and an acknowledged outbox offset.
5. The real receiver command keeps an active POST alive after cancellation and observed listener closure, waits while its body is incomplete, returns HTTP 200 after the body is completed, and retains its receipt and fulfillment before returning from `run`.

## Durable black-box process evidence

The corrected program was executed in separate producer, relay and receiver processes against new file databases. Its final state is recorded in `verification-summary.json`. New public-procedure runs generate their own ignored receipts and databases.

| Observed step | Producer state | Receiver state |
| --- | --- | --- |
| Requested rollback after both writes | orders 0; outbox 0; acked 0 | Not started |
| Committed purchase | orders 1; outbox 1; acked 0 | Not started |
| Actual HTTP 503 delivery failure, then relay SIGKILL | orders 1; outbox 1; acked 0 | Not started |
| New relay process delivers through test-only lost-response injector | orders 1; outbox 1; acked 1 | receipts 1; effects 1; deliveries 2 |
| Receiver stopped, restarted on the same file, conflicting payload submitted | unchanged | HTTP 409; receipts 1; effects 1; deliveries 2 |

The fault injector is a bounded loopback HTTP test server, not a production transport or relay. It records an HTTP 503 while delivery is unavailable, then forwards to the real receiver process. It closes the first successful response connection after reading the committed receiver's HTTP 200 response, and returns the next response normally. Both successful request payload hashes match. Separate process IDs, command exit codes, durable database hashes and the exact two successful receiver responses are recorded.

This evidence establishes the demonstrated local restart and deduplication behavior. It does not establish exactly-once transport, power-loss safety or concurrent-writer safety. Upstream SQLite's beta warning and explicitly skipped concurrent transaction test remain applicable.

## Independent shutdown finding and correction

Independent review `go-source-independent-review-20261009T102442Z` found P2 `OUTBOX-SHUTDOWN-1`: the receiver returned as soon as its listener closed, while its separate Shutdown goroutine was still draining active requests. The correction waits for that goroutine's result and returns a shutdown error when present. The existing three-second shutdown context and Ctrl+C/SIGTERM handling are retained.

The focused regression used an actual TCP request and HTTP 100 Continue to observe the real handler reading an incomplete body. After shutdown closed the listener, the test checked that `run` was still waiting, completed the body, and required HTTP 200 plus one durable receipt and fulfillment. A Go overlay applied the new regression to the preserved reviewed source without editing it: the old source failed in 0.03 seconds with `run returned while an active request was incomplete`, and the corrected source passed in 0.16 seconds. The local before/after records were retained separately. An independent process probe also kept the partial request alive after SIGTERM, completed it with HTTP 200, committed one receipt and fulfillment, and observed a clean exit. The three-second timeout branch was not independently probed.

## Reproduce the copied and assembled checks

Use Go 1.27.2 and Python 3 on Linux. From the repository directory, run:

```sh
python3 tools/verify_components.py public-components
python3 tools/verify_app.py public-app
python3 tools/verify_durable_restart.py public-app public-durable
```

Use fresh labels for another run. The helpers derive from the candidate's executed verification tools. The public component helper checks the copied sources and two selected SQLite tests; it does not repeat the historical original-tree comparison. The application helper runs the pinned module checks and builds without rewriting source or module manifests. Go downloads normal dependencies through its standard checksum-verified module route. The durable helper starts and stops separate loopback-only processes against new synthetic databases.

The staged public procedure passed both selected SQLite tests, the Forwarder contracts, all five app tests, module verification, build and a fresh durable restart/lost-response run. This run reused the existing checksum-verified dependency cache; a new dependency download was not tested. The workflow in `.github/workflows/checks.yml` uses this procedure. That original verification snapshot did not include a remote CI run.
