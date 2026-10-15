# Demo design references

Reader: a developer trying the local outbox example on Linux. Intended use:
build, run from one terminal, inspect the observed transaction/delivery states,
then move to individual commands. Voice: short, concrete technical English.

Inspected references:

- `tools/verify_durable_restart.py`: reused owned child processes, fresh SQLite
  files, ephemeral numeric loopback addresses, bounded waits, exact state checks,
  retained logs and machine-readable outcomes. The demo omits fault injection and
  instead labels its explicit identical HTTP replay. The restart probe remains
  the source for lost-response/restart verification.
- `components/watermill/_examples/real-world-examples/transactional-events-forwarder/main.go`:
  retained the transaction-scoped publish followed by a separate Forwarder. The
  runner invokes the existing adaptation rather than implementing messaging.
- The inspected sibling Ops Daily Report README: adopted build/try/output/manual
  documentation/limits order, a new run directory on repeat, and an actual-output
  screenshot hook rather than a fabricated preview.
- The inspected sibling PII Redaction Review `examples/review.html`: reused the
  semantic offline report structure, restrained ink/teal palette, 16px body text,
  horizontally scrollable comparison table, keyboard focus, optional fingerprint
  details, print rules and restrictive content-security policy. No JavaScript,
  motion or network assets are needed for this result.

Editable sources: `tools/run_demo.py`, root README and `docs/operations.md`.
The report is generated from observed state; dynamic text is HTML-escaped.
`result.json` is the completion marker and stays `running` until checks, process
cleanup and report creation finish. Copied source and license bytes are preserved.
Parent review records the actual desktop/mobile visual checks separately.
