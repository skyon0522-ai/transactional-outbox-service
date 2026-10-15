"""Run the existing outbox commands with fixed synthetic data, retaining all evidence.

Process orchestration adapts tools/verify_durable_restart.py. This is a demo
runner, not another publisher, subscriber, or message engine.
"""
from __future__ import annotations

import argparse
import datetime
import hashlib
from html import escape
import json
from pathlib import Path
import signal
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request


class DemoError(Exception):
    """A demonstrated postcondition was not observed."""


def report_html(result: dict) -> str:
    rows = "".join(
        f"<tr><th scope='row'>{escape(step['name'])}</th>"
        f"<td>{escape(step['meaning'])}</td><td><pre>"
        f"{escape(json.dumps(step['observed'], sort_keys=True))}</pre></td></tr>"
        for step in result["checks"]
    )
    passed = result["status"] == "passed"
    title = "Demo checks passed" if passed else "Demo incomplete"
    failure = ("" if passed else "<p class='failure'>" + escape(result.get("failure") or "Run did not finish.") + "</p>")
    return f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<meta http-equiv="Content-Security-Policy" content="default-src 'none'; style-src 'unsafe-inline'; base-uri 'none'; form-action 'none'">
<title>{title} · Transactional outbox</title><style>
:root{{--ink:#172c3e;--muted:#516678;--line:#d6e0e6;--brand:#16615c;--bg:#f3f6f8}}
*{{box-sizing:border-box}}body{{margin:0;background:var(--bg);color:var(--ink);font:16px/1.55 system-ui,sans-serif}}
main{{max-width:1040px;margin:auto;padding:40px 28px}}h1{{font-size:clamp(28px,4vw,40px);line-height:1.2}}h2{{font-size:23px}}
.eyebrow{{color:var(--brand);font-weight:700}}.intro{{max-width:76ch;color:var(--muted)}}
section{{padding:24px;margin:24px 0;background:white;border:1px solid var(--line);border-radius:12px}}
pre,code{{white-space:pre-wrap;overflow-wrap:anywhere;font-size:15px}}.table-scroll{{overflow-x:auto}}
table{{width:100%;min-width:640px;border-collapse:collapse;text-align:left}}th,td{{padding:12px;border-bottom:1px solid var(--line);vertical-align:top}}thead{{background:#edf3f5}}
summary{{cursor:pointer;min-height:44px}}summary:focus-visible,.table-scroll:focus-visible,a:focus-visible{{outline:3px solid var(--brand);outline-offset:3px}}a{{color:var(--brand)}}.failure{{color:#9b2424}}
@media(max-width:700px){{main{{padding:24px 16px}}section{{padding:16px}}th,td{{padding:8px}}}}
@media print{{body{{background:white}}main{{padding:0}}section{{break-inside:avoid}}.table-scroll{{overflow:visible}}table{{min-width:0}}}}
</style></head><body><main><header><p class="eyebrow">LOCAL SYNTHETIC DEMO</p>
<h1>{title}</h1><p class="intro">One purchase, two transaction boundaries. The producer stores an order and its outbox event together. A separate relay delivers the event; the receiver stores its receipt and fulfillment together.</p>{failure}</header>
<section aria-labelledby="outcome"><h2 id="outcome">What happened</h2>
<div class="table-scroll" role="region" aria-label="Observed demo checks" tabindex="0"><table><thead><tr><th scope="col">Check</th><th scope="col">Meaning</th><th scope="col">Observed state</th></tr></thead><tbody>{rows}</tbody></table></div>
<p>The duplicate check explicitly replays the same bytes over HTTP after the relay acknowledges delivery. It does not simulate a lost response. The separate restart verification covers that failure mode.</p></section>
<section aria-labelledby="files"><h2 id="files">Keep or inspect the evidence</h2>
<p><a href="result.json">result.json</a> is the machine-readable completion marker: only <code>status: passed</code> means the whole run, including owned-process shutdown, finished. A missing file or any other status is incomplete.</p>
<p><code>producer.sqlite</code> retains the order, message log and acknowledged offset. <code>receiver.sqlite</code> retains the receipt, fulfillment and delivery count. <code>*.stdout</code>, <code>*.stderr</code> and <code>*.log</code> retain command and process output.</p>
<details><summary>Run fingerprints and process cleanup</summary><pre>{escape(json.dumps({key: result.get(key) for key in ('binary_sha256', 'database_sha256', 'cleanup')}, indent=2))}</pre></details></section>
<section aria-labelledby="limits"><h2 id="limits">What this example establishes</h2><p>The observed checks cover one sequential producer and one relay on local SQLite files with fixed synthetic data. Repeated byte-identical delivery creates one fulfillment. This is at-least-once delivery with receiver deduplication; it does not establish exactly-once transport, power-loss recovery, concurrent-writer safety or production readiness.</p>
<p>All child processes use numeric loopback addresses. The runner opens no browser and sends no data to an external service. This file works offline without JavaScript.</p></section>
</main></body></html>"""


class Demo:
    def __init__(self, binary: Path, output: Path):
        self.binary = binary.resolve()
        self.output = output.absolute()
        self.children: list[tuple[str, subprocess.Popen]] = []
        self.logs = []
        self.result = {"schema": "local-outbox-demo/v1", "status": "running", "checks": [], "commands": [], "cleanup": []}

    def command(self, name: str, *args, expected: int = 0):
        process = subprocess.run([str(self.binary), *map(str, args)], capture_output=True, timeout=12)
        for suffix, data in (("stdout", process.stdout), ("stderr", process.stderr)):
            (self.output / f"{name}.{suffix}").write_bytes(data)
        self.result["commands"].append({"name": name, "exit_code": process.returncode, "expected_exit_code": expected})
        if process.returncode != expected:
            raise DemoError(f"{name}: expected exit {expected}, got {process.returncode}; see {name}.stderr")
        return json.loads(process.stdout) if process.stdout else None

    def start(self, name: str, *args):
        log = (self.output / f"{name}.log").open("xb")
        self.logs.append(log)
        child = subprocess.Popen([str(self.binary), *map(str, args)], stdout=log, stderr=log)
        self.children.append((name, child))
        return child

    def stop(self, name: str, child: subprocess.Popen):
        if any(record["name"] == name for record in self.result["cleanup"]):
            return
        unexpected_exit = child.poll() is not None
        forced = False
        if not unexpected_exit:
            child.terminate()
            try:
                child.wait(timeout=6)
            except subprocess.TimeoutExpired:
                child.kill()
                child.wait(timeout=3)
                forced = True
        self.result["cleanup"].append({"name": name, "pid": child.pid, "exit_code": child.returncode, "forced_kill": forced, "unexpected_exit": unexpected_exit})
        if unexpected_exit or forced or child.returncode != 0:
            raise DemoError(f"{name} did not stop cleanly; see {name}.log")

    def inspect(self, path: Path, role: str):
        process = subprocess.run([str(self.binary), "inspect", "--db", str(path), "--role", role], capture_output=True, timeout=8)
        if process.returncode:
            raise DemoError(f"{role} inspection failed")
        return json.loads(process.stdout)

    def wait_for(self, check):
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            if any(child.poll() is not None for _, child in self.children):
                raise DemoError("a demo process exited before its postcondition; inspect *.log")
            try:
                if check():
                    return
            except (OSError, urllib.error.URLError):
                pass
            time.sleep(0.05)
        raise DemoError("postcondition timed out; inspect *.log")

    def check(self, name: str, meaning: str, observed, expected):
        self.result["checks"].append({"name": name, "meaning": meaning, "observed": observed, "expected": expected})
        if observed != expected:
            raise DemoError(f"{name}: observed state differs from the expected state")
        print(f"PASS  {name}", flush=True)

    def execute(self):
        producer = self.output / "producer.sqlite"
        receiver = self.output / "receiver.sqlite"
        self.command("init", "init", "--db", producer)
        self.command("rollback", "produce", "--db", producer, "--event", "event-rollback", "--order", "order-rollback", "--item", "synthetic rice", "--quantity", 2, "--rollback", expected=1)
        self.check("Transaction rollback", "Neither the order nor its event remains.", self.command("after-rollback", "inspect", "--db", producer), {"acked": 0, "orders": 0, "outbox": 0})
        self.command("commit", "produce", "--db", producer, "--event", "event-1", "--order", "order-1", "--item", "synthetic rice", "--quantity", 2)
        self.check("Transaction commit", "Order and outbox event exist before delivery.", self.command("after-commit", "inspect", "--db", producer), {"acked": 0, "orders": 1, "outbox": 1})
        # Same ephemeral loopback-port selection as the existing process probe.
        # A bind race fails this run; it never falls back to a non-loopback target.
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            port = sock.getsockname()[1]
        target = f"http://127.0.0.1:{port}/orders"
        # Avoid ambient HTTP proxies for this loopback-only demo.
        http = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        service = self.start("receiver", "receiver", "--db", receiver, "--listen", f"127.0.0.1:{port}")

        def ready():
            try:
                with http.open(target, timeout=0.3):
                    return False
            except urllib.error.HTTPError as error:
                return error.code == 405

        self.wait_for(ready)
        relay = self.start("relay", "relay", "--db", producer, "--target", target)
        self.wait_for(lambda: self.inspect(producer, "producer")["acked"] == 1)
        self.stop("relay", relay)
        self.check("Acknowledged delivery", "The copied subscriber retains offset 1 after relay shutdown.", self.command("after-delivery", "inspect", "--db", producer), {"acked": 1, "orders": 1, "outbox": 1})
        self.check("First fulfillment", "The receiver committed one receipt and one effect.", self.command("receiver-after-delivery", "inspect", "--db", receiver, "--role", "receiver"), {"deliveries": 1, "effects": 1, "receipts": 1})
        payload = b'{"event_id":"event-1","order_id":"order-1","item":"synthetic rice","quantity":2}'
        request = urllib.request.Request(target, data=payload, headers={"X-Topic": "purchase_accepted", "X-Event-ID": "event-1", "Content-Type": "application/json"}, method="POST")
        with http.open(request, timeout=3) as response:
            self.check("Identical replay", "The receiver identifies the explicit duplicate HTTP request.", {"http_status": response.status, **json.loads(response.read())}, {"http_status": 200, "duplicate": True})
        self.stop("receiver", service)
        self.check("Durable deduplication", "After receiver shutdown: two deliveries, one fulfillment.", self.command("receiver-after-replay", "inspect", "--db", receiver, "--role", "receiver"), {"deliveries": 2, "effects": 1, "receipts": 1})

    def run(self) -> int:
        if not self.binary.is_file():
            raise DemoError("binary does not exist; build ./outbox first or pass --binary")
        # Reserve a new directory before starting any command. No existing output
        # directory is reused, removed or traversed for cleanup.
        self.output.mkdir(parents=True, exist_ok=False)
        marker = self.output / "result.json"
        marker.write_text(json.dumps(self.result, indent=2) + "\n", encoding="utf-8")
        failure = None
        try:
            self.result["binary_sha256"] = hashlib.sha256(self.binary.read_bytes()).hexdigest()
            self.execute()
        except (Exception, KeyboardInterrupt) as error:
            failure = f"{type(error).__name__}: {error}"
        finally:
            for name, child in reversed(self.children):
                try:
                    self.stop(name, child)
                except Exception as error:
                    failure = failure or f"{type(error).__name__}: {error}"
            for log in self.logs:
                log.close()
        self.result.update(status="failed" if failure else "passed", failure=failure, finished_utc=datetime.datetime.now(datetime.timezone.utc).isoformat())
        self.result["database_sha256"] = {path.name: hashlib.sha256(path.read_bytes()).hexdigest() for path in (self.output / "producer.sqlite", self.output / "receiver.sqlite") if path.is_file()}
        try:
            (self.output / "report.html").write_text(report_html(self.result), encoding="utf-8")
        except Exception as error:
            self.result.update(status="failed", failure=f"report write failed: {error}")
        # Replace only our own running marker, after checks, cleanup and report.
        final = self.output / "result.final.json"
        final.write_text(json.dumps(self.result, indent=2) + "\n", encoding="utf-8")
        final.replace(marker)
        print(f"Result: {self.result['status']}\nReport: {self.output / 'report.html'}\nReceipt: {marker}", flush=True)
        return 0 if self.result["status"] == "passed" else 1


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Run a local synthetic outbox demo; retain a new output directory.")
    parser.add_argument("--binary", type=Path, default=Path("outbox"), help="built outbox executable (default: ./outbox)")
    parser.add_argument("--output-dir", type=Path, required=True, help="new directory; existing paths are refused")
    args = parser.parse_args(argv)
    def interrupted(*_):
        raise KeyboardInterrupt("SIGTERM")

    previous = signal.signal(signal.SIGTERM, interrupted)
    try:
        return Demo(args.binary, args.output_dir).run()
    except (OSError, DemoError) as error:
        print(f"Demo failed: {error}", file=sys.stderr)
        return 1
    finally:
        signal.signal(signal.SIGTERM, previous)


if __name__ == "__main__":
    raise SystemExit(main())
