"""Fresh, append-only checks of pinned duplicated OSS components."""
import datetime
import hashlib
import json
import os
import shutil
from pathlib import Path
import subprocess
import sys
import time

candidate = Path(__file__).resolve().parents[1]
runtime = Path(shutil.which("go") or sys.exit("Go 1.27.2 must be on PATH"))
copies = json.loads((candidate / "source-copy-manifest.json").read_text())
run_name = sys.argv[1] if len(sys.argv) > 1 else "public-components"
verification = candidate / ".verification" / run_name
verification.mkdir(parents=True)
cache = candidate / ".go-cache"
env = {**os.environ, "GOTOOLCHAIN": "local", "GOFLAGS": "-mod=readonly",
       "GOPROXY": os.environ.get("GOPROXY", "https://proxy.golang.org"), "GOSUMDB": "sum.golang.org",
       "GOPATH": os.environ.get("GOPATH", str(cache / "gopath")),
       "GOMODCACHE": os.environ.get("GOMODCACHE", str(cache / "modules")), "GOCACHE": os.environ.get("GOCACHE", str(cache / "build"))}
receipt = {"at_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
           "scope": "Selected contracts from copied component source; no original-tree comparison in this public run",
           "module_proxy": env["GOPROXY"], "checksum_verification": "GOSUMDB=sum.golang.org and preserved upstream go.sum",
           "steps": [], "public_writes": []}
failed = False
for label, tree in (("duplicate", candidate / "components"),):
    workspace = verification / f"{label}.work"
    modules = [tree / "watermill", tree / "watermill-sqlite/wmsqlitemodernc", tree / "watermill-sqlite/test"]
    workspace.write_text("go 1.27.2\n\nuse (\n" + "".join("\t" + json.dumps(str(path)) + "\n" for path in modules) + ")\n")
    for component, directory, selected in (
        ("forwarder", tree / "watermill", ["./components/forwarder"]),
        ("sqlite-transaction", tree / "watermill-sqlite/wmsqlitemodernc",
         ["-run", "^(TestPublishingInTransaction|TestTopicTableCreation)$", "."]),
    ):
        command = [str(runtime), "test", "-json", "-count=1", "-timeout=120s", *selected]
        started = time.monotonic()
        try:
            process = subprocess.run(command, cwd=directory, env={**env, "GOWORK": str(workspace)},
                                     capture_output=True, timeout=480)
            stdout, stderr, code = process.stdout, process.stderr, process.returncode
        except subprocess.TimeoutExpired as error:
            stdout, stderr, code = error.stdout or b"", error.stderr or b"", "timeout"
        name = f"{label}-{component}"
        (verification / f"{name}.jsonl").open("xb").write(stdout)
        (verification / f"{name}.stderr").open("xb").write(stderr)
        events = [json.loads(line) for line in stdout.splitlines() if line.startswith(b"{")]
        entry = {"stage": label, "component": component, "command": command, "exit_code": code,
                 "elapsed_seconds": round(time.monotonic() - started, 2),
                 "passed_tests": [event["Test"] for event in events if event.get("Action") == "pass" and event.get("Test")],
                 "failed_tests": [event["Test"] for event in events if event.get("Action") == "fail" and event.get("Test")],
                 "stdout_sha256": hashlib.sha256(stdout).hexdigest(), "stderr_sha256": hashlib.sha256(stderr).hexdigest()}
        receipt["steps"].append(entry)
        print(json.dumps(entry), flush=True)
        if code != 0:
            failed = True
            break
    if failed:
        break
for item in copies["components"]:
    for label, tree in (("duplicate", candidate / "components"),):
        changed = [entry["path"] for entry in item["files"]
                   if hashlib.sha256((tree / item["name"] / entry["path"]).read_bytes()).hexdigest() != entry["sha256"]]
        receipt.setdefault("source_preservation", []).append({"stage": label, "component": item["name"], "changed": changed})
        failed = failed or bool(changed)
receipt["status"] = "failed_preserved" if failed else "copied_selected_contracts_passed"
(verification / "receipt.json").open("x").write(json.dumps(receipt, indent=2) + "\n")
print(json.dumps({"status": receipt["status"], "step_count": len(receipt["steps"])}), flush=True)
raise SystemExit(1 if failed else 0)
