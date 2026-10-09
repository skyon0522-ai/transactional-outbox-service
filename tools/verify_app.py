"""Verify the application with read-only pinned modules and copied sources."""
import datetime
import difflib
import hashlib
import json
import os
import shutil
from pathlib import Path
import subprocess
import sys

candidate=Path(__file__).resolve().parents[1]
runtime=Path(shutil.which("go") or sys.exit("Go 1.27.2 must be on PATH"))
proof=candidate/".verification"/(sys.argv[1] if len(sys.argv)>1 else "assembled-1")
proof.mkdir(parents=True)
env={**os.environ,"GOTOOLCHAIN":"local","GOFLAGS":"-mod=readonly","GOWORK":"off","GOPROXY":os.environ.get("GOPROXY","https://proxy.golang.org"),"GOSUMDB":"sum.golang.org","GOPATH":os.environ.get("GOPATH",str(candidate/".go-cache/gopath")),"GOMODCACHE":os.environ.get("GOMODCACHE",str(candidate/".go-cache/modules")),"GOCACHE":os.environ.get("GOCACHE",str(candidate/".go-cache/build"))}
steps=[]
commands=[("tests",[str(runtime),"test","-json","-count=1","-timeout=60s","."]),("module-verification",[str(runtime),"mod","verify"]),("runtime-dependency-paths",[str(runtime),"list","-json","-deps","."]),("build",[str(runtime),"build","-o",str(proof/"outbox"),"."])]
failed=False
for name,command in commands:
 process=subprocess.run(command,cwd=candidate/"app",env=env,capture_output=True,timeout=240)
 (proof/(name+".stdout")).open("xb").write(process.stdout);(proof/(name+".stderr")).open("xb").write(process.stderr)
 entry={"stage":name,"command":command,"exit_code":process.returncode,"stdout_sha256":hashlib.sha256(process.stdout).hexdigest(),"stderr_sha256":hashlib.sha256(process.stderr).hexdigest()}
 if name=="tests":
  events=[json.loads(x) for x in process.stdout.splitlines() if x.startswith(b"{")]
  entry["passed_tests"]=[x["Test"] for x in events if x.get("Action")=="pass" and x.get("Test")]
 steps.append(entry);print(json.dumps(entry),flush=True)
 if process.returncode:
  print(process.stderr.decode()[-3000:]+process.stdout.decode()[-3000:],flush=True);failed=True;break
source=candidate/"components/watermill/_examples/real-world-examples/transactional-events-forwarder/main.go"
adapted=candidate/"app/main.go"
(proof/"adaptation.diff").write_text("".join(difflib.unified_diff(source.read_text().splitlines(keepends=True),adapted.read_text().splitlines(keepends=True),fromfile="copied-watermill-transactional-example/main.go",tofile="app/main.go")))
receipt={"at_utc":datetime.datetime.now(datetime.timezone.utc).isoformat(),"status":"failed_preserved" if failed else "assembled_unit_tests_and_build_passed","steps":steps,"adapted_sha256":hashlib.sha256(adapted.read_bytes()).hexdigest(),"public_writes":[]}
copies=json.loads((candidate/"source-copy-manifest.json").read_text())
for item in copies["components"]:
 for label,tree in (("duplicate",candidate/"components"),):
  changed=[x["path"] for x in item["files"] if hashlib.sha256((tree/item["name"]/x["path"]).read_bytes()).hexdigest()!=x["sha256"]]
  receipt.setdefault("source_preservation",[]).append({"stage":label,"component":item["name"],"changed":changed})
  if changed:failed=True;receipt["status"]="failed_source_preservation"
(proof/"receipt.json").open("x").write(json.dumps(receipt,indent=2)+"\n")
raise SystemExit(1 if failed else 0)
