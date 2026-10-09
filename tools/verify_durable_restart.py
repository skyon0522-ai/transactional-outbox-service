"""Black-box local process/restart proof, with a loopback-only lost-response injector."""
import datetime
import hashlib
from http.server import BaseHTTPRequestHandler, HTTPServer
import json
from pathlib import Path
import socket
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request

candidate=Path(__file__).resolve().parents[1]
build=candidate/".verification"/sys.argv[1]
assert json.loads((build/"receipt.json").read_text())["status"]=="assembled_unit_tests_and_build_passed"
binary=build/"outbox"
proof=candidate/".verification"/(sys.argv[2] if len(sys.argv)>2 else "durable-1")
proof.mkdir()
producer=proof/"producer.sqlite";receiver=proof/"receiver.sqlite"
steps=[];processes=[];logs=[]

def command(label,*args,expected=0):
 result=subprocess.run([str(binary),*map(str,args)],capture_output=True,timeout=12)
 (proof/(label+".stdout")).open("xb").write(result.stdout);(proof/(label+".stderr")).open("xb").write(result.stderr)
 assert result.returncode==expected,(label,result.returncode,result.stderr.decode())
 record={"stage":label,"command":[str(binary),*map(str,args)],"exit_code":result.returncode,"stdout":result.stdout.decode().strip(),"stderr":result.stderr.decode().strip()}
 steps.append(record);print(json.dumps(record),flush=True)
 return json.loads(result.stdout) if result.stdout else None

def start(label,*args):
 log=(proof/(label+".log")).open("xb");logs.append(log)
 process=subprocess.Popen([str(binary),*map(str,args)],stdout=log,stderr=log)
 processes.append(process);steps.append({"stage":label,"pid":process.pid,"command":[str(binary),*map(str,args)]})
 return process

def wait_for(check,seconds=8):
 end=time.monotonic()+seconds
 while time.monotonic()<end:
  try:
   if check():return
  except (OSError,urllib.error.URLError):pass
  time.sleep(.05)
 raise AssertionError("postcondition timed out")

def port():
 with socket.socket() as sock:
  sock.bind(("127.0.0.1",0));return sock.getsockname()[1]

def inspect(path,role):
 result=subprocess.run([str(binary),"inspect","--db",str(path),"--role",role],capture_output=True,timeout=8)
 assert result.returncode==0,result.stderr.decode()
 return json.loads(result.stdout)

receiver_port=port()
destination=f"http://127.0.0.1:{receiver_port}/orders"
fault_records=[]
failed_delivery_records=[]
delivery_enabled=threading.Event()
class LoseFirstResponse(BaseHTTPRequestHandler):
 def log_message(self,*args):pass
 def do_POST(self):
  body=self.rfile.read(int(self.headers["Content-Length"]))
  if not delivery_enabled.is_set():
   failed_delivery_records.append({"status":503,"event_id":self.headers["X-Event-ID"]})
   time.sleep(.05)
   self.send_response(503);self.send_header("Content-Length","0");self.end_headers();return
  request=urllib.request.Request(destination,data=body,headers={"X-Topic":self.headers["X-Topic"],"X-Event-ID":self.headers["X-Event-ID"],"Content-Type":"application/json"},method="POST")
  with urllib.request.urlopen(request,timeout=3) as response:
   received=response.read();status=response.status
  fault_records.append({"receipt":len(fault_records)+1,"receiver_status":status,"payload_sha256":hashlib.sha256(body).hexdigest(),"response_lost":len(fault_records)==0})
  if len(fault_records)==1:
   self.connection.shutdown(socket.SHUT_RDWR);self.connection.close();return
  self.send_response(status);self.send_header("Content-Length",str(len(received)));self.end_headers();self.wfile.write(received)

injector=HTTPServer(("127.0.0.1",0),LoseFirstResponse)
thread=threading.Thread(target=injector.serve_forever,daemon=True);thread.start()
target=f"http://127.0.0.1:{injector.server_port}/orders"
failed=None
try:
 command("init","init","--db",producer)
 command("rolled-back-produce","produce","--db",producer,"--event","event-rollback","--order","order-rollback","--item","synthetic rice","--quantity","2","--rollback",expected=1)
 state=command("after-rollback","inspect","--db",producer)
 assert state=={"acked":0,"orders":0,"outbox":0},state
 command("committed-produce","produce","--db",producer,"--event","event-1","--order","order-1","--item","synthetic rice","--quantity","2")
 state=command("after-commit","inspect","--db",producer)
 assert state=={"acked":0,"orders":1,"outbox":1},state
 # Observe a rejected HTTP delivery before killing the real relay process.
 relay=start("relay-with-unavailable-receiver","relay","--db",producer,"--target",target)
 wait_for(lambda:len(failed_delivery_records)>0)
 assert relay.poll() is None
 relay.kill();relay.wait(timeout=5)
 state=command("after-relay-kill","inspect","--db",producer)
 assert state=={"acked":0,"orders":1,"outbox":1},state
 service=start("receiver-process","receiver","--db",receiver,"--listen",f"127.0.0.1:{receiver_port}")
 def ready():
  try:urllib.request.urlopen(destination,timeout=.2)
  except urllib.error.HTTPError as error:return error.code==405
  return False
 wait_for(ready)
 delivery_enabled.set()
 relay=start("restarted-relay","relay","--db",producer,"--target",target)
 wait_for(lambda:inspect(producer,"producer")["acked"]==1)
 relay.terminate();relay.wait(timeout=5);service.terminate();service.wait(timeout=5)
 state=command("receiver-after-shutdown","inspect","--db",receiver,"--role","receiver")
 assert state=={"deliveries":2,"effects":1,"receipts":1},state
 assert len(fault_records)==2 and fault_records[0]["response_lost"] and not fault_records[1]["response_lost"],fault_records
 # Restart the receiver process on the same durable file, then reject conflicting bytes.
 service=start("restarted-receiver","receiver","--db",receiver,"--listen",f"127.0.0.1:{receiver_port}")
 wait_for(ready)
 conflict=json.dumps({"event_id":"event-1","order_id":"order-1","item":"synthetic rice","quantity":3},separators=(",",":")).encode()
 request=urllib.request.Request(destination,data=conflict,headers={"X-Topic":"purchase_accepted","X-Event-ID":"event-1"},method="POST")
 try:urllib.request.urlopen(request,timeout=3);raise AssertionError("conflicting ID accepted")
 except urllib.error.HTTPError as error:assert error.code==409;steps.append({"stage":"conflicting-id-after-receiver-restart","status_code":error.code})
 service.terminate();service.wait(timeout=5)
 assert command("receiver-after-conflict","inspect","--db",receiver,"--role","receiver")==state
 assert command("producer-after-restart-delivery","inspect","--db",producer)=={"acked":1,"orders":1,"outbox":1}
except Exception as error:
 failed=repr(error);raise
finally:
 for process in processes:
  if process.poll() is None:process.kill();process.wait(timeout=5)
 for log in logs:log.close()
 injector.shutdown();injector.server_close();thread.join(timeout=3)
 receipt={"at_utc":datetime.datetime.now(datetime.timezone.utc).isoformat(),"status":"failed_preserved" if failed else "durable_process_restart_and_lost_ack_passed","failure":failed,"binary_sha256":hashlib.sha256(binary.read_bytes()).hexdigest(),"steps":steps,"failed_delivery_injection":failed_delivery_records,"lost_ack_injection":fault_records,"database_sha256":{path.name:hashlib.sha256(path.read_bytes()).hexdigest() for path in (producer,receiver) if path.exists()},"public_writes":[]}
 (proof/"receipt.json").open("x").write(json.dumps(receipt,indent=2)+"\n")
