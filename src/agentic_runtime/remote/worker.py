from __future__ import annotations

import json
import base64
import os

from agentic_runtime.remote.config import setting
import platform
import shutil
import signal
import ssl
import struct
import subprocess
import threading
import time
import uuid
import urllib.error
from pathlib import Path
from typing import Any, Mapping

from .protocol import (MAX_ARTIFACT_BYTES, RemoteClientError, RemoteWorkerExecutionAdapter,
    WorkerIdentity, build_worker_tls_context, digest_bytes)


DEFAULT_RESOURCE_POLICY={"cpu_seconds":10,"memory_bytes":256*1024*1024,"process_count":32,
    "wall_time_seconds":20,"max_sandbox_bytes":32*1024*1024}


class BwrapSandboxRunner:
    """Worker-local private filesystem view and bounded subprocess tree."""

    def __init__(self, *, bwrap: str="bwrap", python: str="/usr/bin/python3") -> None:
        self.bwrap=bwrap; self.python=python

    def execute_json(self, workspace: Path, payload: Mapping[str,Any], policy: Mapping[str,int],
                     authority_lost: threading.Event | None=None,
                     cancel_requested: threading.Event | None=None) -> bytes:
        workspace=workspace.resolve(strict=True)
        input_path=workspace/"input.json"; output_path=workspace/"result.json"
        input_path.write_text(json.dumps(dict(payload),sort_keys=True),encoding="utf-8")
        output_path.touch(mode=0o600,exist_ok=True)
        script="""import json,os,time
d=json.load(open('/workspace/input.json'))
v={'ok':True,'value':d.get('value')}
if d.get('sleep_seconds'):
    time.sleep(float(d['sleep_seconds']))
if d.get('spawn_tree_seconds'):
    import subprocess,sys
    duration=min(float(d['spawn_tree_seconds']),20.0)
    tree_script=("import json,os,subprocess,sys,time; "
        "g=subprocess.Popen([sys.executable,'-c','import time; time.sleep(30)']); "
        "open('/workspace/result.json','w').write(json.dumps({'ok':True,'value':'tree-running',"
        "'child_pid':os.getpid(),'grandchild_pid':g.pid})); time.sleep(30)")
    subprocess.Popen([sys.executable,'-c',tree_script])
    time.sleep(duration)
if d.get('probe_path'):
    try:
        open(d['probe_path'],'rb').read(1)
        v['cross_sandbox_access']=True
    except OSError:
        v['cross_sandbox_access']=False
if d.get('probe_network'):
    import socket
    try:
        socket.socket().connect(('127.0.0.1',9))
        v['network_blocked']=False
    except OSError:
        v['network_blocked']=True
open('/workspace/result.json','w').write(json.dumps(v,sort_keys=True,separators=(',',':')))
"""
        seccomp_fd=self._network_filter()
        args=[self.bwrap,"--die-with-parent","--unshare-user","--unshare-pid","--unshare-ipc","--unshare-uts",
            "--ro-bind","/usr","/usr"]
        for path in ("/lib","/lib64"):
            if Path(path).exists(): args += ["--ro-bind",path,path]
        python=Path(self.python).resolve()
        args += ["--proc","/proc","--dev","/dev","--tmpfs","/tmp","--size",str(policy["max_sandbox_bytes"]),
            "--tmpfs","/workspace","--ro-bind",str(input_path),"/workspace/input.json",
            "--bind",str(output_path),"/workspace/result.json","--seccomp",str(seccomp_fd),
            "--chdir","/workspace","--","/usr/bin/prlimit",
            f"--cpu={policy['cpu_seconds']}:{policy['cpu_seconds']+1}",
            f"--as={policy['memory_bytes']}:{policy['memory_bytes']}",
            f"--nproc={policy['process_count']}:{policy['process_count']}",
            f"--fsize={MAX_ARTIFACT_BYTES}:{MAX_ARTIFACT_BYTES}","--nofile=64:64","--core=0:0","--",
            str(python),"-c",script]
        process=subprocess.Popen(args,stdin=subprocess.DEVNULL,stdout=subprocess.PIPE,stderr=subprocess.PIPE,
            env={"PATH":"/usr/bin:/bin","LANG":"C.UTF-8"},start_new_session=True,pass_fds=(seccomp_fd,))
        os.close(seccomp_fd)
        started=time.monotonic()
        try:
            while process.poll() is None:
                if cancel_requested and cancel_requested.is_set():
                    raise PermissionError("remote cancellation requested")
                if authority_lost and authority_lost.is_set():
                    raise PermissionError("remote lease authority lost")
                if time.monotonic()-started>policy["wall_time_seconds"]:
                    raise TimeoutError("worker-local execution deadline exceeded")
                time.sleep(.05)
            stdout,stderr=process.communicate()
            if process.returncode:
                detail=stderr.decode(errors="replace")[:1000]
                raise RuntimeError(f"sandbox execution failed with status {process.returncode}: {detail}")
            total=sum(p.stat().st_size for p in workspace.rglob("*") if p.is_file())
            if total>policy["max_sandbox_bytes"]:
                raise OSError("sandbox output exceeds configured disk budget")
            content=output_path.read_bytes()
            if len(content)>MAX_ARTIFACT_BYTES:
                raise OSError("artifact exceeds transfer limit")
            return content
        except BaseException:
            if process.poll() is None:
                try: os.killpg(process.pid,signal.SIGTERM)
                except ProcessLookupError: pass
                try: process.wait(timeout=.5)
                except subprocess.TimeoutExpired:
                    try: os.killpg(process.pid,signal.SIGKILL)
                    except ProcessLookupError: pass
                    process.wait(timeout=2)
            raise

    @staticmethod
    def _network_filter() -> int:
        """Create a fail-closed x86_64 seccomp filter for network syscalls."""
        if platform.machine() != "x86_64":
            raise RuntimeError("sandbox network filter has no verified syscall map for this architecture")
        # seccomp_data.arch must be AUDIT_ARCH_X86_64; unknown ABIs are killed.
        instructions=[(0x20,0,0,4),(0x15,1,0,0xC000003E),(0x06,0,0,0x80000000),
            (0x20,0,0,0),(0x45,0,1,0x40000000),(0x06,0,0,0x80000000)]
        network_syscalls=(41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,288,299,307,425)
        for number in network_syscalls:
            instructions.extend(((0x15,0,1,number),(0x06,0,0,0x00050000|1)))
        instructions.append((0x06,0,0,0x7fff0000))
        fd=os.memfd_create("agentic-worker-seccomp",0)
        os.write(fd,b"".join(struct.pack("=HBBI",*item) for item in instructions))
        os.lseek(fd,0,os.SEEK_SET)
        return fd


class RemoteWorker:
    def __init__(self, *, endpoint: str, worker_id: str, token: str,
                 root: Path, capabilities: list[str], task_types: list[str],
                 resource_classes: list[str], resource_policy: Mapping[str,int],
                 allowed_tools: list[str] | None=None,
                 worker_class: str="REMOTE_PERSISTENT", tls_context: ssl.SSLContext | None=None,
                 poll_seconds: float=.2, lease_heartbeat_seconds: float=1.0,
                 cognitive_adapters: Mapping[str,Any] | None=None) -> None:
        if worker_class not in {"LOCAL","REMOTE_PERSISTENT","REMOTE_EPHEMERAL"}:
            raise ValueError("invalid worker class")
        self.identity=WorkerIdentity.new_instance(worker_id,token)
        self.client=RemoteWorkerExecutionAdapter(endpoint,self.identity,tls_context=tls_context)
        self.root=Path(root).resolve(); self.root.mkdir(parents=True,exist_ok=True,mode=0o700)
        os.chmod(self.root,0o700)
        self.tombstones=self.root/".reconciled"
        self.tombstones.mkdir(mode=0o700,exist_ok=True)
        self.capabilities=capabilities; self.task_types=task_types; self.resource_classes=resource_classes
        self.allowed_tools=list(allowed_tools or [])
        self.worker_class=worker_class
        self.resource_policy=dict(resource_policy); self.poll_seconds=poll_seconds
        self.lease_heartbeat_seconds=lease_heartbeat_seconds
        self.cognitive_adapters=dict(cognitive_adapters or {})
        self.runner=BwrapSandboxRunner(); self._active:dict[str,Any]|None=None
        self._authority_lost=threading.Event(); self._stop_heartbeats=threading.Event()
        self._cancel_requested=threading.Event(); self._cancel_command_id: str | None=None
        self.registration_record: Mapping[str,Any] | None = None

    def start(self) -> Mapping[str,Any]:
        response=self.client.register(software_version="agentic-runtime-python-worker/1",capabilities=self.capabilities,
            task_types=self.task_types,resource_classes=self.resource_classes,resource_policy=self.resource_policy,
            allowed_tools=self.allowed_tools,worker_class=self.worker_class)
        self.registration_record=response
        inventory=[]
        for marker in self.root.glob("*/.agentic-attempt.json"):
            try:
                record=json.loads(marker.read_text())
                entry=self._sandbox_entry(record, "PRESENT")
                # A process restart never resumes an old writable attempt. Report
                # it present first; only durable absence can tombstone it.
                try:
                    shutil.rmtree(marker.parent)
                except OSError:
                    pass
                if not marker.parent.exists():
                    entry["state"]="ABSENT"
                    self._write_tombstone(entry)
                inventory.append(entry)
            except (OSError,ValueError,KeyError):
                continue
        inventory.extend(self._read_tombstones())
        inventory=list({entry["sandbox_id"]:entry for entry in inventory}.values())
        if inventory:
            response=self.client.reconcile_local(inventory)
            confirmed=set(response.get("confirmed_absent",[]))
            untracked=set(response.get("untracked",[]))
            for sandbox_id in confirmed|untracked:
                (self.tombstones/f"{sandbox_id}.json").unlink(missing_ok=True)
        return response

    @staticmethod
    def _sandbox_entry(record: Mapping[str,Any], state: str) -> dict[str,Any]:
        return {"sandbox_id":record["sandbox_id"],"attempt_id":record["attempt_id"],
            "task_id":record["task_id"],"lease_epoch":int(record["lease_epoch"]),
            "worker_instance_id":record["worker_instance_id"],"state":state}

    def _write_tombstone(self, entry: Mapping[str,Any]) -> None:
        target=self.tombstones/f"{entry['sandbox_id']}.json"
        temporary=target.with_suffix(".tmp")
        with temporary.open("w",encoding="utf-8") as stream:
            json.dump(dict(entry),stream,sort_keys=True); stream.flush(); os.fsync(stream.fileno())
        os.replace(temporary,target)

    def _read_tombstones(self) -> list[dict[str,Any]]:
        entries=[]
        for path in self.tombstones.glob("sbx_*.json"):
            try:
                entry=json.loads(path.read_text(encoding="utf-8"))
                if entry.get("state")=="ABSENT" and not (self.root/entry["sandbox_id"]).exists():
                    entries.append(entry)
            except (OSError,ValueError,KeyError):
                continue
        return entries

    def _heartbeat_loop(self, task: Mapping[str,Any]) -> None:
        active=[{"task_id":task["task_id"],"attempt_id":task["attempt_id"],"lease_epoch":task["lease_epoch"]}]
        failures=0
        while not self._stop_heartbeats.wait(self.lease_heartbeat_seconds):
            try:
                response=self.client.heartbeat(active)
                failures=0
                commands=[c for c in response.get("commands",[])
                    if c.get("command")=="CANCEL_ATTEMPT"
                    and c.get("task_id")==task["task_id"]
                    and c.get("attempt_id")==task["attempt_id"]
                    and c.get("lease_epoch")==task["lease_epoch"]]
                if commands:
                    self._cancel_command_id=commands[0].get("command_id")
                    self._cancel_requested.set()
                    return
                if response.get("stale_attempts"):
                    self._authority_lost.set(); return
            except Exception:
                failures+=1
                if failures>=2:
                    self._authority_lost.set(); return

    def run_once(self, *, idempotency_key: str | None=None) -> Mapping[str,Any]:
        pulse=self.client.heartbeat([])
        if pulse.get("status")=="DRAINING":
            return {"status":"DRAINING","worker_id":self.identity.worker_id}
        claim_key=idempotency_key or uuid.uuid4().hex
        response=self.client.claim(claim_key)
        task=response.get("task")
        if not task:
            return {"status":"IDLE","worker_id":self.identity.worker_id,"worker_instance_id":self.identity.worker_instance_id}
        self._active=task; self._authority_lost.clear(); self._cancel_requested.clear()
        self._cancel_command_id=None; self._stop_heartbeats.clear()
        if task.get("task_type")=="cognitive_invocation":
            metadata=task.get("metadata") or {}
            request_envelope=metadata.get("worker_input_cognitive_request")
            route_id=request_envelope.get("route_id") if isinstance(request_envelope,dict) else None
            if route_id not in self.cognitive_adapters:
                return self.client.cancel_attempt(task["task_id"],task["attempt_id"],task["lease_epoch"])
        started_response=self.client.start_attempt(task["task_id"],task["attempt_id"],task["lease_epoch"])
        sandbox_id="sbx_"+uuid.uuid4().hex
        workspace=self.root/sandbox_id; workspace.mkdir(mode=0o700)
        marker={"sandbox_id":sandbox_id,"task_id":task["task_id"],"attempt_id":task["attempt_id"],
            "lease_epoch":task["lease_epoch"],"worker_id":self.identity.worker_id,
            "worker_instance_id":self.identity.worker_instance_id}
        (workspace/".agentic-attempt.json").write_text(json.dumps(marker,sort_keys=True))
        policy={k:min(int(v),int(self.resource_policy[k])) for k,v in
            {**DEFAULT_RESOURCE_POLICY,**(task.get("budget") or {})}.items()
            if k in self.resource_policy and isinstance(v,(int,float)) and v>0}
        for key,value in DEFAULT_RESOURCE_POLICY.items(): policy.setdefault(key,min(value,self.resource_policy.get(key,value)))
        heartbeat=None
        try:
            self.client.sandbox_created(task["task_id"],task["attempt_id"],task["lease_epoch"],sandbox_id,policy)
            heartbeat=threading.Thread(target=self._heartbeat_loop,args=(task,),daemon=True)
            heartbeat.start()
            cognitive_result=None
            if task.get("task_type")=="cognitive_invocation":
                from agentic_runtime.cognitive.contracts import CognitiveInvocationRequest
                envelope=started_response.get("cognitive_request")
                if not isinstance(envelope,dict):
                    raise RuntimeError("control plane did not provide a frozen cognitive request")
                route_id=envelope.get("requested_route")
                adapter=self.cognitive_adapters.get(route_id)
                if adapter is None:
                    raise RuntimeError("requested cognitive route is not installed on this worker")
                request_data={k:v for k,v in envelope.items() if k!="request_hash"}
                request_data["context_refs"]=tuple(request_data.get("context_refs",()))
                request_data["allowed_tools"]=tuple(request_data.get("allowed_tools",()))
                cognitive_request=CognitiveInvocationRequest(**request_data)
                finished=threading.Event(); invocation_result:dict[str,Any]={}
                def invoke_one() -> None:
                    try: invocation_result["result"]=adapter.invoke(cognitive_request)
                    except BaseException as exc: invocation_result["error"]=exc
                    finally: finished.set()
                invoke_thread=threading.Thread(target=invoke_one,name="cognitive-invocation",daemon=True)
                invoke_thread.start()
                process_termination_confirmed=False
                while not finished.wait(.05):
                    if self._cancel_requested.is_set():
                        process_termination_confirmed=bool(adapter.cancel(cognitive_request.invocation_id))
                        finished.wait(2)
                        break
                    if self._authority_lost.is_set():
                        adapter.cancel(cognitive_request.invocation_id)
                        finished.wait(2)
                        break
                invoke_thread.join(timeout=1)
                if "error" in invocation_result:
                    raise invocation_result["error"]
                if "result" not in invocation_result:
                    raise TimeoutError("cognitive adapter did not stop after authority loss")
                cognitive_result=invocation_result["result"]
                if self._cancel_requested.is_set():
                    if not process_termination_confirmed or cognitive_result.status.value!="CANCELLED":
                        self._authority_lost.set()
                        return {"status":"STALE","task_id":task["task_id"],"attempt_id":task["attempt_id"]}
                    return self.client.cancel_attempt(task["task_id"],task["attempt_id"],task["lease_epoch"],
                        invocation_id=cognitive_request.invocation_id,command_id=self._cancel_command_id,
                        process_termination_confirmed=True)
                raw=adapter.artifacts.read(cognitive_result.raw_artifact_id) if cognitive_result.raw_artifact_id else None
                normalized=adapter.artifacts.read(cognitive_result.normalized_artifact_id) if cognitive_result.normalized_artifact_id else None
                bundle={"format":"agentic-cognitive-bundle-v1","invocation_id":cognitive_result.invocation_id,
                    "request_hash":envelope["request_hash"],"status":cognitive_result.status.value,
                    "raw_hash":cognitive_result.raw_hash,"normalized_hash":cognitive_result.normalized_hash,
                    "raw_b64":base64.b64encode(raw).decode("ascii") if raw is not None else None,
                    "normalized_b64":base64.b64encode(normalized).decode("ascii") if normalized is not None else None,
                    "provider":cognitive_result.provider,"requested_route":cognitive_result.requested_route,
                    "resolved_route":cognitive_result.resolved_route,"resolution_state":cognitive_result.resolution_state,
                    "model_version":cognitive_result.model_version,"adapter_version":cognitive_result.adapter_version,
                    "started_at":cognitive_result.started_at,"completed_at":cognitive_result.completed_at,
                    "latency_ms":cognitive_result.latency_ms,"structured_output":cognitive_result.structured_output,
                    "finish_reason":cognitive_result.finish_reason,"input_tokens":cognitive_result.input_tokens,
                    "output_tokens":cognitive_result.output_tokens,"cached_tokens":cognitive_result.cached_tokens,
                    "reasoning_tokens":cognitive_result.reasoning_tokens,"monetary_cost":cognitive_result.monetary_cost,
                    "cost_state":cognitive_result.cost_state.value,"tool_summary":list(cognitive_result.tool_summary),
                    "stderr_class":cognitive_result.stderr_class,"error_class":cognitive_result.error_class,
                    "telemetry":dict(cognitive_result.telemetry)}
                content=json.dumps(bundle,sort_keys=True,separators=(",",":"),ensure_ascii=False).encode("utf-8")
            else:
                content=self.runner.execute_json(workspace,{"value":task.get("metadata",{}).get("worker_input_value",
                    "remote-ok:"+task["attempt_id"]),"sleep_seconds":min(float(task.get("metadata",{}).get(
                        "worker_sleep_seconds",0)),policy["wall_time_seconds"]),
                    "spawn_tree_seconds":min(float(task.get("metadata",{}).get("worker_spawn_tree_seconds",0)),
                        policy["wall_time_seconds"])},
                    policy,self._authority_lost,self._cancel_requested)
            if self._authority_lost.is_set():
                return {"status":"STALE","task_id":task["task_id"],"attempt_id":task["attempt_id"]}
            digest=digest_bytes(content)
            artifact=self.client.upload_artifact(task["task_id"],task["attempt_id"],task["lease_epoch"],content,digest,
                request_id="transfer_"+task["attempt_id"])
            result=self.client.submit_result(task_id=task["task_id"],attempt_id=task["attempt_id"],
                lease_epoch=task["lease_epoch"],artifact_id=artifact["artifact_id"],result_hash=digest,
                idempotency_key="result_"+task["attempt_id"],usage=(
                    {"input_tokens":cognitive_result.input_tokens,"output_tokens":cognitive_result.output_tokens,
                     "estimated_cost":cognitive_result.monetary_cost} if cognitive_result else {}))
            return result
        except PermissionError:
            try:
                cancelled=self._cancel_requested.is_set()
                return self.client.cancel_attempt(task["task_id"],task["attempt_id"],task["lease_epoch"],
                    command_id=self._cancel_command_id if cancelled else None,
                    process_termination_confirmed=cancelled)
            except Exception:
                return {"status":"STALE","task_id":task["task_id"],"attempt_id":task["attempt_id"]}
        finally:
            self._stop_heartbeats.set()
            if heartbeat:
                heartbeat.join(timeout=2)
            try: shutil.rmtree(workspace)
            except FileNotFoundError: pass
            except OSError: pass
            absent=not workspace.exists()
            if absent:
                self._write_tombstone(self._sandbox_entry(marker,"ABSENT"))
            try:
                if absent:
                    response=self.client.reconcile_local([self._sandbox_entry(marker,"ABSENT")])
                    if sandbox_id in (set(response.get("confirmed_absent",[]))|set(response.get("untracked",[]))):
                        (self.tombstones/f"{sandbox_id}.json").unlink(missing_ok=True)
                else:
                    self.client.cleanup_sandbox(task["task_id"],task["attempt_id"],task["lease_epoch"],
                        sandbox_id,workspace_absent=False)
                    self.client.reconcile_local([self._sandbox_entry(marker,"PRESENT")])
            except Exception:
                # Leave durable cleanup state unresolved; next authenticated
                # instance reports its on-disk inventory during registration.
                pass
            self._active=None

    def drain(self) -> None:
        if self._active:
            return

    def run_forever(self) -> None:
        self.start()
        record=self.registration_record or {}
        # The registration response is returned by the authoritative control
        # plane over the authenticated worker channel. Log only bounded runtime
        # identity/capability metadata; never request envelopes or credentials.
        print(json.dumps({"event":"remote_worker_registered",
            "worker_id":record.get("worker_id"),
            "worker_instance_id":record.get("worker_instance_id"),
            "state":record.get("status"),"worker_class":record.get("worker_class"),
            "protocol_version":record.get("protocol_version"),
            "capabilities":sorted(self.capabilities),"task_types":sorted(self.task_types),
            "resource_classes":sorted(self.resource_classes),
            "cognitive_route_ids":sorted(self.cognitive_adapters)},sort_keys=True),flush=True)
        while True:
            result=self.run_once()
            if result.get("status")=="DRAINING":
                return
            if result.get("status")=="IDLE":
                time.sleep(self.poll_seconds)



def main() -> int:
    endpoint=setting("AGENTIC_RUNTIME_CONTROL_ENDPOINT",required=True)
    worker_id=setting("AGENTIC_RUNTIME_WORKER_ID",required=True)
    token=setting("AGENTIC_RUNTIME_WORKER_TOKEN",required=True)
    root=Path(setting("AGENTIC_RUNTIME_WORKER_ROOT",required=True))
    caps=json.loads(setting("AGENTIC_RUNTIME_WORKER_CAPABILITIES",required=True))
    tasks=json.loads(setting("AGENTIC_RUNTIME_WORKER_TASK_TYPES",required=True))
    classes=json.loads(setting("AGENTIC_RUNTIME_WORKER_RESOURCE_CLASSES",required=True))
    tools=json.loads(setting("AGENTIC_RUNTIME_WORKER_ALLOWED_TOOLS","[]"))
    policy=json.loads(setting("AGENTIC_RUNTIME_WORKER_RESOURCE_POLICY",required=True))
    tls_context=None
    if endpoint.startswith("https://"):
        tls_context=build_worker_tls_context(setting("AGENTIC_RUNTIME_TLS_CA_FILE",required=True),setting("AGENTIC_RUNTIME_TLS_CERT_FILE",required=True),
            setting("AGENTIC_RUNTIME_TLS_KEY_FILE",required=True))
    worker=RemoteWorker(endpoint=endpoint,worker_id=worker_id,token=token,root=root,
        capabilities=caps,task_types=tasks,resource_classes=classes,resource_policy=policy,allowed_tools=tools,
        worker_class=setting("AGENTIC_RUNTIME_WORKER_CLASS","REMOTE_PERSISTENT"),tls_context=tls_context)
    profile_config=os.environ.get("AGENTIC_RUNTIME_COGNITIVE_CONFIG")
    if profile_config:
        if (not endpoint.startswith("https://") or setting("AGENTIC_RUNTIME_REQUIRE_TLS")!="1" or tools
                or "cognitive_invocation" not in tasks
                or not {"cognitive_invocation","structured_output"}.issubset(set(caps))):
            raise ValueError("cognitive adapters require TLS and the structured no-tools worker boundary")
        from agentic_runtime.artifacts.store import ArtifactStore
        from agentic_runtime.cognitive.profiles import configured_adapters
        artifact_root=Path(os.environ.get("AGENTIC_RUNTIME_COGNITIVE_ARTIFACT_ROOT",str(root/"cognitive-artifacts")))
        worker.cognitive_adapters.update(configured_adapters(Path(profile_config),
            artifacts=ArtifactStore(artifact_root), worker_id=worker_id,
            worker_instance_id=worker.identity.worker_instance_id))
    try:
        if setting("AGENTIC_RUNTIME_WORKER_ONCE")=="1":
            worker.start()
            result=worker.run_once()
            print(json.dumps(result,sort_keys=True))
        else:
            worker.run_forever()
    except RemoteClientError as exc:
        print(json.dumps({"status":"ERROR","http_status":exc.status,"error":str(exc)},sort_keys=True))
        return 2
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        print(json.dumps({"status":"CONTROL_PLANE_UNAVAILABLE","error":type(exc).__name__},sort_keys=True))
        return 2
    return 0


if __name__=="__main__": raise SystemExit(main())
