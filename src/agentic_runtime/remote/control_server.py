from __future__ import annotations

import json
import logging
import signal
import threading
import time

from psycopg import InterfaceError, OperationalError

from agentic_runtime.coordinator.service import Coordinator
from agentic_runtime.persistence.outbox import OutboxDispatcher
from .control import RemoteControlPlane, build_server_tls_context, serve_local, serve_secure
from .config import setting

logger=logging.getLogger("agentic_runtime.control")


def emit(level: int, event: str, **fields) -> None:
    logger.log(level,json.dumps({"event":event,"at":time.time(),**fields},sort_keys=True))


def dispatch_outbox_item(db, service, key, payload):
    """Resolve pull-dispatch hints from durable task state; never push authority."""
    if key.startswith("dispatch:"):
        task=db.execute("SELECT task_id FROM runtime.tasks WHERE task_id=%s",(payload.get("task_id"),)).fetchone()
        if not task:
            raise ValueError("outbox task reference is missing")
        return "pull-queue:"+task["task_id"]
    if key.startswith("verify:"):
        task_id,attempt_id,epoch=payload.get("task_id"),payload.get("attempt_id"),payload.get("lease_epoch")
        row=db.execute("SELECT status FROM runtime.tasks WHERE task_id=%s",(task_id,)).fetchone()
        if not row:
            raise ValueError("outbox verification task is missing")
        if row["status"]=="RESULT_COMMITTED":
            evidence=Coordinator(db).accept_verified_result(task_id=task_id,attempt_id=attempt_id,
                lease_epoch=int(epoch),artifact_store=service.store,actor_id="outbox-verifier")
            return "verified:"+str(evidence)
        if row["status"]=="ACCEPTED":
            return "verified:"+task_id
        raise ValueError("result is not in a verifiable state")
    raise ValueError("unsupported outbox topic")


def dispatch_outbox_batch(db, service, *, worker_id: str="remote-pull-outbox", max_items: int=32) -> int:
    if not 1 <= max_items <= 256:
        raise ValueError("outbox batch size must be between 1 and 256")
    dispatcher=OutboxDispatcher(db,lambda key,payload:dispatch_outbox_item(db,service,key,payload))
    delivered=0
    for _ in range(max_items):
        if not dispatcher.dispatch_one(worker_id):
            break
        delivered+=1
    return delivered


def main() -> int:
    logging.basicConfig(level=setting("AGENTIC_RUNTIME_LOG_LEVEL","INFO"),format="%(message)s")
    host=setting("AGENTIC_RUNTIME_CONTROL_HOST",setting("AGENTIC_RUNTIME_CONTROL_HOST_LEGACY","127.0.0.1"))
    port=int(setting("AGENTIC_RUNTIME_CONTROL_PORT",setting("AGENTIC_RUNTIME_CONTROL_PORT_LEGACY","8765")))
    max_connections=int(setting("AGENTIC_RUNTIME_MAX_CONNECTIONS","8"))
    interval=float(setting("AGENTIC_RUNTIME_MAINTENANCE_INTERVAL_SECONDS","2"))
    if not 0.1 <= interval <= 60:
        raise ValueError("maintenance interval must be between 0.1 and 60 seconds")
    service=RemoteControlPlane(setting("AGENTIC_RUNTIME_CONTROL_DATABASE_URL",required=True),setting("AGENTIC_RUNTIME_ARTIFACT_ROOT",required=True),
        database_role=setting("AGENTIC_RUNTIME_CONTROL_DATABASE_ROLE","agentic_runtime_runtime"),
        lease_seconds=int(setting("AGENTIC_RUNTIME_LEASE_SECONDS","15")),
        heartbeat_suspect_seconds=int(setting("AGENTIC_RUNTIME_HEARTBEAT_SUSPECT_SECONDS","3")),
        admin_token=setting("AGENTIC_RUNTIME_CONTROL_ADMIN_TOKEN"))
    cert=setting("AGENTIC_RUNTIME_TLS_CERT_FILE"); key=setting("AGENTIC_RUNTIME_TLS_KEY_FILE")
    client_ca=setting("AGENTIC_RUNTIME_TLS_CLIENT_CA_FILE")
    if host not in {"127.0.0.1","::1","localhost"}:
        if not (cert and key and client_ca):
            raise RuntimeError("non-loopback bind requires mutual TLS certificate and client CA")
        context=build_server_tls_context(cert,key,client_ca)
        server=serve_secure(service,host,port,context,max_connections=max_connections)
    else:
        if cert or key or client_ca:
            if not (cert and key and client_ca):
                raise RuntimeError("TLS requires server certificate, key and client CA together")
            server=serve_secure(service,host,port,build_server_tls_context(cert,key,client_ca),
                max_connections=max_connections)
        else:
            server=serve_local(service,host,port,max_connections=max_connections)
    stop=threading.Event()
    database_available=False
    def maintainer():
        nonlocal database_available
        while not stop.wait(interval):
            db=None
            try:
                db=service.db()
                db.execute("SELECT 1")
                if not service.startup_reconciled or not database_available:
                    state=Coordinator(db).reconcile_runtime(scan_id="control-reconnect-"+str(time.time_ns()))
                    service.startup_reconciled=True
                    emit(logging.INFO,"database_reconciled",**state)
                database_available=True
                dispatch_outbox_batch(db,service,max_items=32)
                Coordinator(db).reconcile_exhausted_campaigns()
                with db.transaction():
                    db.execute("""UPDATE runtime.worker_instances SET status='SUSPECT'
                        WHERE status IN ('READY','BUSY') AND last_seen<now()-(%s*interval '1 second')""",
                        (service.heartbeat_suspect_seconds,))
                    offline=db.execute("""UPDATE runtime.worker_instances SET status='OFFLINE'
                        WHERE status IN ('SUSPECT','READY','BUSY','DRAINING')
                          AND last_seen<now()-(%s*interval '1 second') RETURNING worker_id,worker_instance_id""",
                        (service.lease_seconds*2,)).fetchall()
                    for row in offline:
                        db.execute("""UPDATE runtime.sandboxes SET status='CLEANUP_PENDING',cleanup_status='WORKER_OFFLINE'
                            WHERE worker_instance_id=%s AND status IN ('ACTIVE','CLEANUP_FAILED')""",
                            (row["worker_instance_id"],))
                        Coordinator(db)._event(event_type="REMOTE_WORKER_OFFLINE",actor_id="control-plane",
                            correlation_id=row["worker_instance_id"],payload={"worker_id":row["worker_id"],
                                "worker_instance_id":row["worker_instance_id"]})
                    if offline:
                        Coordinator(db).reconcile_runtime(scan_id="worker-loss-"+str(time.time_ns()))
            except (InterfaceError,OperationalError) as exc:
                database_available=False
                service.startup_reconciled=False
                emit(logging.ERROR,"control_plane_database_unavailable",error_class=type(exc).__name__)
            except Exception as exc:
                # Never log exception strings/DSNs/SQL parameters; class names
                # are enough to distinguish outages from contract failures.
                emit(logging.ERROR,"control_plane_maintenance_failed",error_class=type(exc).__name__)
            finally:
                if db is not None: db.close()
    thread=threading.Thread(target=maintainer,name="worker-liveness",daemon=True); thread.start()
    print(f"READY:{server.server_address[1]}",flush=True)
    def request_shutdown(_signum,_frame):
        stop.set()
        threading.Thread(target=server.shutdown,name="control-shutdown",daemon=True).start()
    signal.signal(signal.SIGTERM,request_shutdown)
    signal.signal(signal.SIGINT,request_shutdown)
    try: server.serve_forever(poll_interval=.05)
    except KeyboardInterrupt: pass
    finally:
        stop.set(); thread.join(timeout=2); server.shutdown(); server.server_close()
    return 0


if __name__=="__main__": raise SystemExit(main())
