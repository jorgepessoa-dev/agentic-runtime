"""Provision disposable login identities, run the role attack matrix, then remove them.

Set M4C_ADMIN_DATABASE_URL to an administrator DSN and M3_TEST_DATABASE_URL to
the disposable project's test database. Generated passwords exist only in this
process environment and are never written or printed.
"""
from __future__ import annotations

import os
import secrets
import subprocess
import sys
import uuid
import json
import time
import tempfile
from pathlib import Path

import psycopg
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo


NAMES = {
    "runtime": ("agentic_runtime", "agentic_runtime_runtime"),
    "evaluator": ("agentic_evaluator", "agentic_runtime_evaluator"),
    "verifier": ("agentic_e2_verifier", "agentic_runtime_verifier"),
    "governance": ("agentic_governance", "agentic_runtime_governance"),
    "migrator": ("agentic_migrator", "agentic_runtime_migration"),
    "e1promotion": ("agentic_e1_promoter", "agentic_runtime_e1_promotion"),
    "e2promotion": ("agentic_e2_promoter", "agentic_runtime_e2_promotion"),
}
MARKER = "agentic-runtime-m4c-temporary-test-identity"
# This fixed NOLOGIN authority owns SECURITY DEFINER promotion functions in
# fresh migrations. It is not one of the temporary login identities in NAMES.
EXTRA_CAPABILITY_GROUPS = frozenset({"agentic_promotion_executor"})


def _proc(pid: int):
    try:
        raw=Path(f"/proc/{pid}/stat").read_text()
        tail=raw[raw.rfind(")")+2:].split()
        ppid=int(tail[1]); rss=int(tail[21])*os.sysconf("SC_PAGE_SIZE")//1024
        comm=Path(f"/proc/{pid}/comm").read_text().strip()
        return ppid,rss,comm
    except (OSError,ValueError,IndexError): return None


def _sample(root_pid: int):
    procs={}
    for entry in Path("/proc").iterdir():
        if entry.name.isdigit():
            info=_proc(int(entry.name))
            if info: procs[int(entry.name)]=info
    tree={root_pid}; changed=True
    while changed:
        changed=False
        for pid,(ppid,_,_) in procs.items():
            if ppid in tree and pid not in tree: tree.add(pid); changed=True
    runner=sum(procs[pid][1] for pid in tree if pid in procs)
    postgres=sum(rss for _,rss,comm in procs.values() if comm.startswith("postgres"))
    mem={}
    for line in Path("/proc/meminfo").read_text().splitlines():
        bits=line.split()
        if len(bits)>1 and bits[0].rstrip(":") in {"MemAvailable","SwapTotal","SwapFree"}:
            mem[bits[0].rstrip(":")]=int(bits[1])
    pressure={}
    for name in ("memory","io"):
        try:
            pressure[name]=Path(f"/proc/pressure/{name}").read_text().splitlines()
        except OSError: pressure[name]=None
    return {"runner_tree_rss_kib":runner,"postgres_process_tree_rss_kib":postgres,
        "mem_available_kib":mem.get("MemAvailable"),"swap_used_kib":mem.get("SwapTotal",0)-mem.get("SwapFree",0),
        "load_average":os.getloadavg(),"pressure":pressure}


def _run_and_measure(env):
    started=time.monotonic()
    selector=env.get("M4C_TEST_SELECTOR")
    command=[sys.executable,"-m","unittest"]
    if selector:
        command.extend([selector,"-v"])
    else:
        command.extend(["discover","-s","tests","-v"])
    with tempfile.TemporaryFile(mode="w+t",encoding="utf-8") as output_file:
        child=subprocess.Popen(command,
            env=env,stdout=output_file,stderr=subprocess.STDOUT,text=True)
        samples=[]
        while child.poll() is None:
            samples.append(_sample(child.pid)); time.sleep(.2)
        output_file.seek(0)
        print(output_file.read(),end="")
    if not samples: samples.append(_sample(child.pid))
    measurements={"duration_seconds":time.monotonic()-started,"sample_interval_seconds":0.2,
        "samples":len(samples),"runner_tree_peak_rss_kib":max(x["runner_tree_rss_kib"] for x in samples),
        "postgres_peak_rss_kib":max(x["postgres_process_tree_rss_kib"] for x in samples),
        "mem_available_min_kib":min(x["mem_available_kib"] for x in samples if x["mem_available_kib"] is not None),
        "swap_used_max_kib":max(x["swap_used_kib"] for x in samples),
        "load_average_max":max(max(x["load_average"]) for x in samples),
        "pressure_sample":samples[-1]["pressure"],
        "limitations":["suite-wide sample includes tests and test worker processes; soak RSS is not isolated",
            "RSS is sampled at 200ms and may miss shorter peaks","PostgreSQL RSS sums visible postgres processes"]}
    path=Path(tempfile.gettempdir())/"agentic-runtime-test-resource-measurements.json"
    soak_path=path.parent/"soak-resource-measurements.json"
    if soak_path.exists(): measurements["soak"]=json.loads(soak_path.read_text())
    path.parent.mkdir(parents=True,exist_ok=True)
    path.write_text(json.dumps(measurements,indent=2)+"\n")
    return child.returncode


def main() -> int:
    admin_dsn = os.environ["M4C_ADMIN_DATABASE_URL"]
    database_dsn = os.environ["M3_TEST_DATABASE_URL"]
    created: list[str] = []
    migration_database: str | None = None
    try:
        with psycopg.connect(admin_dsn, autocommit=True) as admin:
            database_name=os.environ.get("M4C_TEST_DATABASE_NAME", "agentic_runtime_m4c_trial")
            requested_database=conninfo_to_dict(database_dsn).get("dbname")
            if requested_database != database_name:
                raise RuntimeError("M3_TEST_DATABASE_URL must name the explicitly disposable M4C_TEST_DATABASE_NAME")
            # The target is explicitly a disposable project test database.
            # Recreate it from empty state so forward migrations are verified
            # against their current checksum and never against stale fixtures.
            if not admin.execute("SELECT 1 FROM pg_roles WHERE rolname='agentic_runtime_test'").fetchone():
                raise RuntimeError("agentic_runtime_test role is required for the disposable suite database")
            required_groups = {group for _, group in NAMES.values()} | EXTRA_CAPABILITY_GROUPS
            for group in sorted(required_groups):
                if not admin.execute("SELECT 1 FROM pg_roles WHERE rolname=%s", (group,)).fetchone():
                    admin.execute(sql.SQL("CREATE ROLE {} NOLOGIN").format(sql.Identifier(group)))
            admin.execute(sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(sql.Identifier(database_name)))
            admin.execute(sql.SQL("CREATE DATABASE {} OWNER agentic_runtime_test").format(sql.Identifier(database_name)))
            for key, (name, group) in NAMES.items():
                exists = admin.execute("SELECT shobj_description(oid,'pg_authid') AS marker FROM pg_roles WHERE rolname=%s", (name,)).fetchone()
                if exists:
                    if exists[0] != MARKER:
                        raise RuntimeError(f"role {name} already exists; refusing to change existing identity")
                    admin.execute(sql.SQL("REVOKE {} FROM {}").format(sql.Identifier(group),sql.Identifier(name)))
                    admin.execute(sql.SQL("REVOKE CONNECT ON DATABASE {} FROM {}").format(sql.Identifier(database_name),sql.Identifier(name)))
                    admin.execute(sql.SQL("DROP ROLE {}").format(sql.Identifier(name)))
                password = secrets.token_urlsafe(36)
                admin.execute(sql.SQL("CREATE ROLE {} LOGIN PASSWORD {}").format(sql.Identifier(name),sql.Literal(password)))
                admin.execute(sql.SQL("COMMENT ON ROLE {} IS {}").format(sql.Identifier(name),sql.Literal(MARKER)))
                admin.execute(sql.SQL("GRANT {} TO {}").format(sql.Identifier(group),sql.Identifier(name)))
                admin.execute(sql.SQL("GRANT CONNECT ON DATABASE {} TO {}").format(sql.Identifier(database_name),sql.Identifier(name)))
                created.append(name)
                original=conninfo_to_dict(database_dsn)
                os.environ[f"M4C_ROLE_DSN_{key.upper()}"] = make_conninfo(database_dsn,user=name,password=password,
                    port=original.get("port","5432"))
            for name,_ in NAMES.values():
                admin.execute(sql.SQL("GRANT CONNECT ON DATABASE {} TO {}").format(sql.Identifier(database_name),sql.Identifier(name)))
            migration_database="agentic_runtime_m4c_migrate_"+uuid.uuid4().hex[:10]
            admin.execute(sql.SQL("CREATE DATABASE {} OWNER {}").format(sql.Identifier(migration_database),sql.Identifier("agentic_migrator")))
            # Reuse the provisioned migrator secret without printing or storing it.
            role_parts=conninfo_to_dict(os.environ["M4C_ROLE_DSN_MIGRATOR"])
            migration_dsn=make_conninfo(database_dsn,dbname=migration_database,
                user="agentic_migrator",password=role_parts["password"],port=original.get("port","5432"))
            os.environ["M4C_MIGRATOR_DATABASE_DSN"]=migration_dsn
            from agentic_runtime.persistence.postgres import apply_migrations,connect
            migrator=connect(migration_dsn); migrator.autocommit=True
            try:
                first=apply_migrations(migrator)
                second=apply_migrations(migrator)
                if not first or second:
                    raise RuntimeError("migrator identity failed fresh/replay migration check")
                print(f"migrator fresh migrations={len(first)} replay=0")
            finally: migrator.close()
            # A genuinely fresh suite database needs the same explicit M4 role
            # grants as the historical pre-provisioned test DB. Apply schema
            # first, then the capability bootstrap, before the test runner
            # connects under agentic_runtime_test.
            suite_dsn=make_conninfo(database_dsn,dbname=database_name)
            suite_admin=connect(suite_dsn); suite_admin.autocommit=True
            try:
                suite_admin.execute("SET ROLE agentic_runtime_test")
                apply_migrations(suite_admin)
                suite_admin.execute("RESET ROLE")
            finally: suite_admin.close()
            # Capability-role setup creates NOLOGIN group roles and therefore
            # belongs to the already-authorized test administrator, not the
            # restricted suite owner used for migration replay and tests.
            suite_admin=connect(make_conninfo(admin_dsn,dbname=database_name)); suite_admin.autocommit=True
            try:
                bootstrap=(Path(__file__).resolve().parent/"bootstrap_m4_roles.sql").read_text()
                suite_admin.execute(bootstrap,prepare=False)
            finally: suite_admin.close()
            env = os.environ.copy()
            env["M3_TEST_DATABASE_ROLE"] = os.environ.get("M3_TEST_DATABASE_ROLE", "agentic_runtime_test")
            # M5's control plane is a distinct process and connects through its
            # restricted deployment identity; the worker receives no DB DSN.
            env["M5_CONTROL_DATABASE_URL"] = os.environ["M4C_ROLE_DSN_RUNTIME"]
            env["M5_CONTROL_DATABASE_ROLE"] = "agentic_runtime_runtime"
            env.setdefault("M5_CONTROL_ADMIN_TOKEN", secrets.token_urlsafe(32))
            return _run_and_measure(env)
    finally:
        with psycopg.connect(admin_dsn, autocommit=True) as admin:
            if migration_database:
                admin.execute(sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(sql.Identifier(migration_database)))
            for name in reversed(created):
                marker = admin.execute("SELECT shobj_description(oid,'pg_authid') FROM pg_roles WHERE rolname=%s",(name,)).fetchone()
                if marker and marker[0] == MARKER:
                    group=NAMES[next(k for k,(n,_) in NAMES.items() if n==name)][1]
                    admin.execute(sql.SQL("REVOKE {} FROM {}").format(sql.Identifier(group),sql.Identifier(name)))
                    admin.execute(sql.SQL("REVOKE CONNECT ON DATABASE {} FROM {}").format(sql.Identifier(os.environ.get("M4C_TEST_DATABASE_NAME", "agentic_runtime_m4c_trial")),sql.Identifier(name)))
                    admin.execute(sql.SQL("DROP ROLE {}").format(sql.Identifier(name)))


if __name__ == "__main__":
    raise SystemExit(main())
