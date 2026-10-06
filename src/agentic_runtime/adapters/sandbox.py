from __future__ import annotations

import subprocess
import json
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Mapping, Sequence


@dataclass
class SandboxRecord:
    sandbox_id: str
    task_id: str
    attempt_id: str
    base_revision: str
    workspace_path: Path
    created_at: str
    status: str = "CREATED"
    resource_policy: Mapping[str, object] = field(default_factory=dict)
    cleanup_status: str = "NOT_STARTED"


class GitWorktreeSandboxAdapter:
    """One detached worktree per attempt; it does not construct a runtime worker."""

    def __init__(self, repository: Path, workspace_root: Path) -> None:
        self.repository = Path(repository).resolve(strict=True)
        self.workspace_root = Path(workspace_root).resolve()
        self.workspace_root.mkdir(parents=True, exist_ok=True)
        self._records: dict[str, SandboxRecord] = {}

    @staticmethod
    def _git(repo: Path, *args: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(["git", "-C", str(repo), *args], check=True, text=True,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=30)

    def create(self, task_id: str, attempt_id: str, base_revision: str) -> SandboxRecord:
        sandbox_id = f"sbx-{task_id}-{attempt_id}"
        if sandbox_id in self._records:
            raise ValueError(f"sandbox already exists: {sandbox_id}")
        path = self.workspace_root / sandbox_id
        self._git(self.repository, "worktree", "add", "--detach", str(path), base_revision)
        record = SandboxRecord(sandbox_id, task_id, attempt_id, base_revision, path,
            datetime.now(timezone.utc).isoformat())
        self._records[sandbox_id] = record
        return record

    def prepare(self, sandbox: SandboxRecord, resource_policy: Mapping[str, object]) -> SandboxRecord:
        sandbox.resource_policy = dict(resource_policy)
        sandbox.status = "READY"
        return sandbox

    def collect_artifacts(self, sandbox: SandboxRecord) -> Sequence[str]:
        result = self._git(sandbox.workspace_path, "status", "--porcelain=v1", "-z")
        return tuple(x for x in result.stdout.split("\0") if x)

    def cleanup(self, sandbox: SandboxRecord) -> Mapping[str, object]:
        sandbox.cleanup_status = "READY_TO_DESTROY"
        return {"sandbox_id": sandbox.sandbox_id, "cleanup_status": sandbox.cleanup_status,
                "workspace_exists": sandbox.workspace_path.exists()}

    def destroy(self, sandbox: SandboxRecord) -> Mapping[str, object]:
        self._git(self.repository, "worktree", "remove", "--force", str(sandbox.workspace_path))
        self._records.pop(sandbox.sandbox_id, None)
        sandbox.status = "DESTROYED"
        sandbox.cleanup_status = "DESTROYED"
        return {"sandbox_id": sandbox.sandbox_id, "destroyed": True}


class SandboxCleanupReconciler:
    """Reclaims only terminal/unleased attempt worktrees and records each try."""

    TERMINAL = {"RESULT_COMMITTED", "VERIFIED", "ACCEPTED", "REJECTED", "NEEDS_REVIEW",
                "FAILED_TRANSIENT", "FAILED_PERMANENT", "FAILED", "ABANDONED", "CANCELLED",
                "QUARANTINED", "BUDGET_EXCEEDED"}

    def __init__(self, db: object, adapter: GitWorktreeSandboxAdapter, *, max_retries: int = 3) -> None:
        self.db, self.adapter, self.max_retries = db, adapter, max_retries

    def reconcile(self) -> dict[str, int]:
        rows = self.db.execute("""SELECT s.*,a.status AS attempt_status,l.status AS lease_status,
                l.lease_until,l.attempt_id AS leased_attempt,(l.status='ACTIVE' AND l.lease_until>now()) AS lease_live
            FROM runtime.sandboxes s
            LEFT JOIN runtime.attempts a ON a.attempt_id=s.attempt_id
            LEFT JOIN runtime.leases l ON l.task_id=s.task_id
            WHERE s.status <> 'DESTROYED' OR s.workspace_ref IS NOT NULL""").fetchall()
        result = {"destroyed": 0, "retained_active": 0, "failed": 0, "already_absent": 0}
        for row in rows:
            path = Path(row["workspace_ref"]).resolve()
            if not path.is_relative_to(self.adapter.workspace_root):
                self._record(row["sandbox_id"], "FAILED", {"reason": "workspace escapes configured root"})
                result["failed"] += 1
                continue
            active_lease = (row["lease_live"] is True and row["leased_attempt"] == row["attempt_id"])
            attempt_status = row["attempt_status"]
            terminal = attempt_status is None or attempt_status in self.TERMINAL
            if active_lease and not terminal:
                self._record(row["sandbox_id"], "RETAINED_ACTIVE", {"attempt_status": attempt_status})
                result["retained_active"] += 1
                continue
            if not path.exists():
                self.db.execute("UPDATE runtime.sandboxes SET status='DESTROYED',cleanup_status='DESTROYED',destroyed_at=coalesce(destroyed_at,now()) WHERE sandbox_id=%s", (row["sandbox_id"],))
                self._record(row["sandbox_id"], "DESTROYED", {"workspace_absent": True})
                result["already_absent"] += 1
                continue
            previous = self.db.execute("SELECT cleanup_attempts FROM runtime.sandboxes WHERE sandbox_id=%s", (row["sandbox_id"],)).fetchone()["cleanup_attempts"]
            if previous >= self.max_retries:
                self.db.execute("UPDATE runtime.sandboxes SET status='CLEANUP_FAILED',cleanup_status='RETRY_LIMIT',orphaned_at=coalesce(orphaned_at,now()) WHERE sandbox_id=%s", (row["sandbox_id"],))
                result["failed"] += 1
                continue
            self.db.execute("UPDATE runtime.sandboxes SET status='CLEANUP_PENDING',cleanup_status='RECONCILING',cleanup_attempts=cleanup_attempts+1 WHERE sandbox_id=%s", (row["sandbox_id"],))
            record = SandboxRecord(row["sandbox_id"], row["task_id"], row["attempt_id"],
                row["base_revision"] or "", path, row["created_at"].isoformat(), status="CLEANUP_PENDING")
            try:
                self.adapter.destroy(record)
                self.db.execute("UPDATE runtime.sandboxes SET status='DESTROYED',cleanup_status='DESTROYED',destroyed_at=now() WHERE sandbox_id=%s", (row["sandbox_id"],))
                self._record(row["sandbox_id"], "DESTROYED", {"workspace_removed": True})
                result["destroyed"] += 1
            except (OSError, subprocess.SubprocessError, ValueError) as exc:
                self.db.execute("UPDATE runtime.sandboxes SET status='CLEANUP_FAILED',cleanup_status='FAILED',orphaned_at=coalesce(orphaned_at,now()) WHERE sandbox_id=%s", (row["sandbox_id"],))
                self._record(row["sandbox_id"], "FAILED", {"error_class": type(exc).__name__})
                result["failed"] += 1
        return result

    def _record(self, sandbox_id: str, outcome: str, detail: dict[str, object]) -> None:
        row = self.db.execute("SELECT cleanup_attempts FROM runtime.sandboxes WHERE sandbox_id=%s", (sandbox_id,)).fetchone()
        number = max(1, row["cleanup_attempts"])
        self.db.execute("""INSERT INTO runtime.sandbox_cleanup_attempts
            (cleanup_id,sandbox_id,attempt_number,finished_at,outcome,detail)
            VALUES (%s,%s,%s,now(),%s,%s) ON CONFLICT (sandbox_id,attempt_number)
            DO UPDATE SET finished_at=now(),outcome=EXCLUDED.outcome,detail=EXCLUDED.detail""",
            ("cleanup_" + uuid.uuid4().hex, sandbox_id, number, outcome, json.dumps(detail)))
