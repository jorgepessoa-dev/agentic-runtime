from __future__ import annotations

import uuid
from typing import Any, Callable


class OutboxDispatcher:
    """At-least-once dispatcher. Consumers must deduplicate by idempotency key."""

    def __init__(self, connection: Any, dispatch: Callable[[str, dict[str, Any]], str],
                 *, lock_seconds: int = 30) -> None:
        self.db = connection
        self.dispatch = dispatch
        self.lock_seconds = lock_seconds

    def dispatch_one(self, worker_id: str, *, idempotency_key: str | None = None,
                     topic: str | None = None) -> bool:
        with self.db.transaction():
            row = self.db.execute("""SELECT outbox_id,topic,idempotency_key,payload
                FROM runtime.outbox WHERE delivered_at IS NULL AND available_at<=now()
                  AND (locked_until IS NULL OR locked_until<=now())
                  AND (%s::text IS NULL OR idempotency_key=%s)
                  AND (%s::text IS NULL OR topic=%s)
                ORDER BY created_at LIMIT 1 FOR UPDATE SKIP LOCKED""",
                (idempotency_key,idempotency_key,topic,topic)).fetchone()
            if not row:
                return False
            self.db.execute("""UPDATE runtime.outbox SET locked_by=%s,
                locked_until=now()+(%s * interval '1 second'),attempts=attempts+1
                WHERE outbox_id=%s""", (worker_id,self.lock_seconds,row["outbox_id"]))
        # No database lock is held across external dispatch.
        try:
            effect_ref = self.dispatch(row["idempotency_key"], row["payload"])
        except Exception as exc:
            with self.db.transaction():
                self.db.execute("""UPDATE runtime.outbox SET last_error=%s,locked_by=NULL,locked_until=NULL,
                    available_at=now()+(1 * interval '1 second') WHERE outbox_id=%s AND locked_by=%s""",
                    (type(exc).__name__,row["outbox_id"],worker_id))
            raise
        with self.db.transaction():
            current = self.db.execute("SELECT delivered_at FROM runtime.outbox WHERE outbox_id=%s FOR UPDATE",
                                      (row["outbox_id"],)).fetchone()
            if current and current["delivered_at"] is None:
                self.db.execute("""INSERT INTO runtime.dispatch_receipts
                    (receipt_id,outbox_id,logical_effect_ref) VALUES (%s,%s,%s)
                    ON CONFLICT(outbox_id) DO NOTHING""",
                    (f"receipt_{uuid.uuid4().hex}",row["outbox_id"],effect_ref))
                self.db.execute("UPDATE runtime.outbox SET delivered_at=now(),locked_by=NULL,locked_until=NULL WHERE outbox_id=%s",
                                (row["outbox_id"],))
        return True
