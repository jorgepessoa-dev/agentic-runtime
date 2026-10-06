# Sandbox Model

Isolation belongs to each write-capable `TaskAttempt`, not to a permanent agent identity.

```text
Task → Attempt → Sandbox → Artifact references
       ├─ A1       └─ S1
       └─ A2 (retry)   └─ S2 (fresh state)
Child task/attempt ─────── S3
```

## Attempt contract

Every attempt has a stable identity, task reference, genome/policy references, retry lineage, idempotency key, lease and fencing token, start/deadline, budget, status, executor/location references, event sequence and artifact/evidence references. A retry gets a new attempt and clean sandbox by default. Reuse requires explicit, verifiable snapshot semantics. At-least-once delivery is assumed: operations must be idempotent or reconciled; expired leases and stale fencing tokens cannot commit authoritative results. Cancellation, timeout, crash recovery and deterministic reconciliation are explicit states/operations.

## Sandbox grant

The SandboxAdapter receives a workspace snapshot/reference, filesystem policy, network policy, allowed tools, scoped secrets, CPU/RAM and other resource limits, deadline and process-lifecycle policy. Secret scope is least privilege; secrets are not copied into artifacts or general context. Network access is explicit and auditable. Process trees are tracked and terminated/reconciled on cancellation, timeout or worker loss. Artifact publication is explicit and verified; attempt completion requires persistent evidence, not a message.

## Workspace and collaboration

Parallel write-capable attempts receive separate snapshots/worktrees or equivalent isolated state. They never write to the same mutable tree. Handoffs pass task IDs, artifact references, structured findings and decisions. Integration is a separate attempt that consumes immutable outputs and produces a new artifact. Large artifacts remain referenced; consumers fetch targeted extracts. Shared mutable files and hidden shared chat are not collaboration contracts.

## Retries, artifacts and lifecycle

Retries start from a recorded baseline, with a new sandbox identity; failed sandboxes and logs may be retained under policy for diagnosis. Published artifacts are immutable or content/version addressed, verified for integrity and linked to producing attempt and inputs. Cleanup is lifecycle-managed and auditable, with retention appropriate to reproducibility and resource limits. Future adapters may use worktrees, containers, remote workers or microVMs while honoring the same contract; none is architecturally required.
