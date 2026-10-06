# M10 Acceptance Matrix

**Status: ACCEPTED.** This matrix follows the frozen
M10 Charter gates. Evidence is zero-provider and uses the ordinary M9 runtime.

| Gate | Required proof | Current evidence | Status |
|---|---|---|---|
| G01 | Preserve the accepted M9 execution boundary and publication private/public boundary | M9 runtime was exercised through its normal control plane and remote workers; no provider calls or external publication actions | PASS |
| G02 | Record a genuine workflow observation before generating a structured E2 proposal | Persisted observation, source hash, and proposal lineage in `evidence/m10/automated-e2-m9-cycle-20261006.json` | PASS |
| G03 | Materialize a bounded V2 challenger with immutable parent and rollback lineage | Persisted V1/V2 genomes, content hashes, mutation operations, and parent lineage | PASS |
| G04 | Freeze development and holdout partitions, paired workloads, seeds, evaluator/suite identity, thresholds, and resource limits before execution | Versioned suite has three development scenarios and one holdout scenario; candidate is content-addressed before first holdout execution | PASS |
| G05 | Execute both genomes and holdout through real M9 accepted plans, tasks, attempts, workers, leases, epochs, fencing, and verified artifacts | Fourteen M9 plans: twelve paired development runs, one post-freeze holdout run, one post-promotion check; 28 accepted attempts and 28 verified artifacts | PASS |
| G06 | Derive metrics from persisted M9 evidence and require independent evaluation | Evaluator API accepts no caller metrics; validates artifact-store bytes, accepted task/attempt lineage, worker instance, lease epoch, recovery and hashes | PASS |
| G07 | Apply frozen paired comparison and promote only an eligible candidate | Comparison is independent; all frozen hard gates passed and the measured critical-path improvement met policy | PASS |
| G08 | Enforce separate authorization, atomic champion CAS, concurrency rejection, and idempotence | Two concurrent promotions yielded one decision; same-ID duplicate returned the same decision; conflicting request rejected | PASS |
| G09 | Run post-promotion M9 verification and automatically roll back an admissible regression | Frozen stress workload failed; V1 restored; injected PostgreSQL interruption left no partial rollback, and replayed durable post-check completed rollback and accounting | PASS |
| G10 | Recover across restart; reject stale/late output; keep terminal work terminal | Durable outbox and E2 reservation survived control-plane restart; stale result returned HTTP 409; cancelled queued work remained terminal after restart and duplicate cancellation | PASS |
| G11 | Reconcile E2 budget and preserve provenance/lineage exactly once | 15 of 15 campaign reservations (challenger plus 14 evaluation/post-check runs) settled once; evidence records suite/scenario partitions, hashes, M9 attempts, workers, lease epochs, artifacts, events, decisions and rollback IDs | PASS |
| G12 | Close required M10 failure-matrix cases and reconcile regression results | All E01–E41 are PASS in `FAILURE_MATRIX.md`; private 259/0/0, public 160/0/0, PostgreSQL 35 migrations/replay 0 | PASS |
| G13 | Rebuild and revalidate the final public candidate after the final E2 source changes | Final two 146-file exports have identical manifests; frozen installs pass; scans are zero-finding; no symlink escape, private history or remote; public profile 160/0/0 | PASS |

The evidence artifact's own SHA-256 is stored in its `evidence_hash` field and
can be recomputed from the canonical JSON payload with that field omitted.

M10 mandatory acceptance gates are complete. The private publication candidate
remains `PUBLICATION_READY — AWAITING OWNER APPROVAL`; no repository/package
publication or M11 work was performed.
