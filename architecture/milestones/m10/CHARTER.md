# M10 Charter — Governed Workflow Genome Evolution

**Status: Frozen specification; M10 accepted.** Baseline code commit: `e8a23cc8c21c9e0a95c21a15034f01e722e56a10`. The checked-out tree also contained uncommitted Publication Readiness work; it was preserved, scanned as part of the public candidate, and reconciled into the private M10 closure. M9 remains Accepted; no M9 regression is evidenced.

## Problem

M9 durably validates, versions, materializes, executes, verifies, repairs and recovers bounded plan DAGs. The evolution plane already has generic genome lineage, observations, mutation proposals, immutable evaluation suites, E1 challenger evaluation, promotion separation and rollback. However, the autonomous M7 campaign path is explicitly E1-only, and M9 plans are not governed as workflow-genome challengers. M10 closes this gap with bounded E2 workflow changes evaluated through the existing M9 plan runtime and existing evolution lineage principles.

## Scope and claim

M10 tests whether the runtime can govern bounded changes to **workflow configuration**: a versioned M9 plan template and its declared topology, capability contracts, verifier requirements, retry/repair structure, context/artifact references and resource envelope. The accepted claim is limited to the configured scope and deterministic evidence actually produced. It does not claim arbitrary self-improvement.

E2 is the only autonomous candidate tier. Each E2 genome references one canonical, schema-validated M9 plan template by immutable content digest and exact plan contract version. Existing `evolution.system_genomes`, `genome_parents`, `mutation_proposals`, `eval_suite_versions`, `evaluation_records`, `promotion_decisions`, and M9 plan-version/task-graph records are reused where their constraints permit. M10 adds only E2-specific durable records/contracts required for exact proposal deltas, frozen paired evaluation, lifecycle/accounting, authorization, and recovery; it does not mutate M9 accepted plan history.

The candidate receives no authority to alter evaluator code or suite, baseline, holdout, acceptance thresholds, promotion policy, audit evidence, rollback target, E0 constitution, M9 runtime implementation, deployment configuration, or public/private publication policy. E0, E3 and E4 mutation are rejected. E1 remains under its existing M7 policy and is not broadened by M10.

## Explicit non-goals

- Runtime source, tool, adapter, sandbox or evaluator-code mutation (E3).
- Domain-specific workflow semantics or domain genomes (E4).
- Mutation of governance, evaluator integrity, suite definitions, thresholds, budgets hard caps, or promotion authority (E0).
- Real provider calls, provider-backed proposal generation, or paid evaluation. The M10 real-call budget is **zero**.
- M9 redesign, unbounded planning, black-box optimization, or learned promotion scoring.
- Publishing, changing repository visibility, pushing, package/release creation, or altering publication approval state.
- Deployment of M10 before local contracts, migrations, deterministic failure matrix and regression gates pass.

## Architecture delta M9 → M10

M9 accepted immutable plan versions are the executable workflow representation. M10 introduces an E2 scope policy and a bounded evolution campaign around that representation. A deterministic proposal validator accepts only explicit structural JSON-pointer deltas: task/decomposition and dependency/delegation topology, verification topology, fan-out/fan-in, retry/repair/replan and scheduling policy, bounded concurrency, and already-governed workflow budgets. New nodes must reuse an existing frozen task contract. E2 cannot change task semantics, domain schemas/entities/rules/adapters/tools, provider configuration, runtime/migrations, evaluator/suite, promotion/governance authority, constitution, or publication boundary. Validation is by schema and capability surface; rationale or metadata text has no authority to expand the allowlist. The validator verifies the exact parent/candidate diff and M9 plan validity before materializing a separate challenger template.

Each immutable, versioned evaluation suite has explicit DEVELOPMENT and HOLDOUT scenarios with content hashes and provenance. Row-level security keeps HOLDOUT definitions, missions, results and artifact evidence out of runtime/evaluator reads; those roles can read the DEVELOPMENT partition. Observation intake resolves each source reference to a same-scope champion hash, DEVELOPMENT scenario, completed DEVELOPMENT run, or its accepted artifact evidence. Candidate generation can consume only development scenarios/evidence and permitted aggregate diagnostics. The candidate genome is frozen and content-addressed before any holdout mission is materialized. Mutation/candidate generation, holdout verification, and promotion are separate capabilities. Every result binds suite/version, scenario/partition, workflow hash and M9 mission evidence. Holdout inputs and outcomes cannot be fed back into mutation proposals.

Evaluation freezes the champion and candidate digests, paired workload IDs, environment/capabilities, evaluator and suite versions, seeds, hard gates and resource ceilings before either side runs. Challenger and champion executions are shadow/evaluation missions with no production/domain side effects. An independent evaluator records repeated paired outcomes; deterministic workers are used initially.

Use hard safety/correctness gates, then lexicographic comparison: (1) accepted completion and independent verifier correctness must not regress; (2) robustness/recovery hard gates pass; (3) the registered primary operational metric improves by its pre-registered minimum; (4) tie-break by fewer retries, then lower measured resource use. Unknown cost remains unknown and cannot be scored as zero. There is no scalar utility and no candidate-selected metric.

Promotion requires complete frozen repetitions, all hard gates, independent review, and a separately authorized governance capability. Promotion uses a transaction/CAS against the expected champion and immutable rollback reference. M10 can exercise the normal controlled promotion path in a test/evaluation scope only; no unbounded production activation. A bounded post-promotion check can trigger automatic rollback to the retained prior champion. Duplicate promotion/rollback requests are idempotent and conflicting reuse fails closed.

## Bounds and lifecycle

Initial M10 demonstration bounds: one governed scope; at most two concurrent challengers; at most three retained challenger versions per campaign; one frozen suite version; at least three paired deterministic workload cases; at most two repetitions per candidate/case; one promotion attempt per evaluation; one rollback; maximum 7 workflow nodes per plan; M9 hard limits remain in force (depth 2, 3 children/node, 6 descendants, 3 concurrent descendants, 2 retries/node, 120 seconds/mission, 1 MiB/artifact). Evolution campaign budgets are reserved before dispatch and settled exactly once. Unknown monetary usage remains UNKNOWN. Duplicate canonical mutation content is deduplicated against active and retained history; rejected candidates cannot be reevaluated without a new parent, suite or evidence binding. Terminal campaigns cannot create additional work.

Lifecycle is durable: observation → proposal → challenger → frozen evaluation → comparison → authorized promotion or reject/retain → post-promotion check → rollback if required. Candidate artifacts, negative outcomes, prior champions and lineage are immutable and retained under existing artifact GC protections. Restart recovery resumes or terminates only from persisted state; it never infers a passing evaluation.

## Trust boundaries

Proposer, candidate executor, evaluator/reviewer, promotion authority and runtime coordinator are distinct principals/capabilities. Logical role labels do not imply process or model identity. Candidate task workers cannot access evolution administration, evaluator/promotion APIs, PostgreSQL, campaign budgets, champion pointers, or hidden holdout inputs. The deterministic evaluator binds results to accepted task/attempt/worker/epoch evidence and immutable artifacts. Promotion authority revalidates suite, policy, champion CAS, repetitions, hard gates, budget and rollback target transactionally.

## Evaluation and evidence

The initial claim is deterministic E2 workflow evolution; fake/deterministic workers and frozen public-neutral test workloads suffice, so no provider call is authorized or needed. Each evaluation records candidate/champion genome IDs and hashes, parent, proposal/delta hash, plan contract version, immutable suite/workload hashes, evaluator revision, seed/repetition, capabilities/environment, task/attempt/worker/lease references, verified artifact digests, all metric values and unknowns, gate results, accounting reservations/settlements, independent evaluator identity, comparison, authorization, champion CAS, post-check and rollback lineage. Evidence must be reconstructable from persisted rows/events/artifact manifests. Never synthesize PASS from returned prose.

## Failure matrix and acceptance gates

`FAILURE_MATRIX.md` is the derived initial matrix and must be reconciled to the final implementation before any distributed experiment. M10 cannot be Accepted unless all mandatory gates pass with deterministic evidence:

1. Explicit immutable E2 workflow genome over valid M9 plan templates; parent lineage and hashes are verified.
2. Structured bounded proposal/delta validation; exact diff; E0/E1/E3/E4 and forbidden-field mutation rejected.
3. Stale parent, duplicate mutation, hash mismatch, malformed plan and over-budget proposals fail closed.
4. Champion/challenger isolation; one champion/scope; candidate cannot read/alter evaluator, suite, baseline, holdout, policy, rollback, or promotion authority.
5. Frozen, versioned, paired evaluator inputs and deterministic reproducibility across repeated runs.
6. Independent evaluation/review; insufficient or partial repetitions never promote; unknown cost stays unknown.
7. Reservations, limits, accounting, idempotency and terminal-campaign behavior survive restart and failures.
8. Promotion authorization is independent; concurrent/stale/duplicate promotion is CAS-safe and auditable.
9. Prior champion and rollback target remain immutable; post-promotion regression triggers automatic rollback; duplicate/manual rollback is safe and restart-durable.
10. Stale result, duplicate result, worker loss, outbox replay, DB interruption and artifact corruption cannot create accepted evidence or duplicate accounting.
11. One end-to-end domain-neutral deterministic evolution lifecycle proves proposal → challenger → repeated paired evaluation → independent comparison → authorized promotion → post-check → injected regression → rollback.
12. Failure matrix substantially closed, no unexplained FAIL/SKIP, fresh PostgreSQL/all migrations/replay 0, complete regression, JSON/hash/artifact/provenance/secret/domain-neutrality audits, and deployment/invariant audit if deployment is later justified.
13. Publication Readiness remains intact: reproducible allowlisted candidate; zero secret, personal-data and infrastructure-data findings; no private operational evidence, provider profiles, credential paths or infrastructure dependencies; fresh `uv sync --frozen`; full public test profile; fresh PostgreSQL/all migrations/replay 0; domain-neutrality and symlink-escape scans; no remote; no private `.git` history in candidate. M10 adds no publication or external release action.

No M9 mandatory gate is reopened absent an objective regression. The previous PUBLICATION_READINESS claim was based on a separate candidate; because its current source work is uncommitted, M10 close must rebuild and revalidate from the actual final tree.

## Deployment, rollback and soak

Develop locally first with additive migration and disabled-by-default E2 APIs/policy. Do not deploy until deterministic schema/recovery tests and full regressions pass. If remote deployment is later necessary to support an explicitly frozen M10 gate, use existing M9 backup, restricted bridge, mTLS and loopback PostgreSQL controls; no new privilege or provider route. Disable E2 admission to roll back application behavior; retain immutable rows and use forward-compatible migration recovery. A soak is required only if repeated campaign execution reveals a measurable accumulation/recovery risk; justify its duration from failure and resource evidence rather than defaulting to two hours.

## Repository/publication baseline note

At M10 start, the supplied clean-tree/publication-candidate baseline did not match this checkout: HEAD matched the stated baseline commit but Publication Readiness files were uncommitted. Those changes were preserved, scanned in the final public candidates, and reconciled into the accepted private M10 closure commit.

Closure record: all mandatory E01–E41 failure cases, private/public full
regressions and publication-readiness checks passed. Final evidence is indexed
in `ACCEPTANCE_MATRIX.md`, `FAILURE_MATRIX.md`, and `TEST_RESULTS.md`.
