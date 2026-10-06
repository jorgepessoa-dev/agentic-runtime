# ADR-009: Governed workflow genome evolution

- **Status:** Accepted
- **Date:** 2026-10-06

## Context

M9 provides immutable, bounded plan DAGs, execution, verification, repair, recovery and fencing. The evolution plane provides generic genome lineage and E1 evaluation/promotion controls, but autonomous campaign mutation is explicitly E1-only. The SystemGenome schema already permits versioned component/policy references, and M9 plan versions provide an executable representation of workflow topology. M10 addresses the missing governed E2 bridge without granting authority over evaluator integrity or runtime implementation.

## Decision proposed

Adopt the frozen scope in `architecture/milestones/m10/CHARTER.md`. Represent an E2 genome as immutable lineage metadata plus a content-addressed, schema-validated M9 workflow plan template. Use structured allowlisted deltas; bind challengers to a fixed parent; evaluate champion and challenger on frozen paired deterministic workloads; require independent evaluation and promotion authority; use compare-and-swap champion movement, bounded post-promotion checks and immutable rollback. Reuse existing evolution/M9 contracts where possible and add only required E2-specific storage and policy. No real provider calls are authorized.

E0 governance/evaluator integrity, E1 policy, E3 runtime/capability code and E4 domain semantics remain outside E2 mutation authority. No publication or M9 redesign is in scope. The Publication Readiness boundary is a mandatory regression gate.

## Trust boundaries

Candidate, proposer, evaluator/reviewer, coordinator and promotion authority are distinct capabilities. Candidate output is inert data. It cannot change the suite, evaluator, baseline, holdout, thresholds, budget ceilings, promotion policy, rollback target, champion pointer, runtime code or public/private boundary. PostgreSQL records and immutable artifact hashes remain authoritative.

## Consequences

If accepted after all M10 gates pass, the bounded claim is that the runtime can govern workflow-genome evolution under immutable evaluation and promotion constraints. It does not imply arbitrary self-modification. Rejected candidates and failed evaluations remain auditable; prior champions remain rollback targets. E3 and E4 evolution remain deferred.

## Deferred

Stochastic/provider-backed candidate generation, learned workflow search, autonomous evaluator evolution, broader production rollout, runtime-code mutation and domain genomes.

## Acceptance record

Accepted after M10 gates E01–E41 passed. Evidence and regression results are
recorded in `architecture/milestones/m10/ACCEPTANCE_MATRIX.md`,
`architecture/milestones/m10/FAILURE_MATRIX.md`, and
`architecture/milestones/m10/TEST_RESULTS.md`. The accepted claim is limited to
the deterministic, structurally bounded E2 workflow-genome lifecycle through
M9 with isolated development/holdout evaluation, independent promotion, and
automatic rollback. No provider call or publication was part of acceptance.
