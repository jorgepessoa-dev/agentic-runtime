# Architecture Invariants

1. Mission governance, hard constraints and promotion evidence are outside autonomous mutation.
2. Role is a capability and policy mapping; role is never a model, provider or harness identity.
3. Models, harnesses, tools, sandboxes and execution locations are replaceable adapters, not sources of truth.
4. Agents are execution instances; durable work and knowledge live in explicit objects and events.
5. `TaskAttempt` is the isolation and identity boundary for execution; write-capable attempts do not share mutable workspaces.
6. A candidate, its evaluator and its promotion authority are distinct; no candidate self-promotes.
7. A completion claim is not proof. Completion requires persistent, verifiable state and artifacts.
8. Material evolution requires traceable lineage, reproducible evaluation and a rollback reference before promotion.
9. Generic runtime contracts contain no application-domain semantics; domain behavior enters through explicit adapters/contracts.
10. Execution is assumed at-least-once. Idempotency, fencing and deterministic reconciliation reject stale or duplicate effects.
11. Evaluation integrity is constitutional; each evaluation pins an independently governed, immutable EvalSuiteVersion that the candidate cannot select or change.
12. Exploration has an explicit configurable allocation and alternatives/negative results remain discoverable.

These invariants should become contract checks where practical. The normative source for mission and authority is in `governance/`; this list is the compact cross-system guardrail set.
