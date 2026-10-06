# Evolution Policy

## Two loops

The **inner loop** executes assigned work: task → attempt → artifact/evidence → task evaluation. The **outer loop** improves how work is selected and performed: observe → diagnose → hypothesise → mutate → evaluate → compare → promote, reject or retain. Inner-loop evidence can inform the outer loop; the outer loop cannot bypass governance or directly edit the active configuration.

## SystemGenome

A SystemGenome is a versioned, reviewable description of behavior-affecting configuration for one governed `scope_id`; scopes may be independently controlled (for example routing or context policy), and an aggregate/system scope is also possible. Exactly one champion is active per controlled scope. The design does not prescribe inheritance between scopes. A genome references (rather than embeds) topology, component configurations and policies. Over time it may describe role/capability mappings, routing, prompts, skills, tools, context, delegation, review/escalation, sandbox, budgets and concurrency. Evaluation integrity and the Mission Charter are not genome-editable. The minimal schema is extensible and does not prescribe a runtime database.

Each genome records identity, scope, parent genome(s), version, creation time/author, mutation description and rationale, evaluation references, lifecycle/promotion state and rollback reference. Lifecycle concepts: `DRAFT`, `CHALLENGER`, `CHAMPION`, `REJECTED`, `RETAINED_FOR_DIVERSITY`, `SUPERSEDED`. Multiple challengers may be evaluated concurrently within a scope.

## Tiers of change

| Tier | Scope | Autonomy boundary |
|---|---|---|
| E0 — Constitution | Mission, immutable governance, hard security and evaluation-integrity constraints | Never autonomously modified. A proposal goes to human governance. |
| E1 — Soft Genome | Prompts, skills, context policies, routing and low-risk budget adjustments | May propose and run bounded isolated trials under fixed evaluation and resource limits; promotion remains gated and independently authorized. |
| E2 — Workflow Genome | Topology, delegation, role structure, review/escalation and exploration strategy | Requires champion/challenger evaluation, independent review and explicit promotion policy. |
| E3 — Capability Genome | Tools, adapters, harness/sandbox implementations and runtime code | Requires stronger isolation, security review, reproducible tests and independent promotion. |
| E4 — Domain Genome | Future domain-specific capabilities and semantics | Outside the generic core; introduced only through explicit domain adapters/contracts and their own governance. |

Autonomy at a tier does not grant authority over E0 or the evaluator.

## Candidate lifecycle

1. **Observe:** collect outcomes, traces, resource use, failures and uncertainty with provenance.
2. **Diagnose:** identify opportunity, weakness, plateau or failure pattern; test for duplicated/biased evidence.
3. **Hypothesise:** create a durable `MutationProposal` referencing observations, hypothesis, target scope, base genome, bounded change, expected effect, risks, falsifiers and requested evaluation plan.
4. **Mutate:** produce a new genome from that proposal in an isolated candidate workspace; do not edit champion state.
5. **Evaluate:** an independent evaluator binds the run to a governed `EvalSuiteVersion` and records candidate/champion IDs, suite and evaluator versions, conditions, evidence/artifact references and results in an `EvaluationRecord`. The candidate cannot select or alter this binding.
6. **Compare:** create a `PromotionDecision` referencing the proposal, candidate, evaluation records, hard gates, multi-objective comparison and decision rationale.
7. **Decide:** promote, reject, or retain for diversity. Proposer cannot be sole evaluator/approver. Preserve artifacts, decision rationale, lineage and rollback reference.

No lineage or reproducible evidence means no promotion. Promotion atomically changes the champion reference for its scope and records the previous champion as rollback target. Rollback restores a known genome and does not erase the failed promotion or its evidence.

## Champion/challenger and evaluation

Each challenger runs in an evaluation environment isolated from production state. **Evaluation integrity**—independence, provenance, non-self-selection, protected evidence and promotion separation—is constitutional. **Evaluation definitions** (suite cases, metrics and procedures) are versioned and may evolve through a separate governed process. Every run is pinned to one immutable `EvalSuiteVersion` and evaluator version; candidates cannot modify, select, weaken or choose between suites for their own judgment. Use fixed regression sets, protected holdouts where useful, trace review, deterministic checks where possible, failure injection, downstream outcome measures, resource metrics and independent review as appropriate.

An observed evaluator weakness creates a separate `EvalSuiteChangeProposal`, outside the candidate's mutation proposal. An independent reviewer validates the successor against prior suites, known evidence and the identified weakness, checks for regressions and metric gaming, and authorizes the version change under governance policy. The successor becomes authoritative only after that review and decision; prior versions remain immutable and addressable. Existing evaluations keep their original suite binding. Thus G20 cannot change the suite judging G20; a separately governed proposal may validate EvalSuite v8 to supersede v7 for future evaluations.

Log metric definitions and evaluator versions. Detect metric gaming with multiple measures, regressions and qualitative review; do not make one scalar score sovereign. Promotion gates remain governance-controlled and cannot be weakened by a suite revision or candidate genome.

Rejected challengers and negative results remain durable evidence. A promising non-champion can be retained in a novelty archive with its rationale, provenance, applicability and evaluation status; retention is not activation.

## Exploration, exploitation and stagnation

Track separate exploitation and exploration budgets. Initial planning may start near 85–90% exploitation and 10–15% exploration, but values are configurable by scope and resource constraints. Exploration may compare different models, role mappings, topology, harnesses, tools, skills, context strategies and solution approaches. Selection policies must preserve a minimum exploration allocation unless governance explicitly changes it.

Telemetry should reveal repeated failures and retries, similarity of proposals, quality plateau, cost growth without quality gain, changing executor effectiveness, topology-specific errors, duplicate research and declining novelty. A stagnation diagnosis should trigger candidate proposals such as increasing exploration, heterogeneous routing, alternative topology/tooling or a fundamentally different hypothesis—not automatic unreviewed production change.

## Value and promotion principles

Evaluation considers verified quality, downstream value, useful novelty, gain over baseline, cost, latency, error/retry rates, regressions and resource consumption. Use hard constraints and Pareto comparisons; weights remain policy-dependent and evidence-informed. Domain evaluators may be plugged in without importing their semantics into the core. Fundamental invariants are gates, never score trade-offs.
