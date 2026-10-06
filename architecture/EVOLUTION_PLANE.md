# Evolution Plane

The Evolution Plane is the outer improvement loop. It can discover opportunities and prepare candidates; it cannot change the active champion in place.

```text
OBSERVE → DIAGNOSE → HYPOTHESISE → MUTATE → EVALUATE → COMPARE
                                                   ├→ PROMOTE
                                                   ├→ REJECT
                                                   └→ RETAIN FOR DIVERSITY
```

## Inputs and outputs

Inputs are versioned outcome evidence, traces, task/attempt results, resource consumption, evaluator reports, prior hypotheses and mutations, novelty records, current champion genome and governance policy. Evidence has provenance, scope, timestamps, evaluator/version references and uncertainty. Observations include negative results and failures.

Outputs are opportunity/weakness records; falsifiable hypotheses and evaluation plans; candidate SystemGenomes and mutation lineage; reproducible evaluation records; and a decision (promote/reject/retain) with rationale and rollback reference. A proposal records expected value, mechanism, risk, alternatives, resource request and how it may fail.

## Durable improvement chain

The minimum auditable chain is `Observation → MutationProposal → SystemGenome challenger → EvaluationRecord → Comparison/PromotionDecision → lineage and rollback reference`. The proposal references its observation(s), target scope and parent champion; the evaluation record pins candidate and champion, conditions, evidence/artifacts and `EvalSuiteVersion`; the decision references the proposal and evaluation records. These are conceptual durable object contracts, not a mandated database schema. Missing links or unversioned evaluation prevent promotion.

## Evaluation-suite evolution

Evaluation integrity remains constitutional while suite definitions evolve by version. Every run is pinned by an independent evaluator to an immutable `EvalSuiteVersion`; the candidate cannot choose or alter it. A discovered weakness creates a separate `EvalSuiteChangeProposal`, independently reviewed and validated against prior versions, known evidence and regressions before it is authorized to supersede the current suite for future runs. Old suite versions and records remain addressable. Suite evolution cannot silently weaken constitutional promotion gates.

## Mutation scopes

Mutations operate at governed tiers E1–E4: soft configuration; workflow topology/delegation; execution capability/runtime implementation; and future domain capability. E0 cannot be mutated autonomously. Each candidate changes a bounded scope where practical so results can be attributed and rollback remains clear.

## Champion/challenger lifecycle

For each controlled scope exactly one genome is champion. Any number of challengers may be drafted and evaluated in parallel. Each runs in an isolated evaluation environment with equivalent inputs, budgets and evaluator versions. The proposer cannot be sole evaluator or promotion authority. Predeclared hard gates and multi-objective comparison inform a separate promotion decision. Rejected candidates and evaluation traces remain available; alternatives with useful novelty may be retained without activation. Promotion records lineage and prior champion as rollback target.

## Stagnation response

Telemetry for repeated failure, repeated/similar proposals, plateau, rising cost without quality improvement, excessive retries, declining executor effectiveness, topology-specific failure, duplicated research and exploration collapse supports diagnosis. When stagnation is credible, the plane can shift configurable resources toward exploration and propose heterogeneous routing, different role mappings/topologies, tools, context strategies or fundamentally different hypotheses. Stagnation signals justify experiments, not automatic relaxation of safety or evaluation gates.

## Why production is not directly mutated

An active champion is relied on to produce governed outcomes; an untested in-place change destroys the baseline and obscures causality. Isolated candidates preserve the champion, enable equivalent comparison and reproducibility, and permit rollback. Promotion is an explicit, auditable change of active reference after evidence and independent authority. No lineage or no reproducibility means no promotion.
