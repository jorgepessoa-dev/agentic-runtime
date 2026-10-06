# Architecture

## Context

```text
               ┌──────────────────────────────┐
               │ Governance Plane             │
               │ Mission · constraints · gates│
               └──────────────┬───────────────┘
                              │ bounds
                              ▼
┌─────────────────┐   ┌───────────────────────┐
│ Goal /           │──▶│ Evolution Plane       │
│ Opportunity Plane│   │ proposes genome drafts│
└────────┬────────┘   └──────────┬────────────┘
         │ tasks                 │ candidate config only
         └────────────┬──────────┘
                      ▼
              ┌───────────────┐       ┌───────────────────┐
              │ Runtime Plane │──────▶│ Execution Plane   │
              │ work/evidence │       │ replaceable ports │
              └───────▲───────┘       └─────────┬─────────┘
                      │                         │ attempts/artifacts
                      └──── evaluation/evidence┘
                              │
                              └──── informs next evolution cycle
```

## Planes and components

- **Governance:** Mission Charter, immutable constraints, autonomy tiers, evaluation integrity, promotion rules and resource principles.
- **Goal/opportunity:** a graph of mission → strategic goals → opportunities/weaknesses → hypotheses → experiments → tasks. It should eventually rank expected value of information and outcomes, detect neglected opportunities and stagnation, and state uncertainty. It does not alter mission.
- **Evolution:** observes evidence and proposes versioned SystemGenome candidates. It manages mutation hypotheses and candidate lifecycle but never edits the active genome directly.
- **Runtime:** future source of durable task, dependency, attempt, lease, budget, policy, event, artifact and knowledge-object state; schedules work and invokes configured routes. No runtime is implemented in this milestone.
- **Execution:** routes an attempt through capability-matched model, harness, decision, sandbox and location adapters. It returns structured results and artifact references; adapters are replaceable.
- **Evaluation/promotion:** applies an independently selected, immutable `EvalSuiteVersion` and predeclared gates, compares challenger to champion, records decision and controls the active champion reference. A separate governed path validates suite successors.

## Information and control flow

Goals produce tasks and experiments. Runtime grants bounded attempts. Each attempt gets an identity, lease/fencing context, resource budget and isolated sandbox. Execution emits events, artifacts and evidence with provenance. Evaluators interpret evidence against the fixed evaluation plan. Results feed goals and evolution; an approved promotion changes only the configuration reference for the applicable scope. Durable collaboration uses task/artifact/evidence/knowledge/event/decision objects and explicit handoffs. Prefer references, structured summaries and targeted extracts over repeated raw context.

Control authority is split: governance bounds proposals and execution; runtime enforces policy; evaluation reports evidence; promotion applies an authorized decision. An agent's “done” is not a state transition unless verified evidence and durable state support it.

## SystemGenome and replaceable boundaries

The genome identifies configurable components and policies, not hidden source assumptions. Role topology, capability requirements, routing, context, delegation, review, budgets and evaluation references can evolve. Roles can be added, removed, split or merged without assigning a permanent model identity. See `schemas/system_genome.schema.json` and `architecture/EXECUTION_BOUNDARIES.md`.

## Evolution and runtime

The runtime executes the active champion and isolated challengers. The Evolution Plane may read runtime evidence and create candidate genomes, but has no write path to active configuration. Candidate, evaluator and promotion authority remain distinct. Evaluation runs pin suite versions; evaluation-suite changes use a separate independent validation path. Failed candidates stay available for learning; diversity retention never means production activation.

## Deployment neutrality

An execution location is a policy-selected capability: local process, another host, remote service or ephemeral worker. Task contracts do not encode location. Local development, remote deployment and hybrid execution should share the same task/attempt/artifact contracts. The architecture requires no specific distribution mechanism, database, model provider or harness.
