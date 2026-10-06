# Authority Model

Authority is separated into five verbs:

| Authority | Meaning | Typical holder |
|---|---|---|
| **Govern** | Set mission, immutable constraints, evaluation integrity, evaluation-suite version authority and autonomy ceilings | Human governance; not an autonomous candidate |
| **Propose** | Identify opportunities and submit a genome mutation or a separate evaluation-suite change proposal with rationale | Evolution process or authorized contributor |
| **Execute** | Run a task/attempt under a granted policy, budget and sandbox | Runtime through execution adapters |
| **Evaluate** | Apply a fixed evaluation plan and report evidence | Independent evaluation capability/process |
| **Promote** | Change the active champion reference after gates pass | Promotion controller under governance policy; material changes require independent authorization |

These are capabilities, not permanent agent personalities. A process may hold more than one for low-risk work only where policy explicitly permits it; for material self-modification, the proposer cannot be the sole evaluator or promotion authority. Evaluation-suite changes are separately reviewed and authorized, and do not retroactively change prior evaluation records. Governance is never delegated to a candidate.

## Tier boundaries

- **E0:** autonomous components may observe and propose; governance changes require human decision.
- **E1:** bounded autonomous proposal and isolated execution/evaluation may be allowed under fixed gates. Promotion follows a preauthorized policy with independent checks and auditable records.
- **E2:** independent review is required in addition to comparative evaluation; promotion authority is separate from proposal.
- **E3:** execution is further isolated; security/code review and reproducibility checks precede promotion, with explicit authorization under the applicable governance policy.
- **E4:** authority is defined by the future domain adapter's governance; it cannot change generic E0 rules.

The runtime must make authority and scope explicit in durable decisions/events. No hidden privilege escalation, self-approval, or metric-driven override is valid. This lightweight separation exists to prevent self-promotion and evaluator gaming, not to create an organizational bureaucracy.
