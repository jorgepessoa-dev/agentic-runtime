# Execution Boundaries

Execution contracts describe capabilities and results without requiring a particular model, harness, vendor, process model or deployment location. Adapters are installed capabilities; agents are ephemeral invocations of those capabilities.

| Boundary | Contract responsibility |
|---|---|
| `HarnessAdapter` | Invoke an execution harness with a normalized request; report lifecycle, logs/trace references, outcome and failure. The harness is not the system's source of truth. |
| `ModelAdapter` | Expose model capability metadata and normalized request/result, including usage and error information; provider details stay behind the adapter. |
| `DecisionAdapter` | Provide a decision capability under a declared input/output contract and policy; it is not a privileged governance authority. |
| `SandboxAdapter` | Create and control an isolated attempt environment under filesystem, network, tool, secret and resource policy; return verifiable artifact references. |
| `ExecutionLocation` | Describe where an attempt can run and its capabilities, locality, trust and resource constraints. A task is independent of local, remote or hybrid placement. |

An Execution Router resolves task capability requirements and genome routing policy to eligible adapters and locations. The chain is conceptually:

```text
TaskAttempt → Execution Router → HarnessAdapter + ModelAdapter + DecisionAdapter
                                  └────────────── SandboxAdapter / ExecutionLocation
```

These are conceptual seams, not required separate services or one-to-one product mappings. A single implementation may satisfy multiple ports where authority remains clear. Routing decisions and adapter versions must be recorded for replay/comparison. Capability eligibility is policy-driven: role → required capabilities → routing policy → eligible executor/model. No role is permanently assigned a model or harness.

## Distributed execution

Milestone 5 validates a replaceable `RemoteWorkerExecutionAdapter` over a versioned HTTP/JSON protocol. The worker receives a short-lived bearer credential scoped by the control plane to capabilities, task types, tools, resource classes and local limits. It receives no database connection or governance credential. `worker_id` identifies the durable logical worker; each process registers a fresh `worker_instance_id`.

The control plane remains authoritative. A worker obtains a task only through a capability-filtered claim, executes the returned `TaskAttempt` under its lease epoch, renews through infrastructure heartbeats, uploads bytes by SHA-256, and submits a result reference. The control plane verifies current fencing, artifact content and output contract before acceptance. A reconnect never revives an expired attempt; the worker reports local inventory and the coordinator decides authority. The current local sandbox uses a private filesystem view, bounded tmpfs, process/resource limits and seccomp network denial. The M5 HTTP service binds only to loopback; deployment transport security is not inferred from this local validation.

PostgreSQL remains the durable queue and transactional outbox. Workers poll; the outbox resolves durable pull-availability hints idempotently rather than pushing authority to a worker. Control plane and worker restart do not make the worker a second coordinator. Transport and worker implementation remain replaceable; task, attempt, lease and artifact contracts do not encode a specific model or harness.

Adapter results are claims until verified against durable state and artifacts. Contracts should normalize cancellation, timeout, retryable/permanent failure, resource use and provenance. Future implementations may add adapters without changing task semantics or making a concrete technology mandatory.
