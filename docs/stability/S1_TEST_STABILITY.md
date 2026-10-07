# S1 — CI and Test Stability

This report documents the S1 fixture correction and the validation evidence. Public CI runs are tied to the public correction commit.

## Incident and classification

The variable gate is `E2GovernedM9ExecutionTests.test_observe_mutate_execute_promote_postcheck_and_auto_rollback`. It uses the frozen M10 repeated-latency range/mean tolerance of 0.50. The failing CI log records `measured repeated latency exceeds the frozen repeatability tolerance` during `store.compare`; it reports 167 tests and one error. The subsequent CI run on public baseline `c8844e46d57a217c7bcc7ed6cbb95ce54c7eb1db` passed as run `37516265433`, without a code change between those two test outcomes. The failed log did not retain the individual latency samples or runner resource snapshot, so the exact outlier and its runner-level trigger cannot be reconstructed.

The test is **correctness plus performance**, and the failure mechanism is a **test-measurement sensitivity**. M10's authoritative `latency_ms` remains the real elapsed makespan from accepted M9 attempt timestamps. Promotion, the 5% minimum-improvement rule, repeatability, and post-promotion rollback continue to use that observed value. A deterministic expected-plan duration is only a diagnostic and never an evaluator input.

## Cause and fixture correction

The M9 fixture maps each `work_unit` to 100 ms of actual worker execution. Before S1, the shortest candidate had 100 ms of expected worker work and the serial champion had 200 ms. In the unchanged baseline's 20-run campaign, their observed medians were 264 ms and 464 ms respectively: diagnostic infrastructure overhead was about 164 ms and 264 ms. For the shortest case, process dispatch, database persistence, and worker coordination therefore contributed more time than the configured worker work. The historical CI log does not identify which source produced its individual outlier; the evidence supports short-workload sensitivity to accumulated runtime overhead, not a proven runtime race.

The test fixture now uses 4, 5, and 5 work units for its development scenarios and 5 for the hidden stress scenario. The runtime mapping remains 100 ms per unit. The shortest expected useful makespan is therefore 400 ms for the parallel candidate and 800 ms for the serial champion; observed median overhead remained about 164 ms and 270 ms. Across the final 50-run campaign, overhead was 169 ms median and 275 ms p95 over all persisted evaluation samples. Thus useful work is about 2.4–3.7 times the median overhead, whereas the former shortest workload was shorter than that overhead. Each worker delay remains bounded at 2.5 seconds, below the 5-second per-task contract.

No M10 policy, threshold, runtime implementation, promotion rule, rollback rule, dependency, or workflow behavior changed. The test harness now emits the persisted observed samples, spread, and expected-plan/overhead diagnostics if the repeatability gate rejects a run; diagnostics do not affect that decision. The negative control asserts that the existing 5× post-promotion stress workload exceeds the frozen 5% regression boundary using real `latency_ms`, while success, verification, and recovery gates pass, and automatic rollback restores V1.

## Local validation

The pre-correction target campaign passed 20/20 runs (120 paired observations); its largest spread was 4.64%. After fixture scaling, the final target campaign passed 50/50 runs (300 repeated pairs, 650 persisted evaluation measurements), with no retries or skipped runs. Spread median was 0.32%, p95 1.13%, p99 2.50%, and maximum 5.83%; all remain below the frozen 50% bound. Observed `latency_ms` had median 668 ms, p95 1,274 ms, and p99 1,281 ms. The post-promotion latency negative control was detected in all 50 runs.

The final E2/M10 integration and contract group passed 10/10 runs. The complete private suite passed 266 tests with no failures or skips; a fresh PostgreSQL instance applied 36 migrations and replayed zero. Ruff, the documented six-file Ruff format scope, progressive mypy, and seven architecture fitness tests passed. `git diff --check` passed.

For each repeated run, PostgreSQL was initialized from zero, the role-based migration/test harness ran against it, then the server was stopped and its data directory removed. Publication evidence remains separate from this public report. No provider calls were made.

## Public candidate and residual limits

Two independent allowlisted exports from the private canonical source produced identical 156-file trees and matching SHA-256 manifests. That candidate contains no `.git`, private `evidence/`, or symlinks. Secret, personal-data, infrastructure-data, and JSON checks reported zero findings. A fresh `uv sync --extra development --frozen` succeeded without changing dependency manifests. The complete public profile passed 167 tests with 0 failures and 0 skips on fresh PostgreSQL: 36 migrations, replay 0. The public GitHub branch contains separate R1 release, vulnerability-reporting, and CI hardening files; S1 changes are applied on top of that accepted public baseline so those files remain intact.

The failing CI artifact lacked per-pair measurements and a runner resource snapshot. The fixture correction addresses the measured sensitivity mechanism, but cannot identify the exact historic CI pause. CI outcomes are preserved against the public correction commit; the historical failure is retained and is not replaced by subsequent passing results.
