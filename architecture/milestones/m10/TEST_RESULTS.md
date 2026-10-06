# M10 Validation Record

**Status: ACCEPTED.** All mandatory M10 gates E01–E41 passed. No provider calls,
publication, push, package release or M11 work occurred.

## End-to-end E2 and holdout evidence

- `tests/integration/test_m10_e2_end_to_end.py`: PASS through fresh PostgreSQL and separate runtime, governance, evaluator, verifier and promotion identities.
- Lifecycle evidence: `evidence/m10/automated-e2-m9-cycle-20261006.json` plus independent reruns; canonical evidence hashes verified.
- Final adversarial/partition evidence: `evidence/m10/automated-e2-m9-holdout-adversarial-20261006.json`; canonical SHA-256 verified. It records zero provider calls, 12 paired DEVELOPMENT runs, one HOLDOUT run, invalid suite-version and forged evidence rejection, candidate-hash mismatch rejection, evaluator outage fail-closed behavior, and PostgreSQL interruption/recovery during automatic rollback.
- Candidate/source isolation: only same-scope champion hashes, DEVELOPMENT scenarios, completed DEVELOPMENT runs and accepted DEVELOPMENT artifacts are accepted as observation inputs. Runtime/evaluator RLS hides HOLDOUT scenarios, missions, results and artifact evidence; verifier records HOLDOUT; governance/promotion inspect it for decisions.
- V1 deficiency → structured proposal → frozen V2 → M9 development and holdout missions → evidence-derived metrics → independent comparison → promotion → post-promotion verification → regression → automatic V1 rollback.
- Fourteen M9 plans (12 paired DEVELOPMENT, one post-freeze HOLDOUT, one post-promotion check) produced 28 accepted attempts and 28 verified artifacts. Fifteen campaign reservations settled exactly once.
- Promotion concurrency yielded one winner; duplicate promotion/post-check were idempotent; late output was fenced; terminal work did not resurrect. Corrupted artifact bytes, forged evidence, wrong holdout version, candidate hash mismatch and invalid rollback target were rejected.
- PostgreSQL backend termination during rollback left champion V2 and no rollback record; replay of the durable failed post-check restored V1 and reconciled campaign accounting. This test exposed and closed an accounting/completion gap in duplicate post-check recovery.

## Regression and migrations

- Private regression: **259 PASS, 0 FAIL, 0 SKIPPED**.
- Public candidate profile: **160 PASS, 0 FAIL, 0 SKIPPED**.
- Fresh PostgreSQL in both profiles: **35 migrations; replay 0**.
- `git diff --check`: PASS.

## Publication Readiness regression

- Final public candidate: **146 manifested files** plus the manifest. An independent export has identical manifest and file hashes. The full public profile ran on the code-identical closure candidate; this final candidate was rebuilt after closure-record-only markdown updates and rescanned.
- Fresh `uv sync --frozen` and fresh development-extra frozen sync passed.
- Secret, personal-data and infrastructure-data scans: **0 findings** each; 4 JSON files validate; 0 symlink escapes.
- Clean candidate has no `.git` or private history. The separate public test copy used a synthetic local-only test commit because existing integration tests inspect Git state; it has no remote and is not the deliverable candidate.
- Private repository has no configured remote. Public readiness remains `PUBLICATION_READY — AWAITING OWNER APPROVAL`; no publication action occurred.

## Closure

All E01–E41 rows are PASS in `FAILURE_MATRIX.md`; G01–G13 are PASS in
`ACCEPTANCE_MATRIX.md`. Publication Readiness changes present in the initial
working tree were reconciled into this private M10 closure and included in the
allowlisted export scan. ADR-009 is Accepted. M10 is accepted; M11 is not
started.
