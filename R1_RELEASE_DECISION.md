# R1 Release Decision

**Status:** PUBLICATION_READY — PUBLISHED SOURCE R1

## Publication record

- Publication date: 2026-10-06
- Public repository: https://github.com/jorgepessoa-dev/agentic-runtime
- Visibility: public
- Default branch: `main`
- Published source commit: `a37cdb091e568994980bf90bc836d7ab53e459c5`
- Canonical candidate: `final-e`, the sanitized allowlisted source export
- Accepted R1 source baseline: `1b36ff66464bf57dca207e8168eb3432326bb47b`
- Accepted base export manifest SHA-256: `ad7e3da016b1daf4d66fca6bc7e4705edbc1fb635292ff728f37b7392b6bb6c9`
- Release candidate manifest SHA-256: `75da028a6954a646c3020664d19e707ae7328258b842ac2e4a910fe42bf5d2c2`
- Final candidate payload: 157 manifest-listed files; the manifest itself and this post-publication decision are separate. The Git commit binds the complete repository tree.

## Acceptance and licensing

- R1: ACCEPTED; all applicable R01–R42 gates are PASS in the accepted private acceptance matrix.
- M10: ACCEPTED. M11: NOT STARTED.
- License: Apache License 2.0 (`Apache-2.0`); GitHub recognizes the repository license as Apache-2.0. The `LICENSE` bytes match the official Apache text.
- Copyright: `Copyright 2026 Jorge Pessoa`, authorized by the owner.
- Attribution: `NOTICE` present with the requested Agentic Runtime and Jorge Pessoa attribution.
- No organization or other legal entity is claimed.

## Security and privacy verification

- `SECURITY.md`: present; vulnerabilities are directed to private reporting, not public issues.
- `.github/VULNERABILITY_REPORT.yml`: present and parsed as a valid private reporting form.
- GitHub Private Vulnerability Reporting: ENABLED and confirmed by the repository API before the source push.
- GitHub secret scanning and push protection: enabled.
- GitHub dependency graph and vulnerability alerts: enabled.
- Candidate scan: zero secret, infrastructure or unauthorized personal-data findings; the owner-authorized copyright attribution is the only personal-identity exception. Zero symlinks and valid JSON. No private history or operational evidence was published.

## Software bill of materials and dependency review

- SPDX SBOM: GENERATED after publication from GitHub's dependency graph, SPDX-2.3.
- SBOM assessment: **PASS WITH OBSERVATIONS**.
- The graph recognizes the `uv.lock` dependencies and CI action dependencies. GitHub's SBOM reports `NOASSERTION` for third-party package license expressions; `THIRD_PARTY_NOTICES.md` records the upstream-verified licenses and versions.
- GitHub reported zero open Dependabot alerts at verification.
- The source-only repository does not vendor dependency wheels or bundle containers/executables. The platform-specific component and LGPL relinking questions for `psycopg[binary]` remain a gate only before a future distribution bundles those components.

## Release scope

Publication contains source only. No GitHub Release, tag, package, container, executable or other binary artifact was created. No runtime architecture changes were made for publication. The public Git history starts with the sanitized candidate commit and does not contain the private source repository's history.

This decision record is a post-publication supplement and is intentionally excluded from `PUBLIC_EXPORT_MANIFEST.json` to avoid self-reference. Its contents are bound by the public Git commit.
