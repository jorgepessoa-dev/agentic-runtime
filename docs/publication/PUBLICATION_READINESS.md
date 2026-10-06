# Publication readiness

**PUBLICATION_READINESS = READY**

**External status: PUBLICATION_READY — PUBLISHED SOURCE R1**

Agentic Runtime R1 was published as a source-only repository on GitHub on 2026-10-06. The public repository starts from a sanitized export and does not contain the private source repository's Git history.

## Public/private boundary

The public candidate excludes private Git history, operational evidence, credentials and administration modules. The public identity is **Agentic Runtime**. The original project code is licensed under Apache-2.0. Copyright attribution is **Copyright 2026 Jorge Pessoa**, as authorized by the owner; no organization or other legal entity is inferred. The full Apache license is in `LICENSE`, attribution is in `NOTICE`, and third-party components retain their own licenses in `THIRD_PARTY_NOTICES.md`.

## Security reporting

GitHub Private Vulnerability Reporting is enabled and verified. `SECURITY.md` directs researchers to the private reporting channel and says not to open vulnerabilities as public issues. `.github/VULNERABILITY_REPORT.yml` defines the structured private report form. GitHub secret scanning, push protection, dependency graph and vulnerability alerts are enabled.

## Dependencies and distribution

GitHub recognizes the Python lockfile and CI workflow dependencies. Its SPDX-2.3 SBOM contains the project, repository, Python dependencies and GitHub Actions. GitHub currently emits `NOASSERTION` for third-party package licenses; `THIRD_PARTY_NOTICES.md` records the licenses verified from upstream metadata and repositories. Open Dependabot alerts: zero at publication review. The source repository does not vendor dependency wheels, containers or executables.

The source-only publication is not blocked by platform-specific components of the development `psycopg[binary]` wheel because the project does not redistribute that wheel or bundle its native libraries. Per-platform SBOM, license/NOTICE and LGPL relinking review remains required before any future image or executable distribution that bundles dependencies.

## Release verification

The final allowlisted candidate manifest hashes the sanitized source payload. The post-publication `R1_RELEASE_DECISION.md` records the base and release manifest hashes, repository URL, source commit, privacy scans, SBOM status and security settings. It is excluded from the export manifest to avoid self-reference; the public Git commit binds the complete release record.

The published default branch is `main`. No package, tag or GitHub Release was created. No runtime architecture change was made as part of publication governance.
