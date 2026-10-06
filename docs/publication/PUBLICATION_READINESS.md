# Publication readiness

**PUBLICATION_READINESS = READY**

**External status: PUBLICATION_READY — RELEASE IN PROGRESS**

R1 is accepted. The public source candidate is based on two identical allowlisted exports. The private source repository and its history are not included.

## Public/private boundary

The candidate excludes private Git history, operational evidence, credentials and administration modules. The release adds an Apache attribution `NOTICE` and a private vulnerability reporting form. The public identity is **Agentic Runtime**. The original project code is licensed under Apache-2.0; copyright attribution is **Copyright 2026 Jorge Pessoa**. No organization or other legal entity is inferred. Dependency licenses remain unchanged and are listed in `THIRD_PARTY_NOTICES.md`.

## Security reporting

`SECURITY.md` directs researchers to GitHub Private Vulnerability Reporting and says not to open vulnerability reports as public issues. `.github/VULNERABILITY_REPORT.yml` provides the custom structured form. The setting is enabled and verified as part of repository creation before the initial source push.

## Dependencies and distribution

The project source repository and current user install do not vendor or redistribute third-party wheels. A future project wheel declares dependencies rather than embedding them. No project wheel, container/image or executable is distributed here. Development uses upstream `psycopg[binary]` for portable setup; production uses system-backed psycopg with OS-provided `libpq` and TLS libraries. Platform-specific licensing review is required before a future distribution bundles dependencies.

## Candidate verification

The accepted R1 base export contains 155 files; independent builds had identical manifests. The updated candidate manifest hashes every source-candidate payload file except the manifest itself. `R1_RELEASE_DECISION.md` is added after publication with the resulting manifest hash, source commit, repository URL, reporting status and SBOM result. It is excluded from the export manifest to avoid self-reference, and its commit SHA is verified separately.

The candidate is rechecked for secrets, personal data, infrastructure data, symlinks, valid JSON and accidental Git history. The exact owner-authorized copyright attribution is permitted in the attribution-bearing files and is separately documented in the release decision. The source-only publication is not blocked by a pre-publication SBOM. An SPDX SBOM and dependency review are performed after GitHub recognizes the manifests; platform-specific review remains required before future bundled distributions.
