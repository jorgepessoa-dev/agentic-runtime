# Agentic Runtime

Agentic Runtime is an experimental, domain-neutral Python runtime for durable task execution, governed coordination, and controlled system evolution. PostgreSQL stores authoritative task, lease, attempt, artifact, and evolution state. Replaceable workers execute bounded tasks through explicit capability and policy contracts.

It is infrastructure for building agentic systems, not a ready-made domain application or an unrestricted autonomous agent. The current codebase is an evolving research and engineering project; operational maturity, compatibility guarantees, and production suitability are not claimed.

## Architecture

- **Coordinator and control plane:** persist tasks, dependencies, leases, attempts, and decisions.
- **Workers and adapters:** run isolated work locally or through a versioned remote protocol.
- **Artifacts and evidence:** use content-addressed storage and durable lineage.
- **Governance:** evaluate proposed changes against independent constraints; promotion remains an explicit policy decision.
- **PostgreSQL:** acts as the durable authority. Workers do not receive database credentials.

See `architecture/` and `governance/` for the concepts and invariants.
Illustrative, non-production systemd and environment templates are in `deploy/templates/`.

## Requirements

- Python 3.12 or later
- PostgreSQL 15 or later for integration tests and runtime persistence
- `uv` for reproducible dependency installation (or an equivalent Python environment)

No paid model or provider credential is needed for the deterministic test suite. Provider-backed adapters require separately configured credentials and are not part of the default test profile.

## Setup

```sh
uv sync --extra development --frozen
cp .env.example .env
```

`.env` is local configuration and must never be committed. Configure `DATABASE_URL` only for a disposable local database when running integration tests. The example intentionally leaves credentials blank.

## Tests

```sh
uv run python -m unittest discover -s tests -p 'test*.py'
```

Database integration tests require an explicitly configured disposable PostgreSQL database. Follow `CONTRIBUTING.md` for the role-based test runner and migration checks. Never point destructive test helpers at a shared or production database.

The reproducible public test profile is `PYTHONPATH=src uv run python scripts/run_m4c_role_tests.py`. It requires administrator access to a disposable local PostgreSQL instance and recreates only the explicitly named `M4C_TEST_DATABASE_NAME` database. The CI workflow uses an ephemeral PostgreSQL service.

## Worker and provider model

Workers receive scoped, expiring authority and execute through adapters. Remote workers use the control-plane protocol and fencing checks. Cognitive/provider execution is optional and configured outside source control; credentials are resolved at runtime and should be delivered through an operating-system or deployment secret store. See `docs/COGNITIVE_ADAPTERS.md` for generic profile configuration and production/development Psycopg profiles.

## Governance and security

Task execution, artifact acceptance, cancellation, and evolution have separate state transitions and authority boundaries. See `governance/` for the normative model and `SECURITY.md` for vulnerability reporting and secret handling. This software is experimental; review isolation, network exposure, database privileges, and recovery controls before any deployment.

## License and publication status

Licensed under the Apache License, Version 2.0. See `LICENSE` and `NOTICE`. Third-party dependencies retain their own licenses; see `THIRD_PARTY_NOTICES.md`. Copyright 2026 Jorge Pessoa.
