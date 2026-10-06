# Contributing

Contributions should keep the runtime domain-neutral and preserve its authority, durability, and isolation boundaries.

## Development setup

Use Python 3.12+, PostgreSQL for integration tests, and `uv sync --frozen`. Copy `.env.example` to an ignored local `.env`; keep all credential values blank unless a local disposable test requires them. Never use production or shared databases with destructive tests.

## Validation

Run the bounded static checks for product code with:

```sh
uv run ruff check src/agentic_runtime
uv run ruff format --check src/agentic_runtime/contracts/serialization.py src/agentic_runtime/coordinator/claim.py src/agentic_runtime/coordinator/transitions.py src/agentic_runtime/coordinator/reconciliation.py src/agentic_runtime/persistence/events.py src/agentic_runtime/remote/config.py
PYTHONPATH=src uv run mypy --follow-imports=skip src/agentic_runtime/contracts src/agentic_runtime/coordinator/state.py src/agentic_runtime/coordinator/claim.py src/agentic_runtime/coordinator/transitions.py src/agentic_runtime/remote/config.py
PYTHONPATH=src uv run python -m unittest tests.unit.test_architecture_fitness
```

The type-check profile is intentionally limited to low-level contracts, state,
and the small runtime services with explicit typed interfaces. Formatting is
enforced on newly introduced/extracted modules; existing compact modules are
not mass-formatted.

Run deterministic tests with:

```sh
uv run python -m unittest discover -s tests -p 'test*.py'
```

Database tests need a disposable database and the role-test setup described by the test helpers. Do not run provider-backed tests by default. Include migration replay and `git diff --check` in changes that affect persistence or documentation.

For the complete local public test profile, set `M4C_ADMIN_DATABASE_URL`, `M3_TEST_DATABASE_URL`, and `M4C_TEST_DATABASE_NAME` for a dedicated disposable PostgreSQL instance, then run `PYTHONPATH=src uv run python scripts/run_m4c_role_tests.py`. The helper recreates only the explicitly named database and creates temporary test identities. The checked-in CI workflow runs that same profile without provider credentials or private infrastructure.

## Engineering rules

- Keep domain semantics in explicit adapters and contracts, not in the generic core.
- Add migrations as forward-only, ordered files; test fresh installation and replay behavior.
- Preserve control-plane authority, lease fencing, idempotency, and explicit promotion boundaries.
- Add deterministic regression coverage for behavior changes and failure paths.
- Keep evidence reproducible and sanitized; never add raw production logs or private operational evidence.
- Do not add secrets, personal paths, real host identifiers, credentials, or infrastructure-specific defaults.
- Treat provider calls and deployment experiments as separately authorized operations.

Pull requests should explain behavior, migration impact, tests run, security implications, and any remaining limitations. Avoid unsupported production-readiness claims.
