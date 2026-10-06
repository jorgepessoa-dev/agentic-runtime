# Cognitive adapter configuration

The remote worker loads operator-owned JSON profiles from `AGENTIC_RUNTIME_COGNITIVE_CONFIG`. Credentials stay outside that file: API adapters name an environment variable through `credential_env`, and the worker resolves it only at invocation. Keep the profile file readable only by the worker service account. The generic deployment template is `deploy/templates/cognitive-profiles.example.json`.

Supported adapters are `openai_compatible` (HTTPS JSON API, strict structured output, no tools, configured per-call ceilings and versioned price metadata) and `claude_code` (bounded CLI adapter with no tools and configurable executable/model alias). Route IDs, provider names, models, endpoints, roles and credential-variable names are configuration; there are no milestone route IDs, local credential-store paths or call-ledger paths in public runtime wiring.

For local quickstart and deterministic CI, install `uv sync --extra development --frozen`. The development extra selects prebuilt Psycopg binary wheels for portability without a compiler or local `libpq`. Production deployment uses the base dependency from `uv sync --frozen`: pure Psycopg linked at runtime to the operating system's `libpq` and TLS libraries, so system security updates service those libraries. This avoids embedding a second copy of network/TLS libraries in a production application. The pure implementation is slower than Psycopg's C implementation; deployments that need the C implementation may build `psycopg[c]` against the host's PostgreSQL development package after validating their target platform.

No provider is contacted by profile loading or deterministic tests. An actual configured task is required to make a provider request.
