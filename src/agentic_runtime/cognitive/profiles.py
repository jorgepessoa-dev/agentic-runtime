from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Mapping

from agentic_runtime.artifacts.store import ArtifactStore
from .api import OpenAICompatibleCognitiveAdapter, TokenPriceSchedule
from .cli_profiles import claude_code_profile
from .contracts import CognitiveCapabilities, UsageState


def load_profile_config(path: Path) -> list[dict[str, Any]]:
    """Load an operator-supplied, secret-free adapter configuration."""
    payload = json.loads(path.read_text(encoding="utf-8"))
    entries = payload.get("profiles") if isinstance(payload, dict) else None
    if not isinstance(entries, list):
        raise ValueError("cognitive config must contain a profiles array")
    ids: set[str] = set()
    for item in entries:
        if not isinstance(item, dict):
            raise ValueError("each cognitive profile must be an object")
        route_id = item.get("route_id")
        adapter = item.get("adapter")
        if not isinstance(route_id, str) or not route_id or route_id in ids:
            raise ValueError("cognitive route ids must be nonempty and unique")
        if adapter not in {"openai_compatible", "claude_code"}:
            raise ValueError("unsupported cognitive adapter type")
        common = {"route_id", "adapter", "provider", "model_family", "model", "supported_roles", "tools"}
        specific = ({"endpoint", "credential_env", "max_input_tokens", "max_output_tokens",
                     "max_call_cost_usd", "price_schedule"} if adapter == "openai_compatible"
                    else {"executable", "effort"})
        if set(item) - common - specific:
            raise ValueError("cognitive profile contains unsupported fields; credentials belong in the environment")
        if item.get("tools", []) != []:
            raise ValueError("cognitive adapters do not accept tools")
        ids.add(route_id)
    return entries


def configured_adapters(config_path: Path, *, artifacts: ArtifactStore,
                        worker_id: str, worker_instance_id: str) -> Mapping[str, Any]:
    adapters: dict[str, Any] = {}
    for item in load_profile_config(config_path):
        route = item["route_id"]
        roles = frozenset(item.get("supported_roles", [
            "hypothesis_generator", "alternative_generator", "adversarial_reviewer"]))
        if not roles or not all(isinstance(role, str) and role for role in roles):
            raise ValueError("supported_roles must be nonempty strings")
        provider = item.get("provider")
        model_family = item.get("model_family")
        model = item.get("model")
        if not all(isinstance(value, str) and value for value in (provider, model_family, model)):
            raise ValueError("provider, model_family and model are required")
        if item["adapter"] == "claude_code":
            adapters[route] = claude_code_profile(route_id=route, model_alias=model,
                executable=item.get("executable", "claude"), artifacts=artifacts,
                worker_id=worker_id, worker_instance_id=worker_instance_id,
                effort=item.get("effort", "low"), provider=provider, model_family=model_family)
            continue
        credential_env = item.get("credential_env")
        if not isinstance(credential_env, str) or not credential_env.isidentifier():
            raise ValueError("credential_env must name an environment variable")
        schedule_data = item.get("price_schedule")
        if not isinstance(schedule_data, dict):
            raise ValueError("API profiles require explicit price_schedule metadata")
        capabilities = CognitiveCapabilities(route_id=route, provider=provider,
            model_family=model_family, requested_model=model, supported_roles=roles,
            structured_output=True, tool_use=False,
            max_input_tokens=int(item.get("max_input_tokens", 8192)), concurrency_limit=1,
            supports_cancel=False, cost_state=UsageState.UNKNOWN,
            declared=frozenset({"strict_json_output", "no_tools"}))
        schedule = TokenPriceSchedule(schedule_id=schedule_data["schedule_id"],
            input_usd_per_million=float(schedule_data["input_usd_per_million"]),
            output_usd_per_million=float(schedule_data["output_usd_per_million"]),
            source_ref=schedule_data["source_ref"], observed_at=schedule_data["observed_at"])
        adapters[route] = OpenAICompatibleCognitiveAdapter(capabilities=capabilities,
            endpoint=item["endpoint"], model=model,
            credential_resolver=lambda name=credential_env: os.environ.get(name, ""),
            price_schedule=schedule, artifacts=artifacts, worker_id=worker_id,
            worker_instance_id=worker_instance_id,
            max_input_tokens=int(item.get("max_input_tokens", 8192)),
            max_output_tokens=int(item.get("max_output_tokens", 256)),
            max_call_cost_usd=float(item.get("max_call_cost_usd", 0.01)))
    return adapters
