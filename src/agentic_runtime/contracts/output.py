from __future__ import annotations

import json
from typing import Any, Mapping


class OutputContractError(ValueError):
    pass


def validate_output(content: bytes, contract: Any) -> None:
    """Validate JSON output against the small schema subset used by the runtime."""
    if not isinstance(contract, Mapping) or "schema" not in contract:
        if not content:
            raise OutputContractError("output is empty")
        return
    try:
        value = json.loads(content)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise OutputContractError("output is not valid JSON") from exc
    _validate_schema(value, contract["schema"], "$output")


def _validate_schema(value: Any, schema: Mapping[str, Any], path: str) -> None:
    if not isinstance(schema, Mapping):
        raise OutputContractError(f"invalid schema at {path}")
    expected = schema.get("type")
    checks = {
        "object": lambda x: isinstance(x, dict),
        "array": lambda x: isinstance(x, list),
        "string": lambda x: isinstance(x, str),
        "number": lambda x: isinstance(x, (int, float)) and not isinstance(x, bool),
        "integer": lambda x: isinstance(x, int) and not isinstance(x, bool),
        "boolean": lambda x: isinstance(x, bool),
        "null": lambda x: x is None,
    }
    if expected not in checks or not checks[expected](value):
        raise OutputContractError(f"expected {expected!r} at {path}")
    if expected == "object":
        required = schema.get("required", [])
        properties = schema.get("properties", {})
        if not isinstance(required, list) or not isinstance(properties, Mapping):
            raise OutputContractError(f"invalid object schema at {path}")
        missing = [key for key in required if key not in value]
        if missing:
            raise OutputContractError(f"missing required properties at {path}: {missing}")
        if schema.get("additionalProperties", True) is False:
            extras = set(value) - set(properties)
            if extras:
                raise OutputContractError(f"unexpected properties at {path}: {sorted(extras)}")
        for key, child_schema in properties.items():
            if key in value:
                _validate_schema(value[key], child_schema, f"{path}.{key}")
    elif expected == "array" and "items" in schema:
        for index, item in enumerate(value):
            _validate_schema(item, schema["items"], f"{path}[{index}]")
