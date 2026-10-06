from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from typing import Any, Mapping


class ProposalValidationError(ValueError):
    pass


class StructuredOutputError(ValueError):
    pass


COGNITIVE_BUNDLE_CONTRACT = {
    "schema": {
        "type": "object",
        "required": ["format", "invocation_id", "status"],
        "properties": {
            "format": {"type": "string"},
            "invocation_id": {"type": "string"},
            "status": {"type": "string"},
        },
        "additionalProperties": True,
    }
}


def canonical_json(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                      allow_nan=False).encode("utf-8")


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _validate_schema(value: Any, schema: Mapping[str, Any], path: str = "$" ) -> None:
    expected = schema.get("type")
    valid = {
        "object": lambda v: isinstance(v, dict),
        "array": lambda v: isinstance(v, list),
        "string": lambda v: isinstance(v, str),
        "integer": lambda v: isinstance(v, int) and not isinstance(v, bool),
        "number": lambda v: isinstance(v, (int, float)) and not isinstance(v, bool),
        "boolean": lambda v: isinstance(v, bool),
        "null": lambda v: v is None,
    }
    if expected in valid and not valid[expected](value):
        raise StructuredOutputError(f"{path}: expected {expected}")
    if "enum" in schema and value not in schema["enum"]:
        raise StructuredOutputError(f"{path}: value outside enum")
    if isinstance(value, dict):
        required = schema.get("required", [])
        missing = set(required) - value.keys()
        if missing:
            raise StructuredOutputError(f"{path}: missing required properties")
        properties = schema.get("properties", {})
        if schema.get("additionalProperties") is False and set(value) - set(properties):
            raise StructuredOutputError(f"{path}: additional properties are forbidden")
        for key, child in properties.items():
            if key in value:
                _validate_schema(value[key], child, f"{path}.{key}")
    elif isinstance(value, list):
        if len(value) < schema.get("minItems", 0) or len(value) > schema.get("maxItems", 1_000_000):
            raise StructuredOutputError(f"{path}: array size outside bounds")
        item_schema = schema.get("items")
        if item_schema:
            for index, item in enumerate(value):
                _validate_schema(item, item_schema, f"{path}[{index}]")
    elif isinstance(value, str):
        if len(value) < schema.get("minLength", 0) or len(value) > schema.get("maxLength", 1_000_000):
            raise StructuredOutputError(f"{path}: string length outside bounds")


def parse_strict_json(raw: bytes | str, schema: Mapping[str, Any]) -> Mapping[str, Any]:
    """Parse exactly one JSON value; never repair or strip prose/fences."""
    try:
        text = raw.decode("utf-8") if isinstance(raw, bytes) else raw
        value = json.loads(text, parse_constant=lambda _: (_ for _ in ()).throw(ValueError("non-finite number")))
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        raise StructuredOutputError("response is not strict JSON") from exc
    _validate_schema(value, schema)
    if not isinstance(value, Mapping):
        raise StructuredOutputError("top-level output must be an object")
    return value


@dataclass(frozen=True)
class E1MutationPolicy:
    scope_id: str
    parent_genome_hash: str
    allowed_paths: Mapping[str, frozenset[str]]
    max_value_bytes: int = 4096
    max_rationale_chars: int = 2000


_FORBIDDEN_KEYS = re.compile(r"(?:secret|password|api[_-]?key|credential|token|sql|shell|command|promotion|champion|eval(?:uation)?_?(?:suite|policy)|mission|governance)", re.I)


class E1ProposalValidator:
    REQUIRED = frozenset({"proposal_id", "scope_id", "tier", "parent_genome_hash", "source_refs",
        "changed_paths", "mutation", "rationale", "expected_effect", "falsification_criteria",
        "evaluation_pack_hash", "budget_impact", "rollback_target"})

    @classmethod
    def validate(cls, proposal: Mapping[str, Any], policy: E1MutationPolicy) -> dict[str, Any]:
        if set(proposal) != cls.REQUIRED:
            raise ProposalValidationError("proposal fields must exactly match the registered schema")
        if proposal["scope_id"] != policy.scope_id or proposal["tier"] != "E1":
            raise ProposalValidationError("only the registered E1 scope is autonomous")
        if proposal["parent_genome_hash"] != policy.parent_genome_hash:
            raise ProposalValidationError("proposal parent genome is stale")
        if not isinstance(proposal["rationale"], str) or len(proposal["rationale"]) > policy.max_rationale_chars:
            raise ProposalValidationError("rationale exceeds registered bounds")
        for key in ("expected_effect", "falsification_criteria", "evaluation_pack_hash", "rollback_target"):
            if not isinstance(proposal[key], str) or not proposal[key].strip():
                raise ProposalValidationError(f"{key} is required")
        if not isinstance(proposal["source_refs"], list) or not proposal["source_refs"]:
            raise ProposalValidationError("proposal must retain source lineage")
        if not isinstance(proposal["changed_paths"], list) or not proposal["changed_paths"]:
            raise ProposalValidationError("proposal must declare changed paths")
        mutation = proposal["mutation"]
        if not isinstance(mutation, Mapping) or set(mutation) != {"path", "value"}:
            raise ProposalValidationError("mutation must contain exactly path and value")
        path = mutation["path"]
        if not isinstance(path, str) or path not in policy.allowed_paths.get(proposal["scope_id"], frozenset()):
            raise ProposalValidationError("mutation path is outside registered autonomous allowlist")
        if proposal["changed_paths"] != [path]:
            raise ProposalValidationError("declared changed paths must equal actual mutation path")
        if isinstance(mutation["value"], (dict, list)) and len(canonical_json(mutation["value"])) > policy.max_value_bytes:
            raise ProposalValidationError("mutation value exceeds policy limit")
        if isinstance(mutation["value"], str) and len(mutation["value"].encode()) > policy.max_value_bytes:
            raise ProposalValidationError("mutation value exceeds policy limit")
        # Only reject control-like keys when they are proposed as configuration.
        # Adversarial prose remains inert and is never executed.
        if isinstance(mutation["value"], Mapping) and any(_FORBIDDEN_KEYS.search(str(k)) for k in mutation["value"]):
            raise ProposalValidationError("mutation includes a forbidden authority/configuration field")
        canonical_json(proposal)
        return dict(proposal)
