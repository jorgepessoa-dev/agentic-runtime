"""Optional local CLI bindings. The generic fabric does not import this module."""

from __future__ import annotations

from pathlib import Path
from typing import Sequence

from agentic_runtime.contracts.execution import ExecutionRequest


def _selected_model(request: ExecutionRequest) -> str:
    if not request.requested_model:
        raise ValueError("this executor registration requires an explicit requested_model")
    return request.requested_model


def codex_command(request: ExecutionRequest) -> Sequence[str]:
    prompt = str(request.metadata["prompt"])
    args = ["codex", "--no-daemon", "exec", "--ephemeral", "--model", _selected_model(request),
            "--sandbox", "workspace-write", "--cd", request.workspace_ref,
            "--output-last-message", str(Path(request.workspace_ref) / ".agentic-runtime-final.txt")]
    return [*args, prompt]


def claude_command(request: ExecutionRequest) -> Sequence[str]:
    prompt = str(request.metadata["prompt"])
    args = ["claude", "--print", prompt, "--output-format", "json", "--no-session-persistence",
            "--model", _selected_model(request)]
    if request.metadata.get("max_budget_usd") is not None:
        args.extend(["--max-budget-usd", str(request.metadata["max_budget_usd"])])
    args.extend([
            "--permission-mode", "acceptEdits", "--permission-prompts", "none",
            "--tools", "Read,Write,Edit"])
    return args


def hermes_command(request: ExecutionRequest) -> Sequence[str]:
    prompt = str(request.metadata["prompt"])
    provider = request.metadata.get("provider")
    if not provider:
        raise ValueError("Hermes registration requires an explicit provider configuration")
    args = ["hermes", "chat", "--oneshot", "--quiet", "--safe-mode", "--toolsets", "file",
            "--model", _selected_model(request), "--provider", str(provider),
            "--max-turns", "4", "--run-budget", str(int(request.timeout_seconds)),
            "--in", request.workspace_ref, "--query", prompt]
    return args


def claude_final_output(raw: bytes) -> bytes:
    """Reduce the CLI's machine envelope to the final response before storage."""
    import json
    try:
        value = json.loads(raw)
        result = value.get("result") if isinstance(value, dict) else None
        return str(result).encode("utf-8") if result is not None else b""
    except (UnicodeDecodeError, json.JSONDecodeError):
        return b""
