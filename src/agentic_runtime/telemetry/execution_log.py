from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Mapping


class ExecutionLog:
    """Append-only JSONL summaries. Callers must omit prompts and credentials."""

    def __init__(self, path: Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def append(self, event: Mapping[str, object]) -> None:
        line = json.dumps(dict(event), sort_keys=True, separators=(",", ":")) + "\n"
        fd = os.open(self.path, os.O_CREAT | os.O_APPEND | os.O_WRONLY, 0o600)
        try:
            os.write(fd, line.encode("utf-8")); os.fsync(fd)
        finally:
            os.close(fd)
