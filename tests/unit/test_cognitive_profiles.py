from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from agentic_runtime.artifacts.store import ArtifactStore
from agentic_runtime.cognitive.api import OpenAICompatibleCognitiveAdapter
from agentic_runtime.cognitive.profiles import configured_adapters, load_profile_config


class CognitiveProfileTests(unittest.TestCase):
    def write_config(self, root: Path, profiles):
        path = root / "profiles.json"
        path.write_text(json.dumps({"profiles": profiles}), encoding="utf-8")
        return path

    def test_profile_config_creates_generic_api_adapter_without_provider_io(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            config = self.write_config(root, [{
                "route_id": "test-route", "adapter": "openai_compatible",
                "provider": "test-provider", "model_family": "test-family", "model": "test-model",
                "endpoint": "https://api.example.invalid/v1/chat/completions",
                "credential_env": "TEST_PROVIDER_TOKEN", "tools": [],
                "price_schedule": {"schedule_id": "test-rate", "input_usd_per_million": 1,
                    "output_usd_per_million": 2, "source_ref": "https://example.invalid/rate",
                    "observed_at": "2026-01-01"},
            }])
            with patch.dict("os.environ", {"TEST_PROVIDER_TOKEN": "configured-only"}):
                adapters = configured_adapters(config, artifacts=ArtifactStore(root / "artifacts"),
                    worker_id="worker", worker_instance_id="instance")
                self.assertEqual(set(adapters), {"test-route"})
                self.assertIsInstance(adapters["test-route"], OpenAICompatibleCognitiveAdapter)
                self.assertEqual(adapters["test-route"].credential_resolver(), "configured-only")
                self.assertEqual(adapters["test-route"].capabilities().provider, "test-provider")

    def test_duplicate_routes_and_tool_permissions_are_rejected(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            duplicate = self.write_config(root, [
                {"route_id": "same", "adapter": "claude_code"},
                {"route_id": "same", "adapter": "claude_code"},
            ])
            with self.assertRaisesRegex(ValueError, "unique"):
                load_profile_config(duplicate)
            tools = self.write_config(root, [{"route_id": "route", "adapter": "claude_code", "tools": ["shell"]}])
            with self.assertRaisesRegex(ValueError, "tools"):
                load_profile_config(tools)


if __name__ == "__main__":
    unittest.main()
