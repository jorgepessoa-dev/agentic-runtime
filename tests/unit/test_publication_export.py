from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

from scripts import build_public_export


class PublicExportTests(unittest.TestCase):
    def test_candidate_is_allowlisted_manifested_and_has_no_private_history(self):
        with tempfile.TemporaryDirectory() as temporary:
            target = Path(temporary) / "candidate"
            manifest = build_public_export.copy_candidate(target)
            self.assertTrue(manifest["files"])
            self.assertFalse((target / ".git").exists())
            self.assertFalse((target / "evidence").exists())
            self.assertTrue((target / "deploy/templates").is_dir())
            self.assertFalse((target / "deploy/systemd").exists())
            listed = {entry["path"] for entry in manifest["files"]}
            self.assertIn("src/agentic_runtime/remote/worker.py", listed)
            self.assertIn("PUBLICATION_BLOCKERS.md", listed)
            self.assertIn("deploy/templates/worker.env.example", listed)
            self.assertTrue((target / "PUBLIC_EXPORT_MANIFEST.json").is_file())
            saved = json.loads((target / "PUBLIC_EXPORT_MANIFEST.json").read_text())
            self.assertEqual(saved, manifest)
            self.assertNotIn("source_revision", saved)
            env_example = (target / ".env.example").read_text()
            self.assertIn("PROVIDER_API_KEY=\n", env_example)
            self.assertIn("example.invalid", env_example)
            self.assertTrue((target / "LICENSE").is_file())
            self.assertTrue((target / "NOTICE").is_file())
            self.assertTrue((target / ".github/VULNERABILITY_REPORT.yml").is_file())
            self.assertTrue((target / "THIRD_PARTY_NOTICES.md").is_file())
            for item in manifest["files"]:
                payload = (target / item["path"]).read_bytes()
                private_marker = b"-----BEGIN " + b"PRIVATE KEY" + b"-----"
                self.assertNotIn(private_marker, payload)

    def test_nonempty_destination_is_rejected_without_overwrite(self):
        with tempfile.TemporaryDirectory() as temporary:
            target = Path(temporary) / "candidate"
            target.mkdir()
            sentinel = target / "keep.txt"
            sentinel.write_text("keep")
            with self.assertRaises(ValueError):
                build_public_export.copy_candidate(target)
            self.assertEqual(sentinel.read_text(), "keep")

    def test_publication_scans_reject_private_identifiers_and_operational_values(self):
        samples = [
            b"operator=" + b"jor" + b"ge",
            b"path=/" + b"home" + b"/operator/state",
            b"endpoint=" + b"203.0." + b"113.8",
            b"mailto=maintainer" + b"@example.org",
            b"provider=" + b"Net" + b"cup",
            b"Authorization: Bearer " + b"a" * 32,
        ]
        for sample in samples:
            with self.subTest(pattern=sample.split(b"=")[0]):
                with self.assertRaises(ValueError):
                    build_public_export.validate_payload("sample.txt", sample)
        authorized = b"Copyright 2026 " + b"Jor" + b"ge Pessoa\n"
        build_public_export.validate_payload("NOTICE", authorized)
        with self.assertRaisesRegex(ValueError, "personal identifier"):
            build_public_export.validate_payload("NOTICE", b"Maintainer " + b"Jor" + b"ge Pessoa\n")

    def test_allowlisted_symlink_escape_is_rejected(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "source"
            outside = Path(temporary) / "outside"
            root.mkdir()
            outside.mkdir()
            (root / "src").mkdir()
            (root / "src/agentic_runtime").symlink_to(outside, target_is_directory=True)
            destination = Path(temporary) / "candidate"
            with mock.patch.object(build_public_export, "ROOT", root):
                with self.assertRaisesRegex(ValueError, "symlink directory rejected"):
                    build_public_export.copy_candidate(destination)


if __name__ == "__main__":
    unittest.main()
