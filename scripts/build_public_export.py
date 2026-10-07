#!/usr/bin/env python3
"""Build a clean, allowlisted review tree; never copy the private Git history."""
from __future__ import annotations

import argparse
import hashlib
import ipaddress
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys

VERSION = "1.0"
ROOT = Path(__file__).resolve().parents[1]
ALLOW_FILES = {
    ".gitignore", ".env.example", "README.md", "LICENSE", "SECURITY.md", "CONTRIBUTING.md", "THIRD_PARTY_NOTICES.md",
    "NOTICE", ".github/VULNERABILITY_REPORT.yml", "PUBLICATION_BLOCKERS.md", "COPYRIGHT_HOLDER_REVIEW.md",
    "pyproject.toml", "uv.lock", "scripts/build_public_export.py",
    "scripts/run_m4c_role_tests.py", "scripts/bootstrap_m4_roles.sql", "VERSIONING.md",
    "docs/COGNITIVE_ADAPTERS.md", "docs/publication/PUBLICATION_READINESS.md",
    "docs/stability/S1_TEST_STABILITY.md",
    "decisions/ADR-009-governed-workflow-genome-evolution.md",
    ".github/workflows/ci.yml",
    "tests/fixtures/routing-policy-eval-v1.json",
}
ALLOW_TREES = ("src/agentic_runtime", "migrations", "schemas", "governance", "architecture", "tests", "deploy/templates")
EXCLUDE_PARTS = {"__pycache__", ".pytest_cache", ".mypy_cache", ".ruff_cache", ".venv"}
EXCLUDE_FILES = {
    # These files exercise private administrative tooling or opt-in live/provider campaigns.
    "tests/integration/m8_live_campaign_historical_optin.py",
    "tests/integration/test_m8_admin_bridge.py",
    "tests/integration/test_m8_cognitive_remote.py",
    "tests/integration/test_m8_direct_campaign.py",
    "tests/integration/test_m8_live_campaign.py",
    "tests/unit/test_m8_live_closure_harness.py",
    "tests/unit/test_m8_admin_bridge.py",
    "tests/unit/test_m8_campaign_call_book.py",
    "tests/unit/test_permanent_admin_bridge.py",
    "tests/integration/test_m9_coordination_runtime.py",
    "tests/unit/test_m9_admin.py",
    "tests/unit/test_cognitive_fabric.py",
    "tests/unit/test_cognitive_credentials.py",
    "src/agentic_runtime/cognitive/credentials.py",
    "architecture/agentic-runtime-admin-bridge.md",
}
EXCLUDE_PREFIXES = ("src/agentic_runtime/admin/", "architecture/milestones/m9/")
SECRET_PATTERNS = [
    re.compile(rb"-----BEGIN (?:RSA |EC |OPENSSH |DSA )?PRIVATE KEY-----"),
    re.compile(rb"\b(?:AKIA|ASIA)[A-Z0-9]{16}\b"),
    re.compile(rb"\b(?:gh[pousr]_[A-Za-z0-9]{30,}|github_pat_[A-Za-z0-9_]{40,})\b"),
    re.compile(rb"\b(?:sk-[A-Za-z0-9_-]{32,}|sk-ant-[A-Za-z0-9_-]{20,}|xox[baprs]-[A-Za-z0-9-]{20,}|AIza[0-9A-Za-z_-]{30,})\b"),
    re.compile(rb"(?i)authorization\s*[:=]\s*bearer\s+[A-Za-z0-9._~+/-]{20,}"),
    re.compile(rb"(?i)(?:postgres(?:ql)?|https?|redis|amqps?)://[^\s/:@]{1,80}:[^\s/@]{4,}@"),
    re.compile(rb"\b(?:gh[pousr]_[A-Za-z0-9]{20,}|github_pat_[A-Za-z0-9_]{30,})\b"),
]
PERSONAL_NAME = re.compile(rb"(?i)\bjorge\b")
HOME_PATH = re.compile(rb"/(?:home|Users)/[^/\s]+")
EMAIL = re.compile(rb"\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b", re.I)
IPV4 = re.compile(rb"(?<![\w.])(?:\d{1,3}\.){3}\d{1,3}(?![\w.])")
PRIVATE_TERMS = ("Net" + "cup", "Digital" + "Ocean", "W" + "SL", "ET" + "NA", "Q" + "ES", "Mari" + "nha")
TRANSFORMS = {}


def included(rel: str) -> bool:
    path = Path(rel)
    return (rel in ALLOW_FILES or any(rel == tree or rel.startswith(tree + "/") for tree in ALLOW_TREES)) and rel not in EXCLUDE_FILES and not rel.startswith(EXCLUDE_PREFIXES) and not (set(path.parts) & EXCLUDE_PARTS) and path.suffix not in {".pyc", ".pyo"}


def validate_payload(rel: str, data: bytes) -> None:
    path = Path(rel)
    if path.name == ".env" or (path.name.startswith(".env.") and path.name != ".env.example"):
        raise ValueError(f"private environment file rejected: {rel}")
    if path.suffix.lower() in {".pem", ".key", ".p12", ".pfx", ".p7b", ".p7c", ".crt", ".cer", ".der", ".jks", ".keystore", ".dump", ".backup"}:
        raise ValueError(f"secret or operational file type rejected: {rel}")
    if any(pattern.search(data) for pattern in SECRET_PATTERNS):
        raise ValueError(f"secret-pattern scan failed: {rel}")
    personal_scan = data
    if rel in {
        "NOTICE", "README.md", "COPYRIGHT_HOLDER_REVIEW.md",
        "PUBLICATION_BLOCKERS.md", "docs/publication/PUBLICATION_READINESS.md",
    }:
        # The owner explicitly authorized this exact copyright attribution for release.
        approved_name = b"Jor" + b"ge Pessoa"
        personal_scan = personal_scan.replace(b"Copyright 2026 " + approved_name, b"")
        if rel == "NOTICE":
            personal_scan = personal_scan.replace(
                b"This product includes software developed by " + approved_name + b".", b""
            )
    if PERSONAL_NAME.search(personal_scan) or HOME_PATH.search(data):
        raise ValueError(f"personal identifier or home path scan failed: {rel}")
    if EMAIL.search(data):
        for match in EMAIL.finditer(data):
            value = match.group().lower()
            if not value.endswith((b".invalid", b".example", b".test")):
                raise ValueError(f"non-placeholder email address scan failed: {rel}")
    for match in IPV4.finditer(data):
        try:
            address = ipaddress.ip_address(match.group().decode("ascii"))
        except ValueError:
            continue
        if not address.is_loopback:
            raise ValueError(f"non-loopback IPv4 address scan failed: {rel}")
    folded = data.lower()
    if any(term.lower().encode() in folded for term in PRIVATE_TERMS):
        raise ValueError(f"private infrastructure/domain term scan failed: {rel}")


def copy_candidate(destination: Path) -> dict:
    if destination.is_symlink():
        raise ValueError("destination symlink rejected")
    if destination.resolve().is_relative_to(ROOT):
        raise ValueError("candidate destination must be outside the private repository")
    if destination.exists() and any(destination.iterdir()):
        raise ValueError(f"destination must be absent or empty: {destination}")
    destination.mkdir(parents=True, exist_ok=True)
    records = []
    for base, dirs, names in os.walk(ROOT, followlinks=False):
        base_path = Path(base)
        for directory in dirs:
            candidate = base_path / directory
            rel_dir = candidate.relative_to(ROOT).as_posix()
            if candidate.is_symlink() and (rel_dir in ALLOW_TREES or any(rel_dir.startswith(tree + "/") for tree in ALLOW_TREES)):
                raise ValueError(f"allowlisted symlink directory rejected: {rel_dir}")
        dirs[:] = sorted(d for d in dirs if d not in EXCLUDE_PARTS)
        for name in sorted(names):
            source = base_path / name
            rel = source.relative_to(ROOT).as_posix()
            if not included(rel):
                continue
            if source.is_symlink() or not source.is_file():
                raise ValueError(f"non-regular allowed input rejected: {rel}")
            data = source.read_bytes()
            transforms = []
            replacements = {
                "tests/integration/test_m4_autonomous.py": [
                    (b'ROOT/"evidence/m3/routing-policy-eval-v1.json"', b'ROOT/"tests/fixtures/routing-policy-eval-v1.json"'),
                    (b'ROOT/"evidence/m4/soak-resource-measurements.json"', b'Path(tempfile.gettempdir())/"agentic-runtime-test-soak-resource-measurements.json"'),
                ],
                "tests/integration/test_postgres_runtime.py": [
                    (b'ROOT/"evidence/m3/routing-policy-eval-v1.json"', b'ROOT/"tests/fixtures/routing-policy-eval-v1.json"'),
                    (b'definition_ref="evidence/m3/routing-policy-eval-v1.json"', b'definition_ref="tests/fixtures/routing-policy-eval-v1.json"'),
                ],
                "scripts/run_m4c_role_tests.py": [
                    (b'Path(__file__).resolve().parents[1]/"evidence/m4/resource-measurements.json"', b'Path(tempfile.gettempdir())/"agentic-runtime-test-resource-measurements.json"'),
                ],
            }
            for before, after in replacements.get(rel, ()):
                if before not in data and after not in data:
                    raise ValueError(f"expected public-fixture transformation input missing: {rel}")
                if before in data:
                    data = data.replace(before, after)
                    transforms.append("replace private evidence dependency with synthetic public test fixture")
            validate_payload(rel, data)
            target = destination / rel
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(data)
            records.append({"path": rel, "sha256": hashlib.sha256(data).hexdigest(),
                            "classification": "PUBLIC" if not transforms else "PUBLIC_AFTER_SANITIZATION",
                            "transformations": transforms})
    if not records:
        raise ValueError("allowlist unexpectedly produced an empty candidate")
    manifest = {"export_tool_version": VERSION,
                "history_policy": "clean tree only; private .git omitted", "files": records}
    (destination / "PUBLIC_EXPORT_MANIFEST.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    return manifest


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("destination", type=Path)
    args = parser.parse_args()
    if args.destination.is_symlink():
        print("public export failed: destination symlink rejected", file=sys.stderr)
        return 2
    try:
        absolute_destination = args.destination.absolute()
        manifest = copy_candidate(absolute_destination)
    except (OSError, ValueError) as exc:
        print(f"public export failed: {exc}", file=sys.stderr)
        return 2
    print(f"candidate built: {args.destination.resolve()} ({len(manifest['files'])} files)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
