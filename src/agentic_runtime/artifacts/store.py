from __future__ import annotations

import hashlib
import json
import os
import tempfile
import fcntl
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Mapping


@dataclass(frozen=True)
class ArtifactManifest:
    artifact_id: str
    content_hash: str
    kind: str
    producer_execution_id: str
    producer_attempt_id: str
    created_at: str
    location: str
    metadata: Mapping[str, object] = field(default_factory=dict)


class ArtifactIntegrityError(RuntimeError):
    pass


class ArtifactStore:
    def __init__(self, root: Path) -> None:
        self.root = Path(root).resolve()
        self.root.mkdir(parents=True, exist_ok=True)

    def put(self, content: bytes, *, kind: str, producer_execution_id: str,
            producer_attempt_id: str, metadata: Mapping[str, object] | None = None) -> ArtifactManifest:
        digest = hashlib.sha256(content).hexdigest()
        artifact_id = f"sha256:{digest}"
        folder = self.root / "sha256" / digest[:2] / digest[2:4] / digest
        folder.mkdir(parents=True, exist_ok=True)
        lock_fd=os.open(folder / ".write.lock",os.O_CREAT|os.O_RDWR,0o600)
        fcntl.flock(lock_fd,fcntl.LOCK_EX)
        try:
            return self._put_locked(content,digest,artifact_id,folder,kind,producer_execution_id,
                                    producer_attempt_id,metadata or {})
        finally:
            fcntl.flock(lock_fd,fcntl.LOCK_UN)
            os.close(lock_fd)

    def put_shared(self, content: bytes, *, kind: str, producer_execution_id: str,
                   producer_attempt_id: str, metadata: Mapping[str, object] | None = None) -> ArtifactManifest:
        """Persist a content-addressed blob whose per-producer lineage lives elsewhere.

        Ordinary artifacts retain the strict one-manifest/one-producer rule of
        ``put``. Cognitive raw outputs can be byte-identical across calls; the
        immutable invocation/artifact rows retain each producer's lineage while
        this method safely reuses the verified SHA-256 blob.
        """
        digest = hashlib.sha256(content).hexdigest()
        artifact_id = f"sha256:{digest}"
        folder = self.root / "sha256" / digest[:2] / digest[2:4] / digest
        folder.mkdir(parents=True, exist_ok=True)
        lock_fd = os.open(folder / ".write.lock", os.O_CREAT | os.O_RDWR, 0o600)
        fcntl.flock(lock_fd, fcntl.LOCK_EX)
        try:
            blob = folder / "content"
            manifest_path = folder / "manifest.json"
            if blob.exists():
                if hashlib.sha256(blob.read_bytes()).hexdigest() != digest:
                    raise ArtifactIntegrityError("existing shared content-addressed blob failed verification")
                if manifest_path.exists():
                    try:
                        existing = json.loads(manifest_path.read_text(encoding="utf-8"))
                    except (OSError, ValueError) as exc:
                        raise ArtifactIntegrityError("existing shared artifact manifest is invalid") from exc
                    if existing.get("content_hash") != digest or existing.get("artifact_id") != artifact_id:
                        raise ArtifactIntegrityError("existing shared artifact manifest identity mismatch")
            else:
                fd, tmp_name = tempfile.mkstemp(prefix=".pending-", dir=folder)
                try:
                    with os.fdopen(fd, "wb") as stream:
                        stream.write(content); stream.flush(); os.fsync(stream.fileno())
                    if hashlib.sha256(Path(tmp_name).read_bytes()).hexdigest() != digest:
                        raise ArtifactIntegrityError("temporary shared artifact hash mismatch")
                    os.replace(tmp_name, blob)
                    dir_fd = os.open(folder, os.O_DIRECTORY)
                    try:
                        os.fsync(dir_fd)
                    finally:
                        os.close(dir_fd)
                finally:
                    if os.path.exists(tmp_name):
                        os.unlink(tmp_name)
            if not manifest_path.exists():
                self._atomic_json(manifest_path, asdict(ArtifactManifest(artifact_id, digest, kind,
                    producer_execution_id, producer_attempt_id, datetime.now(timezone.utc).isoformat(),
                    str(blob), metadata or {})))
            self.verify(artifact_id)
            return ArtifactManifest(artifact_id, digest, kind, producer_execution_id,
                producer_attempt_id, datetime.now(timezone.utc).isoformat(), str(blob), metadata or {})
        finally:
            fcntl.flock(lock_fd, fcntl.LOCK_UN)
            os.close(lock_fd)

    def _put_locked(self, content: bytes, digest: str, artifact_id: str, folder: Path,
                    kind: str, producer_execution_id: str, producer_attempt_id: str,
                    metadata: Mapping[str, object]) -> ArtifactManifest:
        blob = folder / "content"
        manifest_path = folder / "manifest.json"
        if blob.exists():
            if hashlib.sha256(blob.read_bytes()).hexdigest() != digest:
                raise ArtifactIntegrityError("existing content-addressed blob failed verification")
            # The shared blob is content-addressed, but its current manifest
            # carries producer lineage. Never overwrite that lineage when a
            # second attempt emits byte-identical content.
            if manifest_path.exists():
                try:
                    existing=json.loads(manifest_path.read_text(encoding="utf-8"))
                except (OSError,ValueError) as exc:
                    raise ArtifactIntegrityError("existing artifact manifest is invalid") from exc
                if existing.get("producer_attempt_id") != producer_attempt_id:
                    raise ArtifactIntegrityError("content hash already has different producer lineage")
        else:
            fd, tmp_name = tempfile.mkstemp(prefix=".pending-", dir=folder)
            try:
                with os.fdopen(fd, "wb") as stream:
                    stream.write(content); stream.flush(); os.fsync(stream.fileno())
                if hashlib.sha256(Path(tmp_name).read_bytes()).hexdigest() != digest:
                    raise ArtifactIntegrityError("temporary artifact hash mismatch")
                os.replace(tmp_name, blob)
                dir_fd = os.open(folder, os.O_DIRECTORY)
                try:
                    os.fsync(dir_fd)
                finally:
                    os.close(dir_fd)
            finally:
                if os.path.exists(tmp_name):
                    os.unlink(tmp_name)
        manifest = ArtifactManifest(artifact_id, digest, kind, producer_execution_id,
            producer_attempt_id, datetime.now(timezone.utc).isoformat(), str(blob), metadata)
        if not manifest_path.exists():
            self._atomic_json(manifest_path, asdict(manifest))
        self.verify(artifact_id)
        return manifest

    @staticmethod
    def _atomic_json(path: Path, value: Mapping[str, object]) -> None:
        fd, tmp_name = tempfile.mkstemp(prefix=".manifest-", dir=path.parent)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as stream:
                json.dump(value, stream, sort_keys=True, separators=(",", ":"))
                stream.flush(); os.fsync(stream.fileno())
            os.replace(tmp_name, path)
        finally:
            if os.path.exists(tmp_name):
                os.unlink(tmp_name)

    def path_for(self, artifact_id: str) -> Path:
        prefix, sep, digest = artifact_id.partition(":")
        if prefix != "sha256" or not sep or len(digest) != 64 or any(c not in "0123456789abcdef" for c in digest):
            raise ValueError("invalid artifact ID")
        return self.root / "sha256" / digest[:2] / digest[2:4] / digest

    def verify(self, artifact_id: str) -> ArtifactManifest:
        folder = self.path_for(artifact_id)
        try:
            data = json.loads((folder / "manifest.json").read_text(encoding="utf-8"))
            blob = (folder / "content").read_bytes()
        except (OSError, ValueError) as exc:
            raise ArtifactIntegrityError(f"artifact unavailable or manifest invalid: {artifact_id}") from exc
        digest = hashlib.sha256(blob).hexdigest()
        if digest != data.get("content_hash") or artifact_id != f"sha256:{digest}":
            raise ArtifactIntegrityError(f"artifact hash mismatch: {artifact_id}")
        return ArtifactManifest(**data)

    def read(self, artifact_id: str) -> bytes:
        self.verify(artifact_id)
        return (self.path_for(artifact_id) / "content").read_bytes()
