from __future__ import annotations

import json
import os
from pathlib import Path
import tempfile
from typing import Any

import hashlib
import fcntl


class StoreConflict(RuntimeError):
    pass


class LocalBlobStore:
    """Filesystem adapter for pipeline and release validation tests."""

    def __init__(self, root: str | Path):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)

    def _safe(self, key: str) -> Path:
        path = (self.root / key).resolve()
        if self.root.resolve() not in path.parents:
            raise ValueError(f"unsafe blob key: {key!r}")
        return path


    def read(self, key: str) -> bytes:
        return self._safe(key).read_bytes()

    def iter_chunks(self, key: str):
        with self._safe(key).open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                yield chunk

    def exists(self, key: str) -> bool:
        return self._safe(key).exists()

    def write_json_if_absent(self, key: str, value: Any) -> bool:
        destination = self._safe(key)
        destination.parent.mkdir(parents=True, exist_ok=True)
        if destination.exists():
            return False
        descriptor, temporary_name = tempfile.mkstemp(
            prefix=f".{destination.name}.", dir=destination.parent
        )
        temporary = Path(temporary_name)
        try:
            with os.fdopen(descriptor, "w") as handle:
                handle.write(json.dumps(value, indent=2, sort_keys=True) + "\n")
                handle.flush()
                os.fsync(handle.fileno())
            try:
                os.link(temporary, destination)
            except FileExistsError:
                return False
            return True
        finally:
            temporary.unlink(missing_ok=True)

    def compare_and_swap_json(
        self, key: str, expected_sha256: str | None, value: Any
    ) -> bool:
        """Atomically replace a JSON selector when its current digest matches.

        ``expected_sha256=None`` means create-only.  The lock models Blob
        ``If-Match``/``If-None-Match`` and keeps the local release tests honest
        about concurrent publishers.
        """

        destination = self._safe(key)
        destination.parent.mkdir(parents=True, exist_ok=True)
        lock_path = destination.with_name(f".{destination.name}.lock")
        with lock_path.open("a+") as lock:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
            current = destination.read_bytes() if destination.exists() else None
            if expected_sha256 is None:
                if current is not None:
                    return False
            elif current is None or hashlib.sha256(current).hexdigest() != expected_sha256:
                return False
            descriptor, temporary_name = tempfile.mkstemp(
                prefix=f".{destination.name}.", dir=destination.parent
            )
            temporary = Path(temporary_name)
            try:
                with os.fdopen(descriptor, "w") as handle:
                    handle.write(json.dumps(value, indent=2, sort_keys=True) + "\n")
                    handle.flush()
                    os.fsync(handle.fileno())
                os.replace(temporary, destination)
                return True
            finally:
                temporary.unlink(missing_ok=True)

    def read_json(self, key: str) -> dict[str, Any]:
        return json.loads(self.read(key))
