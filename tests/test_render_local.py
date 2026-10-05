from __future__ import annotations

import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from ingestion import render_local


class _RawService:
    def __init__(self, data: bytes):
        self.data = data
        self.requests: list[tuple[str, str]] = []

    def get_blob_client(self, container: str, blob: str):
        self.requests.append((container, blob))
        return SimpleNamespace(download_blob=lambda **_kwargs: SimpleNamespace(chunks=lambda: iter([self.data])))


def _payload(raw: bytes, projection: bytes, pack: bytes) -> dict:
    raw_sha = hashlib.sha256(raw).hexdigest()
    profile = render_local.RENDERER_PROFILE
    generation = "sha256:" + hashlib.sha256(
        (json.dumps({"raw_sha256": raw_sha, "renderer_profile": profile}, sort_keys=True, separators=(",", ":")) + "\n").encode()
    ).hexdigest()
    pack_sha = hashlib.sha256(pack).hexdigest()
    return {
        "raw": {"key": f"raw/v1/PTGRXML/{raw_sha}.zip", "sha256": raw_sha, "bytes": len(raw)},
        "source_generation": generation,
        "renderer_profile": profile,
        "source_packs": [{"key": f"source/v1/packs/{pack_sha}.zst", "sha256": pack_sha, "bytes": len(pack)}],
        "projection": {"sha256": hashlib.sha256(projection).hexdigest(), "bytes": len(projection)},
    }


def test_consumers_recreate_verified_projection_from_raw_zip(tmp_path: Path, monkeypatch):
    raw, projection, pack = b"raw zip", b"{\"publication_uid\":\"US:1:B2\"}\n", b"source pack"
    payload = _payload(raw, projection, pack)
    service = _RawService(raw)

    def render(command, *, check):
        assert check is True
        assert command[:2] == [render_local.RENDERER_BINARY, "project"]
        Path(command[3]).write_bytes(projection)
        Path(command[5]).write_bytes(pack)

    monkeypatch.setattr(render_local.subprocess, "run", render)
    output = render_local.render_from_raw(service, payload, tmp_path)
    assert output.read_bytes() == projection
    assert service.requests == [("raw", payload["raw"]["key"].split("/", 1)[1])]
    assert not (tmp_path / "raw.zip").exists()
    assert not (tmp_path / "pack.zst").exists()


def test_recreated_projection_must_match_renderer_digest(tmp_path: Path, monkeypatch):
    payload = _payload(b"raw zip", b"expected\n", b"source pack")
    service = _RawService(b"raw zip")

    def render(command, *, check):
        Path(command[3]).write_bytes(b"different\n")
        Path(command[5]).write_bytes(b"source pack")

    monkeypatch.setattr(render_local.subprocess, "run", render)
    with pytest.raises(RuntimeError, match="projection does not match"):
        render_local.render_from_raw(service, payload, tmp_path)
