"""Recreate a verified, temporary NDJSON render from an immutable raw ZIP."""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
from typing import Any, Mapping


RENDERER_PROFILE = "uspto-xml-markdown-v2:zstd-9-checksum"
RENDERER_BINARY = "/usr/local/bin/uspto-ingestion-worker"
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")


def _canonical(value: object) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


def _digest(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def _expected_artifact(value: Any, name: str) -> tuple[str, int]:
    if not isinstance(value, Mapping):
        raise RuntimeError(f"{name} must be an artifact record")
    sha256 = str(value.get("sha256") or "").removeprefix("sha256:")
    if not _SHA256.fullmatch(sha256):
        raise RuntimeError(f"{name} has an invalid SHA-256")
    try:
        size = int(value["bytes"])
    except (KeyError, TypeError, ValueError) as exc:
        raise RuntimeError(f"{name} has no byte count") from exc
    if size < 0:
        raise RuntimeError(f"{name} has a negative byte count")
    return sha256, size


def _verify(path: Path, artifact: Any, name: str) -> None:
    sha256, size = _expected_artifact(artifact, name)
    if path.stat().st_size != size or _digest(path) != sha256:
        raise RuntimeError(f"{name} does not match its recorded bytes and SHA-256")


def render_from_raw(service: Any, payload: Mapping[str, Any], work: Path) -> Path:
    """Download the raw ZIP, rerender, and verify the renderer's exact outputs.

    The caller owns ``work`` and may remove the NDJSON as soon as its job ends.
    No projection Blob is read or written.
    """
    raw = payload.get("raw")
    if not isinstance(raw, Mapping):
        raise RuntimeError("render event has no raw archive")
    raw_key = str(raw.get("key") or "")
    if not raw_key.startswith("raw/") or ".." in raw_key.split("/"):
        raise RuntimeError("raw archive key is invalid")
    profile = str(payload.get("renderer_profile") or "")
    aps_profile = "aps-greenbook-markdown-v2:zstd-9-checksum"
    if profile not in {os.environ.get("PATPUB_RENDERER_PROFILE", RENDERER_PROFILE), aps_profile}:
        raise RuntimeError("renderer profile does not match this worker")
    generation = str(payload.get("source_generation") or "")
    expected_generation = "sha256:" + hashlib.sha256(
        _canonical({"raw_sha256": raw.get("sha256"), "renderer_profile": profile})
    ).hexdigest()
    if generation != expected_generation:
        raise RuntimeError("source generation does not match the raw archive and renderer")

    projection = payload.get("projection")
    if not isinstance(projection, Mapping) or "key" in projection:
        raise RuntimeError("render event requires a projection digest without a Blob key")
    _expected_artifact(projection, "projection")
    packs = payload.get("source_packs")
    if not isinstance(packs, list) or len(packs) != 1:
        raise RuntimeError("render event requires one expected source pack")
    pack_ref = packs[0]
    if not isinstance(pack_ref, Mapping) or not str(pack_ref.get("key") or "").startswith("source/v1/packs/"):
        raise RuntimeError("source pack key is invalid")
    pack_sha, _ = _expected_artifact(pack_ref, "source pack")
    if pack_ref["key"] != f"source/v1/packs/{pack_sha}.zst":
        raise RuntimeError("source pack key does not match its SHA-256")

    work.mkdir(parents=True, exist_ok=True)
    archive_path = work / "raw.zip"
    with archive_path.open("wb") as handle:
        for chunk in service.get_blob_client("raw", raw_key.split("/", 1)[1]).download_blob(max_concurrency=4).chunks():
            handle.write(chunk)
    _verify(archive_path, raw, "raw archive")

    projection_path = work / "projection.ndjson"
    pack_path = work / "pack.zst"
    if profile == aps_profile:
        render_aps(archive_path, projection_path, pack_path, generation, work, Path(raw_key).name)
    else:
        subprocess.run(
            [RENDERER_BINARY, "project", str(archive_path), str(projection_path), generation, str(pack_path)],
            check=True,
        )
    _verify(projection_path, projection, "projection")
    _verify(pack_path, pack_ref, "source pack")
    archive_path.unlink()
    pack_path.unlink()
    return projection_path


def render_aps(archive: Path, projection: Path, pack: Path, generation: str, work: Path, archive_name: str) -> None:
    """APS v2 keeps v1 text/frames and binds unique records to a new generation."""
    import shutil
    zips = work / "aps-zips"
    zips.mkdir()
    shutil.copyfile(archive, zips / archive_name)
    exported = work / "aps-export.ndjson"
    with exported.open("wb") as handle:
        subprocess.run(["aps-backfill", "export", str(zips)], stdout=handle, check=True)
    output = work / "aps-packed"
    subprocess.run(["aps-backfill", "pack", str(exported), str(zips), str(output)], check=True)
    packed = list((output / "packs").glob("*.zst"))
    records = list((output / "ndjson").glob("*.ndjson"))
    if len(packed) != 1 or len(records) != 1:
        raise RuntimeError("APS projection requires exactly one archive output")
    seen = set()
    with records[0].open() as source, projection.open("wb") as target:
        for line in source:
            document = json.loads(line)
            uid = document["publication_uid"]
            if uid in seen:
                raise RuntimeError("Duplicate APS publication UID")
            seen.add(uid)
            document["source_generation"] = generation
            target.write(_canonical(document))
    shutil.copyfile(packed[0], pack)
