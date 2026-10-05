"""Managed identity entrypoint for one durable render event."""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile

from azure.core.exceptions import ResourceExistsError
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient

RENDERER_PROFILE = "uspto-xml-markdown-v2:zstd-9-checksum"


def canonical(value: object) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


def required(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(f"{name} is required")
    return value


def digest(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def immutable_upload(container, key: str, path: Path) -> None:
    blob = container.get_blob_client(key)
    expected = digest(path)
    try:
        with path.open("rb") as handle:
            blob.upload_blob(handle, length=path.stat().st_size, overwrite=False, if_none_match="*")
    except ResourceExistsError:
        properties = blob.get_blob_properties()
        if properties.size != path.stat().st_size:
            raise RuntimeError(f"immutable Blob size conflict: {key}")
        actual = hashlib.sha256()
        for chunk in blob.download_blob(max_concurrency=4).chunks():
            actual.update(chunk)
        if actual.hexdigest() != expected:
            raise RuntimeError(f"immutable Blob hash conflict: {key}")


def main() -> int:
    account = required("AZURE_STORAGE_ACCOUNT")
    event_key = required("PIPELINE_EVENT_KEY")
    result_key = required("PIPELINE_RESULT_KEY")
    service = BlobServiceClient(
        f"https://{account}.blob.core.windows.net", credential=DefaultAzureCredential()
    )
    releases = service.get_container_client(os.environ.get("RELEASE_CONTAINER", "releases"))
    source = service.get_container_client(os.environ.get("SOURCE_CONTAINER", "source"))
    event = json.loads(releases.get_blob_client(event_key).download_blob().readall())
    if event.get("schema_version") != 1 or event.get("stage") != "render":
        raise RuntimeError("invalid render event")
    payload = event["payload"]
    fingerprint = hashlib.sha256(
        canonical({"schema_version": 1, "stage": "render", "payload": payload})
    ).hexdigest()
    if (
        event.get("event_fingerprint") != fingerprint
        or event.get("event_id") != f"render-{fingerprint[:32]}"
    ):
        raise RuntimeError("render event identity mismatch")
    raw = payload["raw"]
    generation = payload["source_generation"]
    if not isinstance(generation, str) or not generation.startswith("sha256:") or len(generation) != 71:
        raise RuntimeError("source_generation must be sha256:<64 hex>")
    if not isinstance(raw.get("key"), str) or not raw["key"].startswith("raw/"):
        raise RuntimeError("raw Blob key must be under raw/")
    renderer_profile = str(payload.get("renderer_profile") or "")
    if renderer_profile != os.environ.get("PATPUB_RENDERER_PROFILE", RENDERER_PROFILE):
        raise RuntimeError("renderer_profile does not match this renderer image")
    expected_generation = "sha256:" + hashlib.sha256(
        canonical({"raw_sha256": raw["sha256"], "renderer_profile": renderer_profile})
    ).hexdigest()
    if generation != expected_generation:
        raise RuntimeError("source_generation does not match raw and renderer profile")
    with tempfile.TemporaryDirectory(prefix="render-", dir=os.environ.get("WORK_DIR", "/work")) as work:
        root = Path(work)
        archive = root / "raw.zip"
        with archive.open("wb") as handle:
            for chunk in service.get_blob_client(
                os.environ.get("RAW_CONTAINER", "raw"), raw["key"].split("/", 1)[1]
            ).download_blob(max_concurrency=4).chunks():
                handle.write(chunk)
        if archive.stat().st_size != int(raw["bytes"]) or digest(archive) != str(raw["sha256"]).removeprefix("sha256:"):
            raise RuntimeError("raw Blob integrity mismatch")
        output = root / "projection.ndjson"
        pack = root / "pack.zst"
        subprocess.run(
            ["/usr/local/bin/uspto-ingestion-worker", "project", str(archive), str(output), generation, str(pack)],
            check=True,
        )
        pack_sha = digest(pack)
        projection_sha = digest(output)
        pack_key = f"source/v1/packs/{pack_sha}.zst"
        immutable_upload(source, pack_key.split("/", 1)[1], pack)
        publication_count = sum(1 for _ in output.open("rb"))
        result = {
            "event_id": event["event_id"],
            "event_fingerprint": event["event_fingerprint"],
            "result": {
                "source_packs": [{"key": pack_key, "sha256": pack_sha, "bytes": pack.stat().st_size}],
                "projection": {"sha256": projection_sha, "bytes": output.stat().st_size},
                "publication_count": publication_count,
            },
        }
        result_path = root / "result.json"
        result_path.write_bytes(canonical(result))
        immutable_upload(releases, result_key, result_path)
        print(f"rendered_publications={publication_count} projection_sha256={projection_sha} pack={pack_key}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
