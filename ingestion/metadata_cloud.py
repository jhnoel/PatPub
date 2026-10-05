"""Managed-identity cloud runner for the Parquet metadata projection."""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path

from .metadata import project_ndjson_to_parquet
from .orchestration import PipelineEvent
from .render_local import render_from_raw
from .release import AzureBlobReleaseStore


def _required(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(f"{name} is required")
    return value


def _sha256_file(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def main() -> int:
    try:
        from azure.core.exceptions import ResourceExistsError
        from azure.identity import DefaultAzureCredential
        from azure.storage.blob import BlobServiceClient
    except ImportError as exc:  # pragma: no cover - deployment-only path
        raise RuntimeError("install the azure optional dependency") from exc

    account = _required("AZURE_STORAGE_ACCOUNT")
    event_key = _required("PIPELINE_EVENT_KEY")
    result_key = _required("PIPELINE_RESULT_KEY")
    release_store = AzureBlobReleaseStore(
        f"https://{account}.blob.core.windows.net", "releases"
    )
    event = PipelineEvent.from_dict(json.loads(release_store.read(event_key)))
    if event.stage != "metadata":
        raise RuntimeError("metadata worker requires a metadata event")
    snapshot_id = str(event.payload.get("snapshot_id") or "")
    if not snapshot_id:
        raise RuntimeError("metadata event requires snapshot_id")
    metadata_container = os.environ.get("METADATA_CONTAINER", "metadata")
    work = Path(os.environ.get("WORK_DIR", "/work"))
    work.mkdir(parents=True, exist_ok=True)

    service = BlobServiceClient(
        f"https://{account}.blob.core.windows.net",
        credential=DefaultAzureCredential(),
    )
    projection_path = render_from_raw(service, event.payload, work)
    if snapshot_id != event.payload["projection"]["sha256"]:
        raise RuntimeError("metadata snapshot_id does not match the rendered projection")

    output_root = work / "output"
    projection = project_ndjson_to_parquet(
        projection_path, output_root, snapshot_id,
        bucket_count=int(os.environ.get("METADATA_BUCKET_COUNT", "16")),
    )
    metadata_client = service.get_container_client(metadata_container)
    for path in sorted(output_root.joinpath("metadata").rglob("*")):
        if not path.is_file():
            continue
        key = path.relative_to(output_root / "metadata").as_posix()
        blob = metadata_client.get_blob_client(key)
        try:
            with path.open("rb") as handle:
                blob.upload_blob(
                    handle,
                    length=path.stat().st_size,
                    overwrite=False,
                    if_none_match="*",
                )
        except ResourceExistsError:
            properties = blob.get_blob_properties()
            if properties.size != path.stat().st_size:
                raise RuntimeError(f"immutable metadata conflict: {key}")
            existing_hash = hashlib.sha256()
            for chunk in blob.download_blob(max_concurrency=4).chunks():
                existing_hash.update(chunk)
            if existing_hash.hexdigest() != _sha256_file(path):
                raise RuntimeError(f"immutable metadata conflict: {key}")
    result = {
        "publication_count": projection.publication_count,
        "metadata": {
            "parquet": {
                "manifest": {
                    "key": projection.manifest_key,
                    "sha256": projection.manifest_sha256,
                    "bytes": projection.manifest_bytes,
                },
                "parts": list(projection.parts),
            },
            "publication_count": projection.publication_count,
        }
    }
    envelope = {
        "event_id": event.event_id,
        "event_fingerprint": event.fingerprint,
        "result": result,
    }
    if not release_store.write_json_if_absent(result_key, envelope):
        if json.loads(release_store.read(result_key)) != envelope:
            raise RuntimeError("immutable metadata result conflict")
    print(
        f"metadata_projection_status=published snapshot={projection.snapshot_id} "
        f"rows={projection.publication_count} parts={len(projection.parts)} "
        f"manifest_sha256={projection.manifest_sha256}"
    )
    return 0


if __name__ == "__main__":  # pragma: no cover - deployment image entrypoint
    raise SystemExit(main())
