"""Project the shared archive render to immutable Parquet metadata."""

from __future__ import annotations

import os

from .metadata import project_ndjson_to_parquet


def project_metadata(service, payload, projection_path, work):
    from .artifacts import upload_file
    snapshot_id = payload["snapshot_id"]
    if snapshot_id != payload["projection"]["sha256"]:
        raise RuntimeError("metadata snapshot_id does not match the rendered projection")
    metadata_container = "metadata"
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
        upload_file(metadata_client.get_blob_client(key), path)
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
    return result
