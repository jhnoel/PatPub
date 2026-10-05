from __future__ import annotations

import json
from pathlib import Path

import pytest

from ingestion.metadata import MetadataProjectionError, project_ndjson_to_parquet

pa = pytest.importorskip("pyarrow")
parquet = pytest.importorskip("pyarrow.parquet")


def _row(uid: str) -> dict[str, object]:
    return {
        "publication_uid": uid,
        "publication_number": uid.split(":")[1],
        "patent_number": uid.split(":")[1],
        "application_number": "17123456",
        "publication_date": "2026-01-02T00:00:00Z",
        "filing_date": "2025-01-01T00:00:00Z",
        "doc_kind": "grant",
        "title": "Fixture",
        "applicant": ["Acme"],
        "assignee": ["Acme"],
        "ipc": ["A01B"],
        "us_class": ["123"],
        "source_generation": "sha256:" + "1" * 64,
        "source_pack": "sha256:" + "2" * 64,
        "source_pack_key": "source/v1/packs/" + "2" * 64 + ".zst",
        "source_revision": "sha256:" + "3" * 64,
        "offset": 0,
        "length": 100,
        "uncompressed_length": 200,
        "source_fingerprint": "blake2b-128:" + "4" * 32,
    }


def test_metadata_projection_is_bucketed_sorted_and_repeatable(tmp_path: Path):
    input_path = tmp_path / "projection.ndjson"
    input_path.write_text("\n".join(json.dumps(_row(uid)) for uid in ["US:2:B2", "US:1:B2"]) + "\n")

    first = project_ndjson_to_parquet(input_path, tmp_path / "a", "snapshot-1", bucket_count=4)
    second = project_ndjson_to_parquet(input_path, tmp_path / "b", "snapshot-1", bucket_count=4)

    assert first.publication_count == second.publication_count == 2
    assert first.manifest_sha256 == second.manifest_sha256
    first_parts = sorted((part["key"], part["sha256"]) for part in first.parts)
    second_parts = sorted((part["key"], part["sha256"]) for part in second.parts)
    assert first_parts == second_parts
    point_uids = []
    for point in (part for part in first.parts if part["projection"] == "point"):
        table = parquet.read_table(tmp_path / "a" / point["key"])
        point_uids.extend(table.column("publication_uid").to_pylist())
    assert sorted(point_uids) == ["US:1:B2", "US:2:B2"]
    analytics = next(part for part in first.parts if part["projection"] == "analytics")
    analytics_table = parquet.read_table(tmp_path / "a" / analytics["key"])
    assert analytics_table.column("publication_uid").to_pylist() == ["US:1:B2", "US:2:B2"]


def test_metadata_projection_rejects_duplicate_publication_uid(tmp_path: Path):
    input_path = tmp_path / "projection.ndjson"
    row = _row("US:1:B2")
    input_path.write_text(json.dumps(row) + "\n" + json.dumps(row) + "\n")

    with pytest.raises(MetadataProjectionError, match="duplicate publication_uid"):
        project_ndjson_to_parquet(input_path, tmp_path / "out", "snapshot-1", bucket_count=4)


def test_metadata_projection_requires_digest_prefix(tmp_path: Path):
    input_path = tmp_path / "projection.ndjson"
    row = _row("US:1:B2")
    row["source_pack"] = row["source_pack"][len("sha256:") :]
    input_path.write_text(json.dumps(row) + "\n")

    with pytest.raises(MetadataProjectionError, match="source_pack must use sha256"):
        project_ndjson_to_parquet(input_path, tmp_path / "out", "bad-prefix", bucket_count=4)


def test_analytics_parts_are_bounded_and_all_rows_are_represented(tmp_path: Path):
    input_path = tmp_path / "projection.ndjson"
    input_path.write_text(
        "\n".join(json.dumps(_row(f"US:{number}:B2")) for number in range(5)) + "\n"
    )
    projection = project_ndjson_to_parquet(
        input_path,
        tmp_path / "out",
        "snapshot-1",
        bucket_count=4,
        analytics_rows_per_part=2,
    )
    analytics = [part for part in projection.parts if part["projection"] == "analytics"]
    assert [part["rows"] for part in analytics] == [2, 2, 1]
    assert len({part["key"] for part in analytics}) == 3
    assert sum(part["rows"] for part in projection.parts if part["projection"] == "point") == 5
