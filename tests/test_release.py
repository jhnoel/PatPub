from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from ingestion.models import canonical_json
from ingestion.release import ReleaseError, ReleaseManifest, ReleasePublisher, artifact_from_store
from ingestion.store import LocalBlobStore


def _manifest(store: LocalBlobStore) -> ReleaseManifest:
    store._safe("source/v1/packs/test.zst").parent.mkdir(parents=True, exist_ok=True)
    store._safe("source/v1/packs/test.zst").write_bytes(b"frame")
    generation = "sha256:" + "a" * 64
    generation_key = "source/v1/generations/" + "a" * 64 + ".json"
    store._safe(generation_key).parent.mkdir(parents=True, exist_ok=True)
    generation_payload = {
        "generation_id": generation,
        "publication_count": 1,
        "source_packs": [
            {
                "key": "source/v1/packs/test.zst",
                "sha256": hashlib.sha256(b"frame").hexdigest(),
                "bytes": 5,
            }
        ],
    }
    store._safe(generation_key).write_bytes(canonical_json(generation_payload))
    part_key = "metadata/v1/snapshot-1/part.parquet"
    store._safe(part_key).parent.mkdir(parents=True, exist_ok=True)
    store._safe(part_key).write_bytes(b"part")
    metadata_manifest_key = "metadata/v1/snapshot-1/manifest.json"
    metadata_manifest = {
        "schema_version": 1,
        "publication_count": 1,
        "parts": [{"key": part_key, "sha256": hashlib.sha256(b"part").hexdigest(), "bytes": 4, "rows": 1}],
    }
    store._safe(metadata_manifest_key).write_bytes(canonical_json(metadata_manifest))
    split_key = "quickwit/indexes/patents-test-1/test-split.split"
    store._safe(split_key).parent.mkdir(parents=True, exist_ok=True)
    store._safe(split_key).write_bytes(b"split")
    metastore_key = "quickwit/metastores/test/metastore.json"
    store._safe(metastore_key).parent.mkdir(parents=True, exist_ok=True)
    metastore = {
        "version": "0.9",
        "index": {"index_config": {"index_id": "patents-test-1"}},
        "splits": [{"split_id": "test-split", "num_docs": 1}],
    }
    store._safe(metastore_key).write_bytes(canonical_json(metastore))
    return ReleaseManifest(
        release_id="release-001",
        publication_count=1,
        source_generations=(generation,),
        canonical_text_profile="patpubrender-test-v1",
        source={
            "generation_manifest": artifact_from_store(store, generation_key),
            "source_pack": artifact_from_store(store, "source/v1/packs/test.zst"),
        },
        fulltext={
            "index_id": "patents-test-1",
            "metastore": artifact_from_store(store, metastore_key),
            "splits": [artifact_from_store(store, split_key)],
        },
        metadata={
            "parquet": {
                "manifest": artifact_from_store(store, metadata_manifest_key),
                "parts": [artifact_from_store(store, part_key)],
            }
        },
    )


def test_release_is_validated_and_activated_atomically(tmp_path: Path):
    store = LocalBlobStore(tmp_path / "blob")
    publisher = ReleasePublisher(store)
    manifest = _manifest(store)

    payload = publisher.activate(manifest)

    assert json.loads(store.read("releases/current.json")) == payload
    assert publisher.verify_selector(payload).release_id == "release-001"

    with pytest.raises(ReleaseError, match="changed concurrently"):
        publisher.activate(manifest, expected_selector_sha256="0" * 64)


def test_release_rejects_missing_or_modified_artifact(tmp_path: Path):
    store = LocalBlobStore(tmp_path / "blob")
    publisher = ReleasePublisher(store)
    manifest = ReleaseManifest(
        release_id="release-bad",
        publication_count=1,
        source_generations=("sha256:" + "b" * 64,),
        canonical_text_profile="profile",
        fulltext={
            "index": {
                "key": "missing",
                "sha256": hashlib.sha256(b"missing").hexdigest(),
                "bytes": 7,
            }
        },
    )
    with pytest.raises(ReleaseError, match="source must reference"):
        publisher.publish(manifest)


def test_selector_digest_uses_canonical_newline_bytes(tmp_path: Path):
    store = LocalBlobStore(tmp_path / "blob")
    publisher = ReleasePublisher(store)
    manifest = _manifest(store)
    selector = publisher.activate(manifest)

    raw_manifest = store.read("releases/release-001.json")
    assert raw_manifest.endswith(b"\n")
    assert hashlib.sha256(canonical_json(json.loads(raw_manifest))).hexdigest() == selector[
        "manifest_sha256"
    ]
    assert publisher.verify_selector(selector).release_id == manifest.release_id


def test_selector_verification_rechecks_semantic_artifacts(tmp_path: Path):
    store = LocalBlobStore(tmp_path / "blob")
    publisher = ReleasePublisher(store)
    manifest = _manifest(store)
    selector = publisher.activate(manifest)
    metastore = store._safe("quickwit/metastores/test/metastore.json")
    metastore.write_bytes(b'{"index":{"index_config":{"index_id":"wrong"}},"splits":[]}\n')
    with pytest.raises(ReleaseError, match="hash mismatch"):
        publisher.verify_selector(selector)


def test_legacy_projection_reference_does_not_require_the_ndjson_blob(tmp_path: Path):
    store = LocalBlobStore(tmp_path / "blob")
    publisher = ReleasePublisher(store)
    manifest = _manifest(store)
    manifest = ReleaseManifest(
        release_id=manifest.release_id,
        publication_count=manifest.publication_count,
        source_generations=manifest.source_generations,
        canonical_text_profile=manifest.canonical_text_profile,
        source=dict(manifest.source, projections=[{
            "generation_id": manifest.source_generations[0],
            "artifact": {"key": "projections/missing.ndjson", "sha256": "a" * 64, "bytes": 1},
        }]),
        fulltext=manifest.fulltext,
        metadata=manifest.metadata,
    )
    selector = publisher.activate(manifest)
    assert publisher.verify_selector(selector).release_id == manifest.release_id


def test_selector_requires_manifest_release_identity(tmp_path: Path):
    store = LocalBlobStore(tmp_path / "blob")
    publisher = ReleasePublisher(store)
    manifest = _manifest(store)
    selector = publisher.activate(manifest)
    key = "releases/release-001.json"
    payload = json.loads(store.read(key))
    payload["release_id"] = "release-evil"
    store._safe(key).write_bytes(canonical_json(payload))
    selector["manifest_sha256"] = hashlib.sha256(canonical_json(payload)).hexdigest()
    with pytest.raises(ReleaseError, match="release_id"):
        publisher.verify_selector(selector)


def test_incremental_release_keeps_two_immutable_shards_and_snapshots(tmp_path: Path):
    store = LocalBlobStore(tmp_path / "blob")
    first = _manifest(store)
    second_generation = "sha256:" + "b" * 64
    second_pack = "source/v1/packs/second.zst"
    store._safe(second_pack).parent.mkdir(parents=True, exist_ok=True)
    store._safe(second_pack).write_bytes(b"second-frame")
    second_generation_key = "source/v1/generations/" + "b" * 64 + ".json"
    store._safe(second_generation_key).parent.mkdir(parents=True, exist_ok=True)
    store._safe(second_generation_key).write_bytes(canonical_json({
        "generation_id": second_generation, "publication_count": 1,
        "source_packs": [artifact_from_store(store, second_pack)],
    }))
    second_part = "metadata/v1/snapshot-2/part.parquet"
    store._safe(second_part).parent.mkdir(parents=True, exist_ok=True)
    store._safe(second_part).write_bytes(b"part2")
    second_manifest_key = "metadata/v1/snapshot-2/manifest.json"
    store._safe(second_manifest_key).write_bytes(canonical_json({
        "schema_version": 1, "publication_count": 1,
        "parts": [artifact_from_store(store, second_part) | {"rows": 1}],
    }))
    second_split = "quickwit/indexes/patents-test-2/second-split.split"
    store._safe(second_split).parent.mkdir(parents=True, exist_ok=True)
    store._safe(second_split).write_bytes(b"split2")
    second_metastore = "quickwit/metastores/test/patents-test-2/metastore.json"
    store._safe(second_metastore).parent.mkdir(parents=True, exist_ok=True)
    store._safe(second_metastore).write_bytes(canonical_json({
        "version": "0.9", "index": {"index_config": {"index_id": "patents-test-2"}},
        "splits": [{"split_id": "second-split", "num_docs": 1}],
    }))
    manifest = ReleaseManifest(
        release_id="release-incremental",
        publication_count=2,
        source_generations=(first.source_generations[0], second_generation),
        canonical_text_profile=first.canonical_text_profile,
        source={
            "source_packs": [first.source["source_pack"], artifact_from_store(store, second_pack)],
            "generation_manifests": [
                {"generation_id": first.source_generations[0], "artifact": first.source["generation_manifest"]},
                {"generation_id": second_generation, "artifact": artifact_from_store(store, second_generation_key)},
            ],
        },
        fulltext={"shards": [
            dict(first.fulltext, generation_id=first.source_generations[0], publication_count=1),
            {"generation_id": second_generation, "publication_count": 1,
             "index_id": "patents-test-2", "metastore": artifact_from_store(store, second_metastore),
             "splits": [artifact_from_store(store, second_split)]},
        ]},
        metadata={"snapshots": [
            dict(first.metadata, generation_id=first.source_generations[0], publication_count=1),
            {"generation_id": second_generation, "publication_count": 1,
             "parquet": {"manifest": artifact_from_store(store, second_manifest_key),
                         "parts": [artifact_from_store(store, second_part)]}},
        ]},
    )
    publisher = ReleasePublisher(store)
    selector = publisher.activate(manifest)
    assert publisher.verify_selector(selector).publication_count == 2
    assert len(publisher.verify_selector(selector).fulltext["shards"]) == 2


def test_azure_immutable_json_cache_keeps_selector_fresh(monkeypatch):
    from ingestion.release import AzureBlobReleaseStore
    monkeypatch.setenv("CACHE_IMMUTABLE_JSON", "true")
    store = object.__new__(AzureBlobReleaseStore)
    calls = []
    versions = {"source/generation.json": b"immutable", "releases/current.json": b"first"}
    class Blob:
        def __init__(self, key): self.key = key
        def download_blob(self):
            calls.append(self.key)
            return self
        def readall(self): return versions[self.key]
    store._blob = Blob
    store.prefetch_json(versions)
    assert store.read("source/generation.json") == b"immutable"
    assert store.read("releases/current.json") == b"first"
    versions["releases/current.json"] = b"second"
    assert store.read("releases/current.json") == b"second"
    assert calls.count("source/generation.json") == 1
    assert calls.count("releases/current.json") == 2
