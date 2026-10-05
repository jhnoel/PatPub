from __future__ import annotations

import pytest

from ingestion import orchestration_cloud as cloud
from ingestion.orchestration import PipelineEvent
from ingestion.store import LocalBlobStore


@pytest.mark.parametrize("new_profile", ["v1", "v2"])
def test_new_archive_keeps_prior_generation_without_projection_blobs(tmp_path, monkeypatch, new_profile):
    store = LocalBlobStore(tmp_path)
    old = "sha256:" + "a" * 64
    new = "sha256:" + "b" * 64
    prior = {
        "publication_count": 2,
        "canonical_text_profile": "v1",
        "source_generations": [old],
        "source": {"generation_manifests": [{"generation_id": old, "artifact": {"key": "old"}}]},
        "fulltext": {"index_id": "patents", "document_count": 2},
        "metadata": {"snapshots": [{"generation_id": old, "publication_count": 2, "parquet": {}}]},
    }
    store.write_json_if_absent("releases/old.json", prior)
    store.write_json_if_absent("old", {"generation_id": old, "source_identity": "f" * 64, "publication_count": 2})
    store.write_json_if_absent("releases/current.json", {"manifest_key": "releases/old.json"})
    projection = {"key": "projections/new.ndjson", "sha256": "c" * 64, "bytes": 1}
    monkeypatch.setattr(cloud, "run_worker", lambda _store, stage, _payload: {
        "render": {"source_packs": [], "projection": projection, "publication_count": 1},
        "metadata": {"metadata": {"parquet": {}, "publication_count": 1}},
        "index": {"fulltext": {"index_id": "patents-" + "c" * 12}, "publication_count": 1},
    }[stage])
    indexed = {}

    def capture_index(_store, payload):
        indexed.update(payload)
        return {
            "fulltext": {"index_id": "patents", "document_count": payload["expected_total_count"]},
            "publication_count": 1,
        }

    monkeypatch.setattr(cloud, "index_with_serving_paused", capture_index)
    monkeypatch.setattr(cloud, "verify_correction_coverage", lambda *args: None)

    class FakeOrchestrator:
        def __init__(self, _store):
            pass

        def run_generation(self, _event):
            return {"generation_id": new, "manifest": {"key": "new"}}

        def run_release(self, event, _canary):
            return dict(event.payload)

    monkeypatch.setattr(cloud, "DownstreamOrchestrator", FakeOrchestrator)
    event = PipelineEvent("render", {
        "raw": {"key": "raw/new.zip", "sha256": "d" * 64, "bytes": 1},
        "source_generation": new, "source_identity": "e" * 64, "renderer_profile": new_profile,
    })
    release = cloud.run_pipeline(store, event)
    assert release["publication_count"] == 3
    assert release["fulltext"]["index_id"] == "patents"
    assert [item["generation_id"] for item in release["metadata"]["snapshots"]] == [old, new]
    assert len(release["generation_manifests"]) == 2
    assert "projections" not in release
    assert indexed["raw"]["key"] == "raw/new.zip"
    assert indexed["projection"] == {"sha256": "c" * 64, "bytes": 1}
    assert indexed["purge_generation_ids"] == []
    assert indexed["renderer_profile"] == new_profile


@pytest.mark.parametrize("new_profile", ["v1", "v2"])
def test_revision_supersedes_prior_generation(tmp_path, monkeypatch, new_profile):
    store = LocalBlobStore(tmp_path)
    old = "sha256:" + "a" * 64
    new = "sha256:" + "b" * 64
    identity = "e" * 64
    store.write_json_if_absent("old-generation", {"generation_id": old, "source_identity": identity, "publication_count": 2})
    store.write_json_if_absent("releases/old.json", {
        "release_id": "old", "publication_count": 2, "canonical_text_profile": "v1", "source_generations": [old],
        "source": {"generation_manifests": [{"generation_id": old, "artifact": {"key": "old-generation"}}]},
        "fulltext": {"index_id": "patents", "document_count": 2},
        "metadata": {"snapshots": [{"generation_id": old, "publication_count": 2, "parquet": {}}]},
    })
    store.write_json_if_absent("releases/current.json", {"manifest_key": "releases/old.json"})
    projection = {"key": "projections/new.ndjson", "sha256": "c" * 64, "bytes": 1}
    monkeypatch.setattr(cloud, "run_worker", lambda _store, stage, _payload: {
        "render": {"source_packs": [], "projection": projection, "publication_count": 1},
        "metadata": {"metadata": {"parquet": {}}, "publication_count": 1},
        "index": {"fulltext": {"index_id": "patents-" + "c" * 12}, "publication_count": 1},
    }[stage])
    indexed = {}

    def capture_index(_store, payload):
        indexed.update(payload)
        return {
            "fulltext": {"index_id": "patents", "document_count": payload["expected_total_count"]},
            "publication_count": 1,
        }

    monkeypatch.setattr(cloud, "index_with_serving_paused", capture_index)
    monkeypatch.setattr(cloud, "verify_correction_coverage", lambda *args: None)

    class FakeOrchestrator:
        def __init__(self, _store):
            pass

        def run_generation(self, _event):
            return {"generation_id": new, "manifest": {"key": "new"}}

        def run_release(self, event, _canary):
            return dict(event.payload)

    monkeypatch.setattr(cloud, "DownstreamOrchestrator", FakeOrchestrator)
    release = cloud.run_pipeline(store, PipelineEvent("render", {
        "raw": {"key": "raw/new.zip", "sha256": "d" * 64, "bytes": 1},
        "source_generation": new, "source_identity": identity, "renderer_profile": new_profile,
    }))
    assert release["superseded_generation_ids"] == [old]
    assert release["publication_count"] == 1
    assert release["fulltext"]["document_count"] == 1
    assert indexed["purge_generation_ids"] == []  # Superseded hits stay in historical splits.


def test_correction_cannot_silently_remove_an_unexplained_publication():
    import io
    import hashlib
    import json
    from types import SimpleNamespace
    pa = pytest.importorskip('pyarrow')
    pq = pytest.importorskip('pyarrow.parquet')
    blobs = {}
    def snapshot(key, uids):
        output = io.BytesIO()
        pq.write_table(pa.table({'publication_uid': uids}), output)
        data = output.getvalue()
        blobs[key] = data
        return {'publication_count': len(uids), 'parquet': {'parts': [{'key': key, 'sha256': hashlib.sha256(data).hexdigest(), 'bytes': len(data), 'projection': 'point'}]}}
    old = snapshot('old', ['US:1:B2', 'US:2:B2'])
    new = snapshot('new', ['US:2:B2'])
    proof_key = 'migration/v1/correction-withdrawal-exact.json'
    blobs[proof_key] = json.dumps({'publications': []}).encode()
    store = SimpleNamespace(read=lambda key: blobs[key])
    with pytest.raises(RuntimeError, match='explicit preservation'):
        cloud.verify_correction_coverage(store, old, new)
    blobs[proof_key] = json.dumps({'publications': [{'publication_uid': 'US:1:B2', 'candidate_found': True}]}).encode()
    cloud.verify_correction_coverage(store, old, new)
