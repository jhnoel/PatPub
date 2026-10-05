from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from ingestion.models import canonical_json
from ingestion.orchestration import (
    DownstreamOrchestrator,
    OrchestrationError,
    PipelineEvent,
)
from ingestion.store import LocalBlobStore


def put(store: LocalBlobStore, key: str, data: bytes) -> dict[str, object]:
    path = store._safe(key)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)
    return {"key": key, "sha256": hashlib.sha256(data).hexdigest(), "bytes": len(data)}


def put_json(store: LocalBlobStore, key: str, value: object) -> dict[str, object]:
    return put(store, key, canonical_json(value))


def test_event_identity_is_deterministic_and_excludes_retry_attempt():
    payload = {
        "publication_count": 1,
        "renderer_profile": "renderer-v1",
        "source_packs": [{"key": "source/v1/packs/a.zst", "sha256": "a" * 64, "bytes": 1}],
    }
    first = PipelineEvent("generation", payload)
    retry = PipelineEvent("generation", payload, attempt=4)

    assert first.event_id == retry.event_id
    assert first.fingerprint == retry.fingerprint
    assert PipelineEvent.from_dict(first.as_dict()) == first
    with pytest.raises(OrchestrationError, match="identity"):
        PipelineEvent.from_dict(first.as_dict() | {"event_id": "generation-tampered"})


def test_artifact_reuses_verified_version_but_checks_changed_objects(tmp_path):
    store = LocalBlobStore(tmp_path / "blob")
    artifact = put(store, "source/a", b"a")
    store.verified_artifact = lambda reference: True
    store._safe(artifact["key"]).unlink()
    orchestrator = DownstreamOrchestrator(store)
    assert orchestrator._artifact(artifact, "source") == artifact
    store.verified_artifact = lambda reference: False
    with pytest.raises(OrchestrationError, match="missing"):
        orchestrator._artifact(artifact, "source")


def test_generation_checks_dependencies_and_is_idempotent(tmp_path: Path):
    store = LocalBlobStore(tmp_path / "blob")
    orchestrator = DownstreamOrchestrator(store)
    source_ref = {
        "key": "source/v1/packs/a.zst",
        "sha256": hashlib.sha256(b"a").hexdigest(),
        "bytes": 1,
    }
    event = PipelineEvent(
        "generation",
        {
            "publication_count": 1,
            "renderer_profile": "renderer-v1",
            "source_packs": [source_ref],
        },
    )
    with pytest.raises(OrchestrationError, match="missing"):
        orchestrator.run_generation(event)

    put(store, "source/v1/packs/a.zst", b"a")
    first = orchestrator.run_generation(event)
    second = orchestrator.run_generation(PipelineEvent("generation", event.payload, attempt=1))
    assert first == second
    assert first["generation_id"].startswith("sha256:")
    assert store.exists(first["manifest"]["key"])


def test_release_requires_canary_and_activates_once(tmp_path: Path):
    store = LocalBlobStore(tmp_path / "blob")
    orchestrator = DownstreamOrchestrator(store)
    source_ref = put(store, "source/v1/packs/a.zst", b"a")
    generation_event = PipelineEvent(
        "generation",
        {
            "publication_count": 1,
            "renderer_profile": "renderer-v1",
            "source_packs": [source_ref],
        },
    )
    generation = orchestrator.run_generation(generation_event)
    metastore_ref = put_json(
        store,
        "fulltext/v1/r/metastore.json",
        {
            "index": {"index_config": {"index_id": "patents-r"}},
            "splits": [{"split_id": "part" , "num_docs": 1}],
        },
    )
    split_ref = put(store, "fulltext/v1/r/part.split", b"split")
    metadata_part = put(store, "metadata/v1/r/part.parquet", b"parquet")
    metadata_manifest = put_json(
        store,
        "metadata/v1/r/manifest.json",
        {"schema_version": 1, "publication_count": 1, "parts": [metadata_part]},
    )
    release_event = PipelineEvent(
        "release",
        {
            "publication_count": 1,
            "canonical_text_profile": "renderer-v1",
            "generation_manifest": generation["manifest"],
            "fulltext": {
                "index_id": "patents-r",
                "metastore": metastore_ref,
                "splits": [split_ref],
            },
            "metadata": {
                "parquet": {"manifest": metadata_manifest, "parts": [metadata_part]}
            },
        },
    )
    with pytest.raises(OrchestrationError, match="canary"):
        orchestrator.run_release(release_event, lambda _manifest: False)
    assert not store.exists("releases/current.json")

    accepted = orchestrator.run_release(release_event, lambda _manifest: True)
    repeated = orchestrator.run_release(release_event, lambda _manifest: False)
    assert accepted == repeated
    assert store.read_json("releases/current.json")["release_id"] == accepted["release_id"]


def test_activation_replay_does_not_restore_superseded_release(tmp_path: Path, monkeypatch):
    store = LocalBlobStore(tmp_path / "blob")
    orchestrator = DownstreamOrchestrator(store)
    source = put(store, "source/packs/a.zst", b"a")
    generation = orchestrator.run_generation(PipelineEvent("generation", {
        "publication_count": 1, "renderer_profile": "v1", "source_packs": [source],
    }))
    metastore = put_json(store, "quickwit/meta.json", {
        "index": {"index_config": {"index_id": "one"}},
        "splits": [{"split_id": "s", "num_docs": 1}],
    })
    split = put(store, "quickwit/s.split", b"s")
    part = put(store, "metadata/part.parquet", b"p")
    metadata_manifest = put_json(store, "metadata/manifest.json", {
        "schema_version": 1, "publication_count": 1, "parts": [part],
    })
    event = PipelineEvent("release", {
        "publication_count": 1, "canonical_text_profile": "v1",
        "generation_manifest": generation["manifest"],
        "fulltext": {"index_id": "one", "metastore": metastore, "splits": [split]},
        "metadata": {"parquet": {"manifest": metadata_manifest, "parts": [part]}},
    })
    original_complete = orchestrator._complete
    monkeypatch.setattr(orchestrator, "_complete", lambda *_args: (_ for _ in ()).throw(RuntimeError("crash")))
    with pytest.raises(RuntimeError, match="crash"):
        orchestrator.run_release(event, lambda _: True)
    monkeypatch.setattr(orchestrator, "_complete", original_complete)
    selector = store.read("releases/current.json")
    newer = {"release_id": "newer", "manifest_key": "releases/newer.json"}
    assert store.compare_and_swap_json("releases/current.json", hashlib.sha256(selector).hexdigest(), newer)
    with pytest.raises(OrchestrationError, match="superseded"):
        orchestrator.run_release(PipelineEvent("release", event.payload, attempt=2), lambda _: True)
    assert store.read_json("releases/current.json") == newer


def test_release_event_fails_when_generation_count_changes(tmp_path: Path):
    store = LocalBlobStore(tmp_path / "blob")
    orchestrator = DownstreamOrchestrator(store)
    source_ref = put(store, "source/v1/packs/a.zst", b"a")
    generation = orchestrator.run_generation(
        PipelineEvent(
            "generation",
            {
                "publication_count": 1,
                "renderer_profile": "renderer-v1",
                "source_packs": [source_ref],
            },
        )
    )
    event = PipelineEvent(
        "release",
        {
            "publication_count": 2,
            "canonical_text_profile": "renderer-v1",
            "generation_manifest": generation["manifest"],
            "fulltext": {},
            "metadata": {},
        },
    )
    with pytest.raises(OrchestrationError, match="publication_count"):
        orchestrator.run_release(event, lambda _manifest: True)


@pytest.mark.parametrize("second_profile", ["renderer-v1", "renderer-v2"])
def test_release_event_binds_multiple_generations_and_packs(tmp_path: Path, second_profile):
    store = LocalBlobStore(tmp_path / "blob")
    orchestrator = DownstreamOrchestrator(store)
    first_ref = put(store, "source/v1/packs/first.zst", b"first")
    second_ref = put(store, "source/v1/packs/second.zst", b"second")
    def generation(ref, profile):
        return orchestrator.run_generation(
            PipelineEvent(
                "generation",
                {
                    "publication_count": 1,
                    "renderer_profile": profile,
                    "source_packs": [ref],
                },
            )
        )

    first = generation(first_ref, "renderer-v1")
    second = generation(second_ref, second_profile)
    metastore_ref = put_json(
        store,
        "fulltext/v1/multi/metastore.json",
        {
            "index": {"index_config": {"index_id": "patents-multi"}},
            "splits": [{"split_id": "part", "num_docs": 2}],
        },
    )
    split_ref = put(store, "fulltext/v1/multi/part.split", b"split")
    metadata_part = put(store, "metadata/v1/multi/part.parquet", b"parquet")
    metadata_manifest = put_json(
        store,
        "metadata/v1/multi/manifest.json",
        {"schema_version": 1, "publication_count": 2, "parts": [metadata_part]},
    )
    event = PipelineEvent(
        "release",
        {
            "publication_count": 2,
            "canonical_text_profile": "renderer-v1",
            "generation_manifests": [first["manifest"], second["manifest"]],
            "fulltext": {
                "index_id": "patents-multi",
                "metastore": metastore_ref,
                "splits": [split_ref],
            },
            "metadata": {
                "parquet": {"manifest": metadata_manifest, "parts": [metadata_part]}
            },
        },
    )
    result = orchestrator.run_release(event, lambda manifest: len(manifest["source_generations"]) == 2)
    assert store.read_json("releases/current.json")["release_id"] == result["release_id"]
    selector = store.read_json("releases/current.json")
    manifest = orchestrator.publisher.verify_selector(selector)
    if second_profile != "renderer-v1":
        from ingestion.release import MIXED_RENDERER_PROFILE, ReleaseError
        from dataclasses import replace
        assert manifest.canonical_text_profile == MIXED_RENDERER_PROFILE
        assert manifest.source["renderer_profiles"] == {
            first["generation_id"]: "renderer-v1", second["generation_id"]: second_profile,
        }
        wrong = dict(manifest.source)
        wrong["renderer_profiles"] = {
            first["generation_id"]: second_profile, second["generation_id"]: "renderer-v1",
        }
        with pytest.raises(ReleaseError, match="renderer profile mismatch"):
            orchestrator.publisher.validate(replace(manifest, source=wrong))
