from __future__ import annotations

import hashlib
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace
from urllib.error import HTTPError
from io import BytesIO

import pytest

pytest.importorskip("azure.storage.blob")

module_path = Path(__file__).resolve().parents[1] / "production" / "indexer" / "indexer.py"
spec = importlib.util.spec_from_file_location("indexer", module_path)
assert spec and spec.loader
indexer = importlib.util.module_from_spec(spec)
spec.loader.exec_module(indexer)


class _Blob:
    def __init__(self, data: bytes):
        self.data = data

    def download_blob(self, **_kwargs):
        return SimpleNamespace(readall=lambda: self.data, chunks=lambda: iter([self.data]))

    def upload_blob(self, data, **_kwargs):
        self.data = data


class _Container:
    def __init__(self, blobs: dict[str, bytes]):
        self.blobs = blobs

    def list_blobs(self, *, name_starts_with: str):
        return [
            SimpleNamespace(name=name) for name in self.blobs
            if name.startswith(name_starts_with)
        ]

    def get_blob_client(self, name: str):
        return _Blob(self.blobs[name])


def test_index_inventory_binds_exact_published_splits():
    index_id = "patents-123456789abc"
    metastore = {
        "index": {"index_config": {"index_id": index_id}},
        "splits": [{"split_id": "one", "num_docs": 2}],
    }
    blobs = {
        f"metastores/production-v1/{index_id}/metastore.json": json.dumps(metastore).encode(),
        f"indexes/{index_id}/one.split": b"split contents",
    }
    quickwit = SimpleNamespace(get_container_client=lambda name: _Container(blobs))
    metastore_ref, splits, docs = indexer.inventory(
        quickwit, "metastores/production-v1", index_id
    )
    assert docs == 2
    assert metastore_ref["sha256"] == hashlib.sha256(blobs[next(iter(blobs))]).hexdigest()
    assert splits[0]["key"] == f"quickwit/indexes/{index_id}/one.split"

    blobs[f"indexes/{index_id}/unexpected.split"] = b"not in metastore"
    with pytest.raises(RuntimeError, match="split objects"):
        indexer.inventory(quickwit, "metastores/production-v1", index_id)


def test_partial_generation_is_deleted_then_reingested(monkeypatch):
    counts = iter([2, 2, 0, 3])
    calls = []
    monkeypatch.setattr(indexer, "search_count", lambda *_: next(counts))
    monkeypatch.setattr(indexer, "quickwit_api", lambda *args: calls.append(args))
    monkeypatch.setattr(indexer.time, "sleep", lambda _: None)
    indexer.reconcile_generation("patents", "sha256:" + "a" * 64, 3,
                                     lambda: calls.append("ingest"))
    assert calls[0][1:] == ("POST", {"query": 'source_generation:"sha256:' + "a" * 64 + '"'})
    assert calls[1] == "ingest"


def test_superseded_generation_is_purged_before_the_release_snapshot(monkeypatch):
    counts = iter([4, 4, 0])
    calls = []
    monkeypatch.setattr(indexer, "search_count", lambda *_: next(counts))
    monkeypatch.setattr(indexer, "quickwit_api", lambda *args: calls.append(args))
    monkeypatch.setattr(indexer.time, "sleep", lambda _: None)
    generation = "sha256:" + "b" * 64
    indexer.purge_generations("patents", [generation])
    assert calls == [(
        f"/api/v1/patents/delete-tasks",
        "POST",
        {"query": f'source_generation:"{generation}"'},
    )]


def test_existing_index_switches_off_no_merge(monkeypatch):
    calls = []
    config = {"version": "0.9", "index_id": "patents", "indexing_settings": {"merge_policy": {"type": "no_merge"}}}

    def api(path, method="GET", body=None):
        calls.append((path, method, body))
        if method == "GET":
            return json.dumps(config).encode()
        return b"{}"

    monkeypatch.setattr(indexer, "quickwit_api", api)
    indexer.ensure_incremental_merge_policy("patents")
    assert calls[1][1] == "PUT"
    assert calls[1][2]["indexing_settings"]["merge_policy"]["type"] == "limit_merge"
    assert calls[1][2]["indexing_settings"]["merge_policy"]["maturation_period"] == "3650d"


def test_compaction_wait_returns_when_the_inventory_is_stable(monkeypatch):
    snapshots = iter([
        {"splits": [
            {"split_state": "Published", "num_docs": 100},
            {"split_state": "Staged", "num_docs": 50},
        ]},
        {"splits": [{"split_state": "Published", "num_docs": 150}]},
        {"splits": [{"split_state": "Published", "num_docs": 150}]},
        {"splits": [{"split_state": "Published", "num_docs": 150}]},
    ])
    monkeypatch.setattr(indexer, "quickwit_api", lambda *_a, **_k: json.dumps(next(snapshots)).encode())
    monkeypatch.setattr(indexer.time, "sleep", lambda *_: None)
    assert indexer.wait_for_compaction("patents") == {
        "published": 1, "staged": 0, "small_published": 1,
    }


def test_complete_generation_is_not_reingested(monkeypatch):
    monkeypatch.setattr(indexer, "search_count", lambda *_: 3)
    indexer.reconcile_generation("patents", "sha256:" + "a" * 64, 3,
                                     lambda: pytest.fail("duplicate ingest"))


def test_search_count_waits_for_transient_searcher_registration(monkeypatch):
    calls = []

    def query(*_args):
        calls.append(1)
        if len(calls) == 1:
            raise HTTPError("http://quickwit", 500, "unavailable", {},
                            BytesIO(b"there are no available searcher nodes in the cluster"))
        return b'{"num_hits":13}'

    monkeypatch.setattr(indexer, "quickwit_api", query)
    monkeypatch.setattr(indexer.time, "sleep", lambda _: None)
    assert indexer.search_count("patents", "*") == 13
    assert len(calls) == 2


def test_metastore_snapshot_is_immutable_release_artifact():
    live = b'{"index":{"index_config":{"index_id":"patents"}}}\n'
    digest = hashlib.sha256(live).hexdigest()
    saved = _Blob(b"")
    quickwit = SimpleNamespace(get_blob_client=lambda *_: _Blob(live))
    releases = SimpleNamespace(get_blob_client=lambda key: saved)
    result = indexer.snapshot_metastore(quickwit, releases, {
        "key": "quickwit/metastores/production-v1/patents/metastore.json",
        "sha256": digest, "bytes": len(live),
    })
    assert result == {"key": f"releases/index-metastores/{digest}.json",
                      "sha256": digest, "bytes": len(live)}
    assert saved.data == live


def test_compaction_does_not_wait_for_retired_split_deletion(monkeypatch):
    monkeypatch.setattr(indexer, "split_inventory", lambda _: [
        {"split_state": "Published", "num_docs": 5533},
        {"split_state": "MarkedForDeletion", "num_docs": 100},
    ])
    monkeypatch.setattr(indexer.time, "sleep", lambda _: None)
    assert indexer.wait_for_compaction("patents") == {
        "published": 1, "staged": 0, "small_published": 0,
    }
