from __future__ import annotations

import importlib.util
import json
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest


def _load_fetch():
    path = Path(__file__).resolve().parents[1] / "fetch-worker" / "fetch_cloud.py"
    spec = importlib.util.spec_from_file_location("fetch_cloud_rerender", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


fetch_cloud = _load_fetch()


class Store:
    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], bytes] = {}
        self.modified: dict[tuple[str, str], datetime] = {}
        self.messages: list[str] = []

    def put(self, container: str, name: str, payload: dict, modified: datetime | None = None) -> None:
        self.objects[(container, name)] = json.dumps(payload).encode()
        if modified is not None:
            self.modified[(container, name)] = modified

    def get_container_client(self, container: str):
        return Container(self, container)

    def get_blob_client(self, container: str, name: str):
        return Blob(self, container, name)


class Container:
    def __init__(self, store: Store, container: str) -> None:
        self.store = store
        self.container = container

    def list_blobs(self, *, name_starts_with: str):
        return [
            SimpleNamespace(name=name, last_modified=modified)
            for (container, name), modified in self.store.modified.items()
            if container == self.container and name.startswith(name_starts_with)
        ]

    def get_blob_client(self, name: str):
        return Blob(self.store, self.container, name)


class Blob:
    def __init__(self, store: Store, container: str, name: str) -> None:
        self.store = store
        self.container = container
        self.name = name

    def exists(self) -> bool:
        return (self.container, self.name) in self.store.objects

    def download_blob(self):
        data = self.store.objects[(self.container, self.name)]
        return SimpleNamespace(readall=lambda: data)

    def upload_blob(self, data, **_kwargs) -> None:
        self.store.objects[(self.container, self.name)] = data


class Queue:
    def __init__(self, store: Store) -> None:
        self.store = store

    def send_message(self, content: str) -> None:
        self.store.messages.append(content)


def _marker(name: str, sha: str) -> dict:
    return {
        "product": "PTGRXML",
        "fileName": name,
        "raw": {"key": f"raw/v1/PTGRXML/{sha}.zip", "sha256": sha, "bytes": 10},
    }


def test_rerender_reads_the_newest_landed_zip_and_skips_uspto():
    store = Store()
    older = datetime(2026, 9, 15, tzinfo=timezone.utc)
    newer = datetime(2026, 9, 22, tzinfo=timezone.utc)
    old_sha = "a" * 64
    new_sha = "b" * 64
    store.put("releases", "fetch/v1/PTGRXML/old.json", _marker("ipg.zip", old_sha), older)
    store.put("releases", "fetch/v1/PTGRXML/new.json", _marker("ipg.zip", new_sha), newer)
    store.put("raw", f"v1/PTGRXML/{old_sha}.zip", {"present": True})
    store.put("raw", f"v1/PTGRXML/{new_sha}.zip", {"present": True})
    queued = fetch_cloud.rerender_from_blob(store, Queue(store))
    assert queued == 1
    event_key = json.loads(store.messages[0])["event_key"]
    event = json.loads(store.objects[("releases", event_key)])
    assert event["payload"]["raw"]["sha256"] == new_sha
    assert event["payload"]["renderer_profile"] == fetch_cloud.PROFILE
    assert event["payload"]["source_generation"] == fetch_cloud.source_generation(new_sha)


def test_rerender_skips_a_generation_the_active_release_already_serves():
    store = Store()
    sha = "c" * 64
    store.put("releases", "fetch/v1/PTGRXML/one.json", _marker("ipg.zip", sha),
              datetime(2026, 9, 22, tzinfo=timezone.utc))
    store.put("raw", f"v1/PTGRXML/{sha}.zip", {"present": True})
    generation = fetch_cloud.source_generation(sha)
    store.put("releases", "releases/current.json", {"manifest_key": "releases/live.json"})
    store.put("releases", "releases/live.json", {
        "source_generations": [generation],
        "source": {"superseded_generation_ids": []},
    })
    assert fetch_cloud.rerender_from_blob(store, Queue(store)) == 0
    assert store.messages == []


def test_profile_change_reenqueues_the_same_blob(monkeypatch):
    store = Store()
    sha = "d" * 64
    store.put("releases", "fetch/v1/PTGRXML/one.json", _marker("ipg.zip", sha),
              datetime(2026, 9, 22, tzinfo=timezone.utc))
    store.put("raw", f"v1/PTGRXML/{sha}.zip", {"present": True})
    old_profile = fetch_cloud.PROFILE
    old_generation = fetch_cloud.source_generation(sha, old_profile)
    store.put("releases", "releases/current.json", {"manifest_key": "releases/live.json"})
    store.put("releases", "releases/live.json", {
        "source_generations": [old_generation],
        "source": {"superseded_generation_ids": []},
    })
    monkeypatch.setattr(fetch_cloud, "PROFILE", old_profile + ":revised")
    assert fetch_cloud.rerender_from_blob(store, Queue(store)) == 1
    event = json.loads(store.objects[("releases", json.loads(store.messages[0])["event_key"])])
    assert event["payload"]["renderer_profile"] == old_profile + ":revised"
    assert event["payload"]["raw"]["key"] == f"raw/v1/PTGRXML/{sha}.zip"
    assert event["payload"]["source_generation"] != old_generation


def test_rerender_fails_when_the_raw_blob_is_missing():
    store = Store()
    sha = "e" * 64
    store.put("releases", "fetch/v1/PTGRXML/one.json", _marker("ipg.zip", sha),
              datetime(2026, 9, 22, tzinfo=timezone.utc))
    with pytest.raises(RuntimeError, match="raw archive"):
        fetch_cloud.rerender_from_blob(store, Queue(store))


def test_fetch_one_skips_a_seeded_marker_before_downloading(monkeypatch):
    store = Store()
    item = {
        "fileName": "ipg260922.zip",
        "fileDownloadURI": "https://api.uspto.gov/api/v1/datasets/products/files/PTGRXML/2026/ipg260922.zip",
        "fileSize": 1,
    }
    monkeypatch.setattr(fetch_cloud, "remote_validator", lambda url, api_key: "")
    source_id = fetch_cloud.archive_identity("PTGRXML", item, "")
    store.put("releases", f"fetch/v1/PTGRXML/{source_id}.json", {"product": "PTGRXML"})
    assert fetch_cloud.fetch_one(store, Queue(store), item, "PTGRXML", "key") is False
    assert store.messages == []


def test_correction_source_identity_matches_original_week():
    original = fetch_cloud.publication_source_identity("PTGRXML", "ipg240102.zip")
    assert fetch_cloud.publication_source_identity("PTGRXML", "ipg240102_r2.zip") == original
    assert fetch_cloud.publication_source_identity("PTGRXML", "ipg240109.zip") != original
    assert fetch_cloud.publication_source_identity("APPXML", "ipg240102.zip") != original
