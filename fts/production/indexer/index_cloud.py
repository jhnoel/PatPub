"""Build one immutable Quickwit index from a pipeline event.

The raw archive and event live in the corpus account. Quickwit's metastore and
splits live in a separate account. A result is published only after all split
objects and the metastore have been enumerated and hashed.
"""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import time
from typing import Callable
from urllib import error, request

from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
from ingestion.render_local import render_from_raw
from ingestion.search_query import generation_filter


ENDPOINT = "http://127.0.0.1:7390"
INTEGRITY_CACHE = None


def required(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(f"{name} is required")
    return value


def canonical(value: object) -> bytes:
    return (json.dumps(value, separators=(",", ":"), sort_keys=True) + "\n").encode()


def quickwit_api(path: str, method: str = "GET", body: dict | None = None) -> bytes:
    data = canonical(body) if body is not None else None
    headers = {"content-type": "application/json"} if body is not None else {}
    req = request.Request(f"{ENDPOINT}{path}", method=method, data=data, headers=headers)
    with request.urlopen(req, timeout=30) as response:
        return response.read()


def search_count(index_id: str, query: str) -> int:
    deadline = time.monotonic() + 120
    while True:
        try:
            result = json.loads(quickwit_api(
                f"/api/v1/{index_id}/search", "POST", {"query": query, "max_hits": 0}
            ))
            return int(result["num_hits"])
        except error.HTTPError as exc:
            if exc.code != 500 or b"no available searcher nodes" not in exc.read():
                raise
            if time.monotonic() >= deadline:
                raise RuntimeError("Quickwit searcher stayed unavailable") from exc
            time.sleep(2)


def wait_for_searcher(index_id: str, *, timeout_seconds: int = 120) -> None:
    """The HTTP API can be ready before Quickwit registers its searcher node."""
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        try:
            search_count(index_id, "*")
            return
        except error.HTTPError as exc:
            if exc.code != 500 or b"no available searcher nodes" not in exc.read():
                raise
        time.sleep(2)
    raise RuntimeError("Quickwit searcher did not become ready")


def generation_query(generation: str) -> str:
    return f'source_generation:"{generation}"'


INCREMENTAL_MERGE_POLICY = {
    "type": "limit_merge",
    "merge_factor": 10,
    "max_merge_factor": 12,
    "max_merge_ops": 4,
    "maturation_period": "3650d",
}


def ensure_incremental_merge_policy(index_id: str) -> None:
    """Keep an existing index on the incremental merge policy.

    Index create uses index.yaml. An index created under no_merge keeps that
    policy in the metastore until this update. Mature splits already
    published by no_merge stay mature; only splits published after this
    change are compacted automatically.
    """
    payload = json.loads(quickwit_api(f"/api/v1/indexes/{index_id}"))
    config = payload.get("index_config", payload)
    settings = config.setdefault("indexing_settings", {})
    if settings.get("merge_policy") == INCREMENTAL_MERGE_POLICY:
        return
    settings["merge_policy"] = dict(INCREMENTAL_MERGE_POLICY)
    settings.setdefault("commit_timeout_secs", 3600)
    settings.setdefault("split_num_docs_target", 100000)
    quickwit_api(f"/api/v1/indexes/{index_id}", "PUT", config)


def split_inventory(index_id: str) -> list[dict]:
    payload = json.loads(quickwit_api(f"/api/v1/indexes/{index_id}/splits"))
    splits = payload.get("splits", payload)
    if not isinstance(splits, list):
        raise RuntimeError("split inventory response is invalid")
    return [item for item in splits if isinstance(item, dict)]


def wait_for_compaction(index_id: str, *, timeout_seconds: int = 1800) -> dict[str, int]:
    """Wait until merges triggered by this append have published.

    A single weekly archive can leave one immature tail split. Stability
    means no staged splits and an unchanged published inventory, not an
    empty tail.
    """
    deadline = time.monotonic() + timeout_seconds
    previous: tuple | None = None
    stable_polls = 0
    latest = {"published": 0, "staged": 0, "small_published": 0}
    while time.monotonic() < deadline:
        splits = split_inventory(index_id)
        published = [item for item in splits if item.get("split_state") == "Published"]
        staged = [item for item in splits
                  if item.get("split_state") not in {"Published", "MarkedForDeletion"}]
        small = sum(1 for item in published if int(item.get("num_docs") or 0) < 1000)
        latest = {"published": len(published), "staged": len(staged), "small_published": small}
        signature = (latest["published"], latest["staged"], latest["small_published"])
        if signature == previous and latest["staged"] == 0:
            stable_polls += 1
            if stable_polls >= 2:
                return latest
        else:
            stable_polls = 0
        previous = signature
        time.sleep(10)
    raise RuntimeError(f"split compaction did not settle: {latest}")


def purge_generations(index_id: str, generations: list[str], *, timeout_seconds: int = 1800) -> None:
    """Delete superseded generations so the live index keeps only active hits.

    Quickwit retains the split files until its delete tasks finish. This waits
    until each generation's searchable count is zero before the release snapshot.
    """
    for generation in generations:
        if not re.fullmatch(r"sha256:[0-9a-f]{64}", generation):
            raise RuntimeError("refusing to purge an invalid generation id")
        query = generation_query(generation)
        if search_count(index_id, query) == 0:
            continue
        quickwit_api(f"/api/v1/{index_id}/delete-tasks", "POST", {"query": query})
        deadline = time.monotonic() + timeout_seconds
        while time.monotonic() < deadline:
            if search_count(index_id, query) == 0:
                break
            time.sleep(5)
        else:
            raise RuntimeError(f"generation {generation} was still searchable after purge")


def reconcile_generation(index_id: str, generation: str, expected_count: int,
                         ingest: Callable[[], None], *, timeout_seconds: int = 1800) -> None:
    """Replace an interrupted append using Quickwit's durable delete queue.

    Wait for the old documents to disappear before appending. A retry after an
    interrupted delete sees the same generation and can issue another delete.
    """
    query = generation_query(generation)
    existing = search_count(index_id, query)
    if existing == expected_count:
        return
    if existing:
        quickwit_api(f"/api/v1/{index_id}/delete-tasks", "POST", {"query": query})
        deadline = time.monotonic() + timeout_seconds
        while time.monotonic() < deadline:
            if search_count(index_id, query) == 0:
                break
            time.sleep(5)
        else:
            raise RuntimeError("generation delete did not complete before retry timeout")
    ingest()
    observed = search_count(index_id, query)
    if observed != expected_count:
        raise RuntimeError(f"generation has {observed} documents after ingest, expected {expected_count}")


def wait_for_quickwit(process: subprocess.Popen[bytes]) -> None:
    for _ in range(120):
        if process.poll() is not None:
            raise RuntimeError(f"Quickwit exited with status {process.returncode}")
        try:
            quickwit_api("/api/v1/indexes")
            return
        except (error.URLError, TimeoutError):
            time.sleep(1)
    raise RuntimeError("Quickwit did not become ready")


def artifact(blob, logical_key: str) -> dict[str, object]:
    before = blob.get_blob_properties() if INTEGRITY_CACHE is not None else None
    if INTEGRITY_CACHE is not None:
        cache_key = hashlib.sha256(logical_key.encode()).hexdigest()
        try:
            evidence = json.loads(INTEGRITY_CACHE.get_blob_client(
                f"migration/v1/integrity/{cache_key}.json").download_blob().readall())
        except ResourceNotFoundError:
            evidence = None
        if (evidence and evidence.get("etag") == before.etag
                and evidence.get("bytes") == before.size
                and f"{evidence.get('container')}/{evidence.get('key')}" == logical_key
                and re.fullmatch(r"[0-9a-f]{64}", str(evidence.get("sha256", "")))):
            after = blob.get_blob_properties()
            if after.etag != before.etag:
                raise RuntimeError("Artifact changed during checkpoint verification")
            print(f"artifact_hash_reused={logical_key} bytes={before.size}", flush=True)
            return {"key": logical_key, "sha256": evidence["sha256"], "bytes": before.size}
    digest = hashlib.sha256()
    size = 0
    for block in blob.download_blob(max_concurrency=4).chunks():
        size += len(block)
        digest.update(block)
    if before is not None:
        after = blob.get_blob_properties()
        if before.etag != after.etag or size != before.size:
            raise RuntimeError("Artifact changed during hash verification")
    return {"key": logical_key, "sha256": digest.hexdigest(), "bytes": size}


def inventory(quickwit, prefix: str, index_id: str) -> tuple[dict, list[dict], int]:
    container = quickwit.get_container_client("quickwit")
    candidates = []
    for item in container.list_blobs(name_starts_with=f"{prefix}/"):
        if item.name.endswith("/metastore.json") and index_id in item.name:
            candidates.append(item.name)
    if len(candidates) != 1:
        raise RuntimeError(f"expected one metastore for {index_id}, found {candidates}")
    metastore_key = candidates[0]
    metastore_blob = container.get_blob_client(metastore_key)
    metastore = json.loads(metastore_blob.download_blob().readall())
    if metastore.get("index", {}).get("index_config", {}).get("index_id") != index_id:
        raise RuntimeError("metastore index identity mismatch")
    split_entries = metastore.get("splits")
    if not isinstance(split_entries, list) or not split_entries:
        raise RuntimeError("index has no published splits")
    expected_names = {
        f"{item['split_id']}.split" for item in split_entries
        if isinstance(item, dict) and item.get("split_id")
    }
    if len(expected_names) != len(split_entries):
        raise RuntimeError("metastore contains duplicate or invalid splits")
    split_prefix = f"indexes/{index_id}/"
    names = {
        item.name.rsplit("/", 1)[-1]: item.name
        for item in container.list_blobs(name_starts_with=split_prefix)
        if item.name.endswith(".split")
    }
    if expected_names != set(names):
        raise RuntimeError("split objects do not match metastore")
    split_refs = [
        artifact(container.get_blob_client(names[name]), f"quickwit/{names[name]}")
        for name in sorted(expected_names)
    ]
    docs = sum(int(item["num_docs"]) for item in split_entries)
    return artifact(metastore_blob, f"quickwit/{metastore_key}"), split_refs, docs


def snapshot_metastore(quickwit, releases, metastore_ref: dict) -> dict:
    """Bind a release to immutable metastore bytes while the live index grows."""
    key = str(metastore_ref["key"])
    live = quickwit.get_blob_client("quickwit", key.split("/", 1)[1]).download_blob().readall()
    digest = hashlib.sha256(live).hexdigest()
    if digest != metastore_ref["sha256"] or len(live) != metastore_ref["bytes"]:
        raise RuntimeError("Quickwit metastore changed during release inventory")
    # AzureBlobReleaseStore retains the releases/ namespace inside the
    # releases container, unlike the other logical containers.
    snapshot_key = f"releases/index-metastores/{digest}.json"
    blob = releases.get_blob_client(snapshot_key)
    try:
        blob.upload_blob(live, overwrite=False, if_none_match="*")
    except ResourceExistsError:
        if blob.download_blob().readall() != live:
            raise RuntimeError("immutable metastore snapshot conflict")
    return {"key": snapshot_key, "sha256": digest, "bytes": len(live)}


def main() -> int:
    account = required("AZURE_STORAGE_ACCOUNT")
    qw_account = required("QW_AZURE_STORAGE_ACCOUNT")
    event_key = required("PIPELINE_EVENT_KEY")
    result_key = required("PIPELINE_RESULT_KEY")
    prefix = os.environ.get("INDEX_METASTORE_PREFIX", "metastores/production-v1").strip("/")
    credential = DefaultAzureCredential()
    corpus = BlobServiceClient(f"https://{account}.blob.core.windows.net", credential=credential)
    quickwit = BlobServiceClient(f"https://{qw_account}.blob.core.windows.net", credential=credential)
    releases = corpus.get_container_client("releases")
    global INTEGRITY_CACHE
    if os.environ.get("REUSE_VERIFIED_ARTIFACT_HASHES") == "true":
        INTEGRITY_CACHE = releases
    event = json.loads(releases.get_blob_client(event_key).download_blob().readall())
    payload = event.get("payload")
    if event.get("stage") != "index" or not isinstance(payload, dict):
        raise RuntimeError("index worker requires an index event")
    fingerprint = hashlib.sha256(canonical({"schema_version": 1, "stage": "index", "payload": payload})).hexdigest()
    event_id = f"index-{fingerprint[:32]}"
    if event.get("event_id") != event_id or event.get("event_fingerprint") != fingerprint:
        raise RuntimeError("event identity mismatch")
    index_id = str(payload.get("index_id") or "")
    if not re.fullmatch(r"[a-z0-9][a-z0-9_-]{2,100}", index_id):
        raise RuntimeError("invalid index ID")
    if index_id != os.environ.get("PATENT_INDEX_ID", "patents"):
        raise RuntimeError("index event must target the configured durable index")
    generation = str(payload.get("source_generation") or "")
    if not re.fullmatch(r"sha256:[0-9a-f]{64}", generation):
        raise RuntimeError("index event has invalid source_generation")
    active_generations = payload.get("active_generation_ids", [])
    if not isinstance(active_generations, list) or any(not re.fullmatch(r"sha256:[0-9a-f]{64}", item) for item in active_generations):
        raise RuntimeError("index event has invalid active generations")
    purge_ids = payload.get("purge_generation_ids", [])
    if not isinstance(purge_ids, list) or any(not re.fullmatch(r"sha256:[0-9a-f]{64}", item) for item in purge_ids):
        raise RuntimeError("index event has invalid purge generations")
    if generation in purge_ids or set(purge_ids) & set(active_generations):
        raise RuntimeError("refusing to purge a generation that this release still serves")
    expected_total = int(payload.get("expected_total_count", -1))
    work = Path("/work")
    work.mkdir(parents=True, exist_ok=True)
    projection_path = render_from_raw(corpus, payload, work)
    publication_count = sum(1 for line in projection_path.open("rb") if line.strip())
    if publication_count < 1:
        raise RuntimeError("projection is empty")

    config = Path("/quickwit/index.yaml").read_text()
    config = re.sub(r"^index_id: .*", f"index_id: {index_id}", config, flags=re.MULTILINE)
    config = re.sub(
        r"^index_uri: .*", f"index_uri: azure://quickwit/indexes/{index_id}",
        config, flags=re.MULTILINE,
    )
    (work / "index.yaml").write_text(config)
    process = subprocess.Popen(["quickwit", "run", "--config", "/quickwit/node.yaml"])
    try:
        wait_for_quickwit(process)
        try:
            quickwit_api(f"/api/v1/indexes/{index_id}")
            index_exists = True
        except error.HTTPError as exc:
            if exc.code != 404:
                raise
            index_exists = False
        if not index_exists:
            subprocess.run(
                ["quickwit", "index", "create", "--endpoint", ENDPOINT,
                 "--index-config", str(work / "index.yaml"), "-y"],
                check=True,
            )
        wait_for_searcher(index_id)
        ensure_incremental_merge_policy(index_id)
        def ingest_projection() -> None:
            # --force commits the archive before this short-lived job exits.
            # Quickwit 0.9 publishes roughly one split per 10 MiB ingest batch
            # under that flag. limit_merge compacts those segments toward
            # split_num_docs_target while this process is still up.
            # A weekly grant projection is about 0.5 GiB and 7–9k documents.
            # On 23 Sep 2026 that ingest took about 4–5 minutes, so a 180s
            # client timeout aborts a real week. The container job allows 4h.
            command = ["quickwit", "index", "ingest", "--endpoint", ENDPOINT,
                       "--index", index_id, "--input-path", str(projection_path),
                       "--batch-size-limit", "10MB",
                       "-f", "-y", "--timeout", "900s"]
            deadline = time.monotonic() + 120
            while True:
                result = subprocess.run(command, capture_output=True, text=True)
                if result.returncode == 0:
                    return
                # The V1 ingest source registers after the search API is ready.
                diagnostic = result.stdout + result.stderr
                if ("code=404 Not Found" in diagnostic
                        and f"index `{index_id}` not found" in diagnostic
                        and time.monotonic() < deadline):
                    time.sleep(5)
                    continue
                raise subprocess.CalledProcessError(
                    result.returncode, command, output=result.stdout, stderr=result.stderr
                )
        reconcile_generation(index_id, generation, publication_count, ingest_projection)
        purge_generations(index_id, purge_ids)
        compaction = wait_for_compaction(index_id)
        print(
            "split_compaction="
            f"published:{compaction['published']},small:{compaction['small_published']}",
            flush=True,
        )
        active_query = generation_filter(active_generations, payload.get("selected_publication_uids"))
        logical_docs = search_count(index_id, active_query)
        if logical_docs != expected_total:
            raise RuntimeError(f"index has {logical_docs} live documents, expected {expected_total}")
        metastore_ref, splits, _physical_docs = inventory(quickwit, prefix, index_id)
        metastore_ref = snapshot_metastore(quickwit, releases, metastore_ref)
        result = {
            "fulltext": {"index_id": index_id, "metastore": metastore_ref,
                         "splits": splits, "document_count": logical_docs},
            "publication_count": publication_count,
        }
        envelope = {"event_id": event_id, "event_fingerprint": fingerprint, "result": result}
        result_blob = releases.get_blob_client(result_key)
        try:
            result_blob.upload_blob(canonical(envelope), overwrite=False, if_none_match="*")
        except ResourceExistsError:
            if json.loads(result_blob.download_blob().readall()) != envelope:
                raise RuntimeError("immutable index result conflict")
        print(f"index_status=published index_id={index_id} documents={logical_docs}", flush=True)
        return 0
    finally:
        process.terminate()
        try:
            process.wait(timeout=20)
        except subprocess.TimeoutExpired:
            process.kill()


if __name__ == "__main__":
    raise SystemExit(main())
