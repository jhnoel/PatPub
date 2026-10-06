"""One scheduled worker: discover, acquire, render, project, index, publish."""
from __future__ import annotations

from contextlib import contextmanager
import json
import os
from pathlib import Path
import signal
import tempfile
import threading
import time

from azure.core.exceptions import ResourceExistsError, HttpResponseError
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient

from .acquisition import fetch_archives
from .config import load_config
from .orchestration import PipelineEvent
from .pipeline import required, run_pipeline
from .release import AzureBlobReleaseStore
from fts.production.indexer.indexer import index_projection


@contextmanager
def writer_lease(blob, *, on_lost, renew_interval=15, safety_window=50, watch_interval=1):
    """An expiring lease protects the whole run, including the Quickwit writer.

    Renewal failure stops this worker and its child processes before another
    worker can acquire the lease. Process death leaves no permanent lock.
    """
    try:
        blob.upload_blob(b'patpub ingestion writer\n', overwrite=False)
    except ResourceExistsError:
        pass
    except HttpResponseError as exc:
        # Azure checks the active lease before the create-only condition, so
        # creating an existing leased lock can return 412 instead of 409.
        if exc.status_code == 412 and getattr(exc, 'error_code', None) == 'LeaseIdMissing':
            yield False
            return
        raise
    try:
        lease = blob.acquire_lease(lease_duration=60)
    except HttpResponseError as exc:
        if exc.status_code == 409:
            yield False
            return
        raise
    stopped = threading.Event()
    deadline = time.monotonic() + safety_window

    def renew():
        nonlocal deadline
        while not stopped.wait(renew_interval):
            try:
                if time.monotonic() >= deadline:
                    raise RuntimeError('writer lease renewal deadline elapsed')
                lease.renew(timeout=5, connection_timeout=5, read_timeout=5, retry_total=0)
                if time.monotonic() >= deadline:
                    raise RuntimeError('writer lease renewal completed too late')
                deadline = time.monotonic() + safety_window
            except Exception:
                on_lost()
                return

    def watchdog():
        # A renewal request can stall inside credential acquisition or transport.
        # This independent deadline still fences the writer before lease expiry.
        while not stopped.wait(watch_interval):
            if time.monotonic() >= deadline:
                on_lost()
                return

    guard = threading.Thread(target=watchdog, name='writer-lease-deadline', daemon=True)
    guard.start()
    thread = threading.Thread(target=renew, name='writer-lease', daemon=True)
    thread.start()
    try:
        yield True
    finally:
        stopped.set()
        guard.join(timeout=2)
        thread.join(timeout=10)
        # Do not release a lease with an outstanding renewal request.
        if thread.is_alive():
            on_lost()
        else:
            lease.release()


def drain_pending(service, store, indexer, work_root, *, pipeline=run_pipeline, limit=None, config=None):
    """Recover events persisted before a crash, independently of fetch markers."""
    processed = 0
    container = service.get_container_client('releases')
    completed = {item.name.rsplit('/', 1)[-1].removesuffix('.json')
                 for item in container.list_blobs(name_starts_with='ingestion/completed/')}
    selected = None
    published = set()
    if store.exists('releases/current.json'):
        selected = json.loads(store.read('releases/current.json'))
        manifest = json.loads(store.read(selected['manifest_key']))
        published = set(manifest['source_generations'])
    pending = []
    for item in container.list_blobs(name_starts_with='events/render-'):
        if not item.name.endswith('.json') or item.name.rsplit('/', 1)[-1].removesuffix('.json') in completed:
            continue
        event = PipelineEvent.from_dict(json.loads(store.read(item.name)))
        if event.payload.get('source_generation') in published:
            # Upgrades can recover old completed events without spending the new
            # backfill budget or rerendering an already published generation.
            store.write_json_if_absent(f'ingestion/completed/{event.event_id}.json',
                                       {'event_id': event.event_id, 'result': {'release_id': selected['release_id'], 'selector': selected}})
            continue
        if config is None or config.accepts_event(event):
            pending.append(event)
    pending.sort(key=lambda event: (event.payload.get('publication_date') or '', event.event_id))
    for event in pending:
        receipt = f'ingestion/completed/{event.event_id}.json'
        if store.exists(receipt):
            continue
        with tempfile.TemporaryDirectory(prefix='archive-', dir=work_root) as folder:
            result = pipeline(store, event, Path(folder), service=service, indexer=indexer)
        if not store.write_json_if_absent(receipt, {'event_id': event.event_id, 'result': result}):
            raise RuntimeError('archive completion raced with another worker')
        processed += 1
        if limit is not None and processed >= limit:
            break
    return processed


def fence_writer():
    """Stop children and exit even when running as a container's PID 1."""
    try:
        os.killpg(os.getpgrp(), signal.SIGKILL)
    finally:
        os._exit(1)


def main() -> int:
    # Own a process group so lease loss also kills the synchronous Quickwit child.
    if os.getpid() != os.getpgrp():
        os.setsid()
    credential = DefaultAzureCredential()
    account = required('AZURE_STORAGE_ACCOUNT')
    qw_account = required('QW_AZURE_STORAGE_ACCOUNT')
    service = BlobServiceClient(f'https://{account}.blob.core.windows.net', credential=credential)
    quickwit = BlobServiceClient(f'https://{qw_account}.blob.core.windows.net', credential=credential)
    store = AzureBlobReleaseStore(f'https://{account}.blob.core.windows.net', 'releases')
    work = Path(os.environ.get('WORK_DIR', '/work'))
    work.mkdir(parents=True, exist_ok=True)
    config = load_config()
    limit = config.archives_per_run
    lock = service.get_blob_client('releases', 'ingestion/writer.lock')
    with writer_lease(lock, on_lost=fence_writer) as acquired:
        if not acquired:
            print('ingestion_status=already_running', flush=True)
            return 0
        def indexer(payload, projection, folder):
            return index_projection(service, quickwit, payload, projection, folder,
                                    integrity_cache=store._containers["releases"])
        processed = drain_pending(service, store, indexer, work, limit=limit, config=config)
        fetched = 0
        if limit is None or processed < limit:
            # Match acquisition to the remaining processing budget.
            remaining = None if limit is None else limit - processed
            fetched = fetch_archives(service, config, limit=remaining)
            processed += drain_pending(service, store, indexer, work, limit=remaining, config=config)
        print(json.dumps({'ingestion_status': 'complete', 'fetched_archives': fetched,
                          'processed_archives': processed}), flush=True)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
