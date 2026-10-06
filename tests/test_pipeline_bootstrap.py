"""First-release and retry coverage without any seeded corpus or migration proofs."""
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import pytest
from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError, HttpResponseError

from ingestion import acquisition, archive, pipeline as pipeline, worker
from ingestion.integrity import artifact_ref
from ingestion.orchestration import PipelineEvent, OrchestrationError
from ingestion.release import ReleasePublisher
from ingestion.store import LocalBlobStore
from ingestion.config import parse_config


from blob_fixture import FileService

def put(service, key, data):
    container, name = key.split('/', 1)
    if container == 'releases':
        name = key
    service.get_blob_client(container, name).upload_blob(data)
    return {'key': key, 'sha256': hashlib.sha256(data).hexdigest(), 'bytes': len(data)}


def setup_archive(tmp_path, monkeypatch):
    store = LocalBlobStore(tmp_path / 'blob')
    service = FileService(store)
    raw = put(service, 'raw/a.zip', b'synthetic archive')
    event = PipelineEvent('render', {'raw': raw, 'renderer_profile': acquisition.PROFILE,
                                   'source_generation': acquisition.source_generation(raw['sha256']),
                                   'source_identity': 'e' * 64})
    renders = []
    def render(command, **kwargs):
        renders.append(command)
        pack = b'canonical source frame'
        sha = hashlib.sha256(pack).hexdigest()
        row = {'publication_uid': 'US:1:B2', 'publication_number': '1', 'patent_number': '1',
               'application_number': '17123456', 'publication_date': '2026-01-02T00:00:00Z',
               'filing_date': '2025-01-01T00:00:00Z', 'doc_kind': 'grant', 'title': 'Fixture',
               'applicant': ['Acme'], 'assignee': ['Acme'], 'ipc': ['A01B'], 'us_class': ['123'],
               'source_generation': event.payload['source_generation'], 'source_pack': f'sha256:{sha}',
               'source_pack_key': f'source/v1/packs/{sha}.zst', 'source_revision': 'sha256:' + '3' * 64,
               'offset': 0, 'length': len(pack), 'uncompressed_length': 200,
               'source_fingerprint': 'blake2b-128:' + '4' * 32}
        Path(command[3]).write_text(json.dumps(row) + '\n')
        Path(command[5]).write_bytes(pack)
    monkeypatch.setattr(archive.subprocess, 'run', render)
    def indexer(payload, projection, work):
        assert projection.read_text().count('\n') == 1
        assert payload['publication_count'] == 1
        assert list((store.root / 'metadata').rglob('*.parquet'))
        split = put(service, 'quickwit/part.split', b'published split')
        meta = put(service, 'releases/index-metastores/fixture.json', json.dumps({
            'index': {'index_config': {'index_id': 'patents'}},
            'splits': [{'split_id': 'part', 'num_docs': 1}],
        }).encode())
        return {'publication_count': 1, 'fulltext': {'index_id': 'patents', 'document_count': 1,
                                                    'metastore': meta, 'splits': [split]}}
    monkeypatch.setattr(pipeline, 'index_with_serving_paused', lambda store, payload, compute:
                        pipeline.checkpoint(store, 'index', payload, compute))
    return store, service, event, renders, indexer


def test_empty_store_publishes_first_release_and_replay_does_not_render(tmp_path, monkeypatch):
    store, service, event, renders, indexer = setup_archive(tmp_path, monkeypatch)
    result = pipeline.run_pipeline(store, event, tmp_path / 'work', service=service, indexer=indexer, canary=lambda _: True)
    manifest = ReleasePublisher(store).verify_selector(store.read_json('releases/current.json'))
    assert manifest.publication_count == 1
    assert len(renders) == 1
    assert service.reads['raw/a.zip'] == 1
    assert not any('migration' in p.as_posix() for p in store.root.rglob('*'))
    repeated = pipeline.run_pipeline(store, event, tmp_path / 'replay', service=service, indexer=indexer, canary=lambda _: False)
    assert repeated['release_id'] == result['release_id']
    assert len(renders) == 1


def test_retry_reuses_metadata_after_interrupted_indexing(tmp_path, monkeypatch):
    from ingestion import projection
    store, service, event, renders, indexer = setup_archive(tmp_path, monkeypatch)
    def interrupted(*args):
        raise RuntimeError('interrupted index')
    with pytest.raises(RuntimeError, match='interrupted index'):
        pipeline.run_pipeline(store, event, tmp_path / 'first', service=service, indexer=interrupted, canary=lambda _: True)
    assert not store.exists('releases/current.json')
    monkeypatch.setattr(projection, 'project_metadata', lambda *args: pytest.fail('metadata must reuse its receipt'))
    pipeline.run_pipeline(store, event, tmp_path / 'retry', service=service, indexer=indexer, canary=lambda _: True)
    assert len(renders) == 2  # One render per attempt, shared by all stages.
    assert store.read_json('releases/current.json')['release_id']


def test_failed_canary_never_selects_first_release(tmp_path, monkeypatch):
    store, service, event, renders, indexer = setup_archive(tmp_path, monkeypatch)
    with pytest.raises(OrchestrationError, match='canary'):
        pipeline.run_pipeline(store, event, tmp_path / 'work', service=service, indexer=indexer, canary=lambda _: False)
    assert not store.exists('releases/current.json')
    assert len(renders) == 1


def test_hash_evidence_is_created_on_first_use_and_invalidated_by_new_bytes(tmp_path):
    service = FileService(LocalBlobStore(tmp_path))
    blob = service.get_blob_client('quickwit', 'part.split')
    blob.upload_blob(b'first version')
    cache = service.get_container_client('releases')
    first = artifact_ref(blob, 'quickwit/part.split', cache)
    assert artifact_ref(blob, 'quickwit/part.split', cache) == first
    assert service.reads['quickwit/part.split'] == 1
    service.store._safe('quickwit/part.split').write_bytes(b'new version')
    second = artifact_ref(blob, 'quickwit/part.split', cache)
    assert second['sha256'] != first['sha256']
    assert service.reads['quickwit/part.split'] == 2


@pytest.mark.parametrize('case', ['unchanged', 'modified', 'too_new', 'wrong_size'])
def test_published_immutable_versions_are_adopted_without_corpus_reads(tmp_path, monkeypatch, case):
    from datetime import datetime, timedelta, timezone
    service = FileService(LocalBlobStore(tmp_path))
    blob = service.get_blob_client('quickwit', 'old.split')
    blob.upload_blob(b'verified split')
    created = datetime(2026, 1, 1, tzinfo=timezone.utc)
    published = created + timedelta(days=1)
    props = SimpleNamespace(size=14, etag='existing', creation_time=created, last_modified=created)
    if case == 'modified':
        props.last_modified += timedelta(seconds=1)
    if case == 'too_new':
        props.creation_time = props.last_modified = published
    if case == 'wrong_size':
        props.size += 1
    monkeypatch.setattr(blob, 'get_blob_properties', lambda: props)
    cache = service.get_container_client('releases')
    expected = {'key': 'quickwit/old.split', 'bytes': 14,
                'sha256': hashlib.sha256(b'verified split').hexdigest()}
    cache._patpub_legacy_versions = {}
    cache._patpub_published_before = published
    cache._patpub_protected_artifacts = {expected['key']: expected}
    # Azure listings omit ETag quotes while Get Blob Properties includes them.
    cache._patpub_properties = {expected['key']: SimpleNamespace(**vars(props))}
    props.etag = '"existing"'
    if case == 'unchanged':
        assert artifact_ref(blob, expected['key'], cache) == expected
        assert artifact_ref(blob, expected['key'], cache) == expected
    else:
        with pytest.raises(RuntimeError, match='refusing rehash'):
            artifact_ref(blob, expected['key'], cache)
    assert service.reads.get(expected['key'], 0) == 0


def test_pending_events_drain_past_two_archive_default(tmp_path):
    store = LocalBlobStore(tmp_path / 'blob')
    service = FileService(store)
    events = [PipelineEvent('render', {'raw': {'key': f'raw/v1/PTGRXML/{number}.zip'},
                                      'category': 'xml_grants', 'publication_year': 2002})
              for number in range(3)]
    for event in events:
        store.write_json_if_absent(f'events/{event.event_id}.json', event.as_dict())
        store.write_json_if_absent(f'fetch/v1/PTGRXML/{event.event_id}.json', {'event_key': f'events/{event.event_id}.json'})
    calls = []
    def run(*args, **kwargs):
        calls.append(args[1].event_id)
        return {'release_id': 'first'}
    work = tmp_path / 'work'
    work.mkdir()
    config = parse_config({})
    assert config.archives_per_run is None
    assert worker.drain_pending(service, store, None, work, pipeline=run,
                                limit=config.archives_per_run, config=config) == 3
    assert worker.drain_pending(service, store, None, work, pipeline=run,
                                limit=config.archives_per_run, config=config) == 0
    assert set(calls) == {event.event_id for event in events}


def test_published_archive_inventory_preserves_old_profiles_and_is_cached(tmp_path):
    from ingestion.models import canonical_json
    service = FileService(LocalBlobStore(tmp_path))
    generation = {'generation_id': 'sha256:' + 'a' * 64,
                  'renderer_profile': 'aps-greenbook-markdown-v1:zstd-9-checksum',
                  'raw': {'key': 'raw/v1/backfill/PTGRAPS/pftaps19760120_wk03.zip',
                          'bytes': 10, 'sha256': 'b' * 64}}
    artifact = put(service, 'source/v1/generations/old.json', canonical_json(generation))
    manifest = {'source': {'generation_manifests': [{'generation_id': generation['generation_id'],
                                                   'artifact': artifact}]}}
    data = canonical_json(manifest)
    put(service, 'releases/old.json', data)
    service.store.write_json_if_absent('releases/current.json', {
        'manifest_key': 'releases/old.json', 'manifest_sha256': hashlib.sha256(data).hexdigest()})
    inventory = acquisition.published_archives(service)
    assert inventory['PTGRAPS/pftaps19760120_wk03.zip']['bytes'] == 10
    assert acquisition.published_archives(service) == inventory
    assert service.reads['source/v1/generations/old.json'] == 1
    assert not any(key.startswith('raw/') for key in service.reads)


@pytest.mark.parametrize('case', ['unchanged', 'new_revision', 'changed_size', 'recent_update', 'no_date'])
def test_existing_published_archives_skip_only_unchanged_catalog_entries(case):
    item = {'fileName': 'week.zip', 'fileSize': 10, 'fileLastModifiedDateTime': '2020-01-01 12:00:00'}
    existing = {'PTGRAPS/week.zip': {'bytes': 10, 'published_at': '2026-01-01T12:00:00+00:00'}}
    if case == 'new_revision': item['fileName'] = 'week_r2.zip'
    if case == 'changed_size': item['fileSize'] = 11
    if case == 'recent_update': item['fileLastModifiedDateTime'] = '2026-01-02 12:00:00'
    if case == 'no_date': del item['fileLastModifiedDateTime']
    assert acquisition.archive_already_published(item, 'PTGRAPS', existing) == (case == 'unchanged')


def test_writer_lease_contention_skips_run_and_exceptions_release_it():
    class Lock:
        def upload_blob(self, *args, **kwargs): pass
        def acquire_lease(self, **kwargs):
            raise HttpResponseError('already leased', response=SimpleNamespace(status_code=409, reason='Conflict', headers={}))
    with worker.writer_lease(Lock(), on_lost=lambda: pytest.fail('not held')) as held:
        assert not held
    released = []
    class Available(Lock):
        def acquire_lease(self, **kwargs):
            return SimpleNamespace(release=lambda: released.append(True))
    with pytest.raises(RuntimeError):
        with worker.writer_lease(Available(), on_lost=lambda: pytest.fail('must not expire')) as held:
            assert held
            raise RuntimeError('failed archive')
    assert released == [True]


@pytest.mark.parametrize('code', ['LeaseIdMissing', 'ConditionNotMet'])
def test_writer_lease_handles_azure_lock_creation_conflict(code):
    class Lock:
        def upload_blob(self, *args, **kwargs):
            error = HttpResponseError('lock conflict', response=SimpleNamespace(
                status_code=412, reason='Precondition Failed', headers={}))
            error.error_code = code
            raise error
        def acquire_lease(self, **kwargs):
            pytest.fail('creation conflict must not acquire a lease')
    if code == 'LeaseIdMissing':
        with worker.writer_lease(Lock(), on_lost=lambda: pytest.fail('not held')) as held:
            assert not held
    else:
        with pytest.raises(HttpResponseError):
            with worker.writer_lease(Lock(), on_lost=lambda: None):
                pytest.fail('unrelated precondition failures must propagate')


def test_backfill_keeps_latest_weekly_correction():
    manifest = {'bulkDataProductBag': [{'productFileBag': {'fileDataBag': [
        {'fileName': name, 'fileDate': '2026-01-02'} for name in ('ipg260102.zip', 'ipg260102_r2.zip', 'ipg260102_r10.zip')
    ]}}]}
    assert [item['fileName'] for item in acquisition.entries(manifest)] == ['ipg260102_r10.zip']


def test_checkpoint_rejects_tampered_result_before_computing(tmp_path):
    store = LocalBlobStore(tmp_path)
    event = PipelineEvent('metadata', {'snapshot_id': 'one'})
    store.write_json_if_absent(f'events/results/{event.event_id}.json', {'event_id': 'other'})
    with pytest.raises(RuntimeError, match='identity mismatch'):
        pipeline.checkpoint(store, 'metadata', event.payload, lambda: pytest.fail('must not compute'))


def test_lease_watchdog_fences_a_stalled_renewal():
    import threading
    lost = threading.Event()
    unblock = threading.Event()
    class Lock:
        def upload_blob(self, *args, **kwargs): pass
        def acquire_lease(self, **kwargs):
            return SimpleNamespace(renew=lambda **kwargs: unblock.wait(1), release=lambda: None)
    try:
        with worker.writer_lease(Lock(), on_lost=lost.set, renew_interval=0.01,
                                 safety_window=0.03, watch_interval=0.005) as held:
            assert held
            assert lost.wait(0.5), 'deadline must fence the writer even while renewal is blocked'
            unblock.set()
    finally:
        unblock.set()


def test_writer_failure_leaves_serving_stopped_and_success_resumes_it(tmp_path, monkeypatch):
    actions = []
    monkeypatch.setattr(pipeline, '_serving_arm', lambda action=None: actions.append(action) or 'Running')
    monkeypatch.setattr(pipeline, '_wait_serving_status', lambda status: actions.append(status))
    monkeypatch.setattr(pipeline, '_wait_serving_health', lambda: actions.append('healthy'))
    store = LocalBlobStore(tmp_path)
    def fail():
        actions.append('writer_failed')
        raise RuntimeError('index failed')
    with pytest.raises(RuntimeError, match='index failed'):
        pipeline.index_with_serving_paused(store, {'index_id': 'patents'}, fail)
    assert actions == [None, 'stop', 'Stopped', 'writer_failed']
    actions.clear()
    def succeed():
        actions.append('writer_exited')
        return {'publication_count': 1}
    pipeline.index_with_serving_paused(store, {'index_id': 'patents'}, succeed)
    assert actions == [None, 'stop', 'Stopped', 'writer_exited', 'start', 'Running', 'healthy']


def test_existing_verified_receipt_is_adopted_without_reading_the_artifact(tmp_path):
    from datetime import datetime, timezone
    service = FileService(LocalBlobStore(tmp_path))
    blob = service.get_blob_client('source', 'old.zst')
    blob.upload_blob(b'old source pack')
    properties = blob.get_blob_properties()
    ref = {'key': 'source/old.zst', 'sha256': hashlib.sha256(b'old source pack').hexdigest(), 'bytes': properties.size}
    capture = {'release_id': 'old', 'manifest_sha256': 'a' * 64,
               'capture_finished_utc': '2026-01-01T00:00:00+00:00',
               'versions': {ref['key']: dict(ref, etag=properties.etag)}}
    for key, value in [
        ('migration/v1/artifact-version-capture.json', capture),
        ('migration/v1/full-release-graph-validation.json', {'release_id': 'old', 'status': 'graph-validated-pending-search-canary'}),
        ('migration/v1/full-release-candidate.json', {'manifest': {'sha256': 'a' * 64}}),
    ]:
        service.store.write_json_if_absent(key, value)
    original = service.get_blob_client
    def get(name):
        result = original('releases', name)
        if name.endswith('full-release-graph-validation.json'):
            result.get_blob_properties = lambda: SimpleNamespace(last_modified=datetime(2026, 1, 2, tzinfo=timezone.utc))
        return result
    cache = SimpleNamespace(get_blob_client=get)
    assert artifact_ref(blob, ref['key'], cache) == ref
    assert service.reads.get('source/old.zst', 0) == 0
    assert service.store.exists('integrity/v1/adopted-versions.json')
    # Future runs work without any migration records after the automatic adoption.
    for path in service.store.root.joinpath('migration').rglob('*.json'):
        path.unlink()
    fresh_cache = service.get_container_client('releases')
    assert artifact_ref(blob, ref['key'], fresh_cache) == ref
    assert service.reads.get('source/old.zst', 0) == 0


def test_missing_historical_hash_evidence_never_triggers_a_rehash(tmp_path):
    service = FileService(LocalBlobStore(tmp_path))
    blob = service.get_blob_client('source', 'old.zst')
    blob.upload_blob(b'old source pack')
    cache = service.get_container_client('releases')
    cache._patpub_protected_artifacts = {'source/old.zst': {}}
    with pytest.raises(RuntimeError, match='refusing rehash'):
        artifact_ref(blob, 'source/old.zst', cache)
    assert service.reads.get('source/old.zst', 0) == 0
