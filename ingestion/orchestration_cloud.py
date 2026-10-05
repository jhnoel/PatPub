"""Queue-driven Azure pipeline controller for immutable raw archive events."""
from __future__ import annotations

import hashlib
import json
import os
import time
from urllib import request as urlrequest
from urllib import error as urlerror

from .orchestration import DownstreamOrchestrator, PipelineEvent, OrchestrationError
from .release import AzureBlobReleaseStore, ReleaseError, _iter_artifacts
from .search_query import generation_filter
from fts.production.serving.api.source_router import Router


def required(name: str) -> str:
    value = os.environ.get(name, '').strip()
    if not value:
        raise RuntimeError(f'{name} is required')
    return value


def artifact(store, key: str) -> dict:
    digest = hashlib.sha256()
    size = 0
    for chunk in store.iter_chunks(key):
        digest.update(chunk)
        size += len(chunk)
    return {'key': key, 'sha256': digest.hexdigest(), 'bytes': size}


def candidate_canary(manifest: dict) -> bool:
    from azure.identity import DefaultAzureCredential
    template = required('CANDIDATE_CANARY_URL')
    query = json.loads(required('CANDIDATE_CANARY_QUERY'))
    expected_uid = os.environ.get('CANDIDATE_PUBLICATION_UID', '').strip()
    headers = {'content-type': 'application/json'}
    token = os.environ.get('CANDIDATE_CANARY_TOKEN', '').strip()
    if token:
        headers['authorization'] = f'Bearer {token}'
    candidate = manifest['fulltext']
    active_generations = [item for item in manifest['source_generations']
                          if item not in set(manifest.get('source', {}).get('superseded_generation_ids', []))]
    if not active_generations:
        return False
    allowed = generation_filter(active_generations, manifest.get("source", {}).get("selected_publication_uids"))
    base_query = query.get('query') or '*'
    count_query = {'query': f'({allowed})', 'max_hits': 0}
    count_request = urlrequest.Request(template.format(index_id=manifest['fulltext']['index_id']),
                                       data=json.dumps(count_query).encode(), headers=headers, method='POST')
    with urlrequest.urlopen(count_request, timeout=45) as response:
        if int(json.loads(response.read()).get('num_hits', -1)) != int(manifest['publication_count']):
            return False
    newest_generation = active_generations[-1]
    query['query'] = f"({base_query}) AND source_generation:\"{newest_generation}\""
    req = urlrequest.Request(template.format(index_id=candidate['index_id']),
                             data=json.dumps(query).encode(), headers=headers, method='POST')
    with urlrequest.urlopen(req, timeout=45) as response:
        hits = json.loads(response.read()).get('hits', [])
    hit = next((h for h in hits if isinstance(h, dict) and
                (not expected_uid or h.get('publication_uid') == expected_uid)), None)
    if hit is None:
        return False
    uid = str(hit.get('publication_uid') or '')
    if not uid:
        return False
    generation = str(hit.get('source_generation') or '')
    pair = (str(hit.get('source_pack') or ''), str(hit.get('source_pack_key') or ''))
    store = AzureBlobReleaseStore(f"https://{required('AZURE_STORAGE_ACCOUNT')}.blob.core.windows.net", 'releases')
    refs = manifest.get('source', {}).get('generation_manifests', [])
    valid = False
    for ref in refs:
        if ref.get('generation_id') != generation:
            continue
        source = json.loads(store.read(ref['artifact']['key']))
        valid = pair in {(f"sha256:{p['sha256']}", p['key']) for p in source.get('source_packs', [])}
    if not valid:
        return False
    try:
        offset, length, output_length = (int(hit[k]) for k in ('offset', 'length', 'uncompressed_length'))
    except (KeyError, ValueError, TypeError):
        return False
    if offset < 0 or length <= 0 or length > 64 * 1024 * 1024 or output_length <= 0:
        return False
    if not pair[1].startswith('source/'):
        return False
    from azure.storage.blob import BlobServiceClient
    service = BlobServiceClient(f"https://{required('AZURE_STORAGE_ACCOUNT')}.blob.core.windows.net",
                                credential=DefaultAzureCredential())
    blob = service.get_blob_client('source', pair[1].split('/', 1)[1])
    compressed = Router._range_bytes(blob.download_blob(offset=offset, length=length), offset, length)
    if hashlib.blake2b(compressed, digest_size=64).hexdigest()[:32] != str(hit.get('source_fingerprint', '')).removeprefix('blake2b-128:'):
        return False
    import zstandard
    markdown = zstandard.ZstdDecompressor().decompress(compressed, max_output_size=output_length)
    revision = str(hit.get('source_revision') or '')
    return revision.startswith('sha256:') and hashlib.sha256(markdown).hexdigest() == revision[7:] and markdown.startswith(f'<!-- publication_uid: {uid} -->\n'.encode())


def start_job(resource_id: str, event_key: str, result_key: str) -> None:
    from azure.identity import DefaultAzureCredential
    credential = DefaultAzureCredential()
    token = credential.get_token('https://management.azure.com/.default').token
    api = '2024-03-01'
    base = f'https://management.azure.com{resource_id}'
    headers = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}
    with urlrequest.urlopen(urlrequest.Request(f'{base}?api-version={api}', headers=headers), timeout=30) as response:
        job = json.loads(response.read())
    containers = job['properties']['template']['containers']
    if len(containers) != 1:
        raise RuntimeError('job must have exactly one container')
    container = dict(containers[0])
    env = [item for item in container.get('env', []) if item.get('name') not in {'PIPELINE_EVENT_KEY', 'PIPELINE_RESULT_KEY'}]
    env.extend([{'name': 'PIPELINE_EVENT_KEY', 'value': event_key},
                {'name': 'PIPELINE_RESULT_KEY', 'value': result_key}])
    container['env'] = env
    body = json.dumps({'containers': [container]}).encode()
    with urlrequest.urlopen(urlrequest.Request(f'{base}/start?api-version={api}', data=body, headers=headers, method='POST'), timeout=45) as response:
        response.read()


def _serving_arm(action: str | None = None) -> str:
    """Read or change the serving app state through Azure Resource Manager."""
    from azure.identity import DefaultAzureCredential

    resource_id = required('SERVING_APP_RESOURCE_ID')
    token = DefaultAzureCredential().get_token('https://management.azure.com/.default').token
    url = f'https://management.azure.com{resource_id}'
    if action:
        url += f'/{action}'
    url += '?api-version=2024-03-01'
    request = urlrequest.Request(
        url,
        headers={'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'},
        data=b'{}' if action else None,
        method='POST' if action else 'GET',
    )
    deadline = time.monotonic() + 600
    while True:
        try:
            with urlrequest.urlopen(request, timeout=45) as response:
                if action:
                    response.read()
                    return ''
                return str(json.loads(response.read()).get('properties', {}).get('runningStatus') or '')
        except urlerror.HTTPError as exc:
            if not action or exc.code != 409 or time.monotonic() >= deadline:
                raise
            time.sleep(5)


def _wait_indexer_idle() -> None:
    from azure.identity import DefaultAzureCredential
    resource = required('INDEX_JOB_RESOURCE_ID')
    token = DefaultAzureCredential().get_token('https://management.azure.com/.default').token
    req = urlrequest.Request('https://management.azure.com'+resource+'/executions?api-version=2024-03-01', headers={'Authorization':'Bearer '+token})
    deadline = time.monotonic() + int(os.environ.get('WORKER_RESULT_TIMEOUT_SECONDS', '14400'))
    while time.monotonic() < deadline:
        with urlrequest.urlopen(req, timeout=45) as response:
            executions = json.load(response)['value']
        if not any(x.get('properties', {}).get('status') == 'Running' for x in executions):
            return
        time.sleep(5)
    raise RuntimeError('Index writer has not exited; serving stays stopped')


def _wait_serving_status(expected: str) -> None:
    deadline = time.monotonic() + int(os.environ.get('SERVING_TRANSITION_TIMEOUT_SECONDS', '600'))
    while time.monotonic() < deadline:
        if _serving_arm() == expected:
            return
        time.sleep(5)
    raise RuntimeError(f'serving Container App did not reach {expected}')


def _wait_serving_health() -> None:
    url = required('SERVING_HEALTH_URL')
    deadline = time.monotonic() + int(os.environ.get('SERVING_HEALTH_TIMEOUT_SECONDS', '600'))
    while time.monotonic() < deadline:
        try:
            with urlrequest.urlopen(url, timeout=10) as response:
                if response.status == 200:
                    return
        except Exception:
            pass
        time.sleep(5)
    raise RuntimeError('serving Quickwit did not become healthy after restart')


def index_with_serving_paused(store, payload: dict) -> dict:
    """Give the file-backed Quickwit metastore to one process at a time."""
    index_event = PipelineEvent('index', payload)
    if store.exists(f'events/results/{index_event.event_id}.json'):
        _wait_indexer_idle()
        result = run_worker(store, 'index', payload)
        if _serving_arm() != 'Running':
            _serving_arm('start')
            _wait_serving_status('Running')
        _wait_serving_health()
        return result
    status = _serving_arm()
    if status not in {'Running', 'Stopped'}:
        raise RuntimeError(f'serving Container App is in unexpected state: {status}')
    if status == 'Running':
        _serving_arm('stop')
        _wait_serving_status('Stopped')
    # Keep serving stopped after an indexing failure. Restarting could expose
    # a partially appended generation under the previous release selector.
    result = run_worker(store, 'index', payload)
    _wait_indexer_idle()
    _serving_arm('start')
    _wait_serving_status('Running')
    _wait_serving_health()
    return result


def run_worker(store, stage: str, payload: dict) -> dict:
    event = PipelineEvent(stage, payload)
    event_key = f'events/{event.event_id}.json'
    result_key = f'events/results/{event.event_id}.json'
    if not store.write_json_if_absent(event_key, event.as_dict()):
        if json.loads(store.read(event_key)) != event.as_dict():
            raise RuntimeError('immutable event conflict')
    if not store.exists(result_key):
        start_job(required({'render': 'RENDER_JOB_RESOURCE_ID', 'metadata': 'METADATA_JOB_RESOURCE_ID', 'index': 'INDEX_JOB_RESOURCE_ID'}[stage]), event_key, result_key)
    deadline = time.monotonic() + int(os.environ.get('WORKER_RESULT_TIMEOUT_SECONDS', '14400'))
    while time.monotonic() < deadline:
        if store.exists(result_key):
            envelope = json.loads(store.read(result_key))
            if envelope.get('event_id') != event.event_id or envelope.get('event_fingerprint') != event.fingerprint:
                raise RuntimeError('worker result identity mismatch')
            print('worker_result_ready stage=' + stage + ' event=' + event.event_id, flush=True)
            return envelope['result']
        time.sleep(10)
    raise RuntimeError(f'{stage} worker did not publish its result in time')


def verify_correction_coverage(store, old_snapshot: dict, new_snapshot: dict) -> None:
    """Never silently drop distinct publications when replacing an archive."""
    import io
    import pyarrow.parquet as pq
    def inventory(snapshot):
        ids = set()
        for part in snapshot['parquet']['parts']:
            if part.get('projection') != 'point':
                continue
            data = store.read(part['key'])
            if len(data) != part['bytes'] or hashlib.sha256(data).hexdigest() != part['sha256']:
                raise RuntimeError('Correction metadata part integrity mismatch')
            ids.update(pq.read_table(io.BytesIO(data), columns=['publication_uid'])['publication_uid'].to_pylist())
        if len(ids) != snapshot['publication_count']:
            raise RuntimeError('Correction publication inventory count mismatch')
        return ids
    missing = inventory(old_snapshot) - inventory(new_snapshot)
    if missing:
        proof = json.loads(store.read('migration/v1/correction-withdrawal-exact.json'))
        withdrawn = {p['publication_uid'] for p in proof['publications'] if p['candidate_found']}
        unexplained = missing - withdrawn
        if unexplained:
            raise RuntimeError('Correction requires explicit preservation before indexing: ' + ', '.join(sorted(unexplained)))


def run_pipeline(store, event: PipelineEvent) -> dict:
    """Append one archive to the durable index and atomically publish it."""
    if event.stage != 'render':
        raise RuntimeError('queue entry must be a render event')
    prior = None
    predecessor_sha = None
    if store.exists('releases/current.json'):
        selector_bytes = store.read('releases/current.json')
        predecessor_sha = hashlib.sha256(selector_bytes).hexdigest()
        active = json.loads(selector_bytes)
        prior = json.loads(store.read(active['manifest_key']))
        if event.payload['source_generation'] in prior['source_generations']:
            return {'release_id': prior['release_id'], 'selector': active}
        if prior['fulltext'].get('index_id') != os.environ.get('PATENT_INDEX_ID', 'patents'):
            raise OrchestrationError('configured index does not match the active release')
        prefetch = getattr(store, 'prefetch_json', None)
        if prefetch:
            prefetch(a['key'] for _, a in _iter_artifacts(prior, 'release'))
        print('historical_controls_prefetched', flush=True)

    raw = event.payload['raw']
    render = run_worker(store, 'render', dict(event.payload))
    generation = DownstreamOrchestrator(store).run_generation(PipelineEvent('generation', {
        'raw': raw, 'generation_id': event.payload['source_generation'],
        'source_identity': event.payload['source_identity'],
        'renderer_profile': event.payload['renderer_profile'],
        'source_packs': render['source_packs'], 'publication_count': render['publication_count'],
    }))
    # Older completed render events may still record a Blob key. Only the
    # digest is needed now that consumers recreate NDJSON from the raw ZIP.
    projection = {
        'sha256': render['projection']['sha256'],
        'bytes': render['projection']['bytes'],
    }
    metadata = run_worker(store, 'metadata', {
        'raw': raw,
        'source_generation': event.payload['source_generation'],
        'renderer_profile': event.payload['renderer_profile'],
        'source_packs': render['source_packs'],
        'projection': projection,
        'snapshot_id': projection['sha256'],
    })

    source_generation = generation['generation_id']
    generation_manifests = [{
        'generation_id': source_generation, 'artifact': generation['manifest'],
    }]
    snapshots = [dict(metadata['metadata']) | {
        'generation_id': source_generation,
        'publication_count': render['publication_count'],
    }]
    superseded = []
    selections = dict(prior["source"].get("selected_publication_uids", {})) if prior else {}
    prior_count = 0
    if prior:
        old_refs = prior['source'].get('generation_manifests', [])
        generation_manifests = old_refs + generation_manifests
        snapshots = prior['metadata'].get('snapshots', [prior['metadata']]) + snapshots
        superseded = list(prior['source'].get('superseded_generation_ids', []))
        source_identity = event.payload['source_identity']
        for ref in old_refs:
            old_manifest = json.loads(store.read(ref['artifact']['key']))
            if (old_manifest.get('source_identity') == source_identity and ref['generation_id'] not in selections
                    and ref['generation_id'] not in superseded):
                old_snapshot = next(item for item in snapshots if item['generation_id'] == ref['generation_id'])
                verify_correction_coverage(store, old_snapshot, snapshots[-1])
                superseded.append(ref['generation_id'])
        prior_count = int(prior['publication_count'])
    superseded = list(dict.fromkeys(superseded))
    replaced_count = sum(
        int(json.loads(store.read(ref['artifact']['key']))['publication_count'])
        for ref in generation_manifests
        if ref['generation_id'] in superseded
        and (not prior or ref['generation_id'] not in set(prior['source'].get('superseded_generation_ids', [])))
    )
    selected_count = prior_count - replaced_count + render['publication_count']

    index_payload = {
        'raw': raw,
        'renderer_profile': event.payload['renderer_profile'],
        'source_packs': render['source_packs'],
        'projection': projection,
        'index_id': os.environ.get('PATENT_INDEX_ID', 'patents'),
        'source_generation': source_generation,
        'active_generation_ids': [
            ref['generation_id'] for ref in generation_manifests
            if ref['generation_id'] not in superseded
        ],
        'publication_count': render['publication_count'],
        'expected_total_count': selected_count,
        # Logical supersession preserves large historical splits; physical GC is separate.
        'purge_generation_ids': [],
        'selected_publication_uids': selections,
    }
    index = index_with_serving_paused(store, index_payload)
    metadata_count = metadata.get('publication_count', metadata.get('metadata', {}).get('publication_count'))
    if metadata_count != render['publication_count'] or index['publication_count'] != render['publication_count']:
        raise RuntimeError('stage publication counts differ')

    release = PipelineEvent('release', {
        'generation_manifests': generation_manifests,
        'selected_publication_uids': selections,
        'superseded_generation_ids': superseded,
        'publication_count': selected_count,
        'canonical_text_profile': event.payload['renderer_profile'],
        'expected_selector_sha256': predecessor_sha,
        'fulltext': index['fulltext'],
        'metadata': {'snapshots': [item for item in snapshots if item['generation_id'] not in superseded]},
    })
    return DownstreamOrchestrator(store).run_release(release, candidate_canary)

def main() -> int:
    diagnostic_seconds = int(os.environ.get('DIAGNOSTIC_TRACEBACK_SECONDS', '0'))
    if diagnostic_seconds:
        import faulthandler
        faulthandler.dump_traceback_later(diagnostic_seconds, repeat=True)
    from azure.identity import DefaultAzureCredential
    from azure.storage.queue import QueueClient
    account = required('AZURE_STORAGE_ACCOUNT')
    store = AzureBlobReleaseStore(f'https://{account}.blob.core.windows.net', 'releases')
    queue = QueueClient(account_url=f'https://{account}.queue.core.windows.net',
                        queue_name=required('PIPELINE_EVENT_QUEUE'), credential=DefaultAzureCredential())
    message = next(queue.receive_messages(messages_per_page=1, visibility_timeout=300), None)
    if message is None:
        return 0
    data = json.loads(message.content)
    event = PipelineEvent.from_dict(json.loads(store.read(data['event_key'])))
    # Renew invisibility while the indexer runs, preserving at-most-one controller.
    import threading
    done = threading.Event()
    def renew() -> None:
        nonlocal message
        while not done.wait(120):
            message = queue.update_message(message, visibility_timeout=300)
    thread = threading.Thread(target=renew, daemon=True)
    thread.start()
    try:
        result = run_pipeline(store, event)
        if os.environ.get('STOP_SERVING_AFTER_RELEASE', 'true').lower() == 'true' and _serving_arm() == 'Running':
            _serving_arm('stop')
            _wait_serving_status('Stopped')
        queue.delete_message(message)
        print(json.dumps(result, sort_keys=True))
        return 0
    finally:
        try:
            if os.environ.get('STOP_SERVING_AFTER_RELEASE', 'true').lower() == 'true' and _serving_arm() == 'Running':
                _serving_arm('stop')
                _wait_serving_status('Stopped')
        finally:
            done.set()
            thread.join(timeout=2)

if __name__ == '__main__':
    raise SystemExit(main())
