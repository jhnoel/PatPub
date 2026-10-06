"""Sequential ingestion with durable checkpoints and atomic release publication."""
from __future__ import annotations

import hashlib
import json
import os
import time
from urllib import request as urlrequest
from urllib import error as urlerror

from .orchestration import DownstreamOrchestrator, PipelineEvent, OrchestrationError
from .release import AzureBlobReleaseStore, _iter_artifacts
from .search_query import generation_filter
from .models import canonical_json
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


def index_with_serving_paused(store, payload: dict, compute) -> dict:
    """Give the file-backed Quickwit metastore to one process at a time."""
    index_event = PipelineEvent('index', payload)
    if store.exists(f'events/results/{index_event.event_id}.json'):
        result = checkpoint(store, 'index', payload, compute)
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
    result = checkpoint(store, 'index', payload, compute)
    _serving_arm('start')
    _wait_serving_status('Running')
    _wait_serving_health()
    return result


def checkpoint(store, stage: str, payload: dict, compute) -> dict:
    """Run a stage in this process, retaining its immutable completion receipt."""
    event = PipelineEvent(stage, payload)
    result_key = f'events/results/{event.event_id}.json'
    if store.exists(result_key):
        envelope = json.loads(store.read(result_key))
        if envelope.get('event_id') != event.event_id or envelope.get('event_fingerprint') != event.fingerprint:
            raise RuntimeError('worker result identity mismatch')
        return envelope['result']
    result = compute()
    envelope = {'event_id': event.event_id, 'event_fingerprint': event.fingerprint, 'result': result}
    if not store.write_json_if_absent(result_key, envelope) and json.loads(store.read(result_key)) != envelope:
        raise RuntimeError('immutable stage result conflict')
    return result


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
        raise RuntimeError('Correction requires explicit preservation before indexing: ' + ', '.join(sorted(missing)))


def run_pipeline(store, event: PipelineEvent, work, *, service, indexer, canary=candidate_canary) -> dict:
    """Append one archive to the durable index and atomically publish it."""
    if event.stage != 'render':
        raise RuntimeError('archive entry must be a render event')
    prior = None
    predecessor_sha = None
    if store.exists('releases/current.json'):
        selector_bytes = store.read('releases/current.json')
        predecessor_sha = hashlib.sha256(selector_bytes).hexdigest()
        active = json.loads(selector_bytes)
        prior_bytes = store.read(active['manifest_key'])
        prior = json.loads(prior_bytes)
        if hashlib.sha256(canonical_json(prior)).hexdigest() != active.get('manifest_sha256'):
            raise OrchestrationError('active release manifest digest mismatch')
        if event.payload['source_generation'] in prior['source_generations']:
            return {'release_id': prior['release_id'], 'selector': active}
        if prior['fulltext'].get('index_id') != os.environ.get('PATENT_INDEX_ID', 'patents'):
            raise OrchestrationError('configured index does not match the active release')
        protect = getattr(store, 'protect_artifacts', None)
        if protect:
            protect((artifact for _, artifact in _iter_artifacts(prior, 'release')),
                    manifest_key=active['manifest_key'])
        prefetch = getattr(store, 'prefetch_json', None)
        if prefetch:
            prefetch(a['key'] for _, a in _iter_artifacts(prior, 'release'))
        print('historical_controls_prefetched', flush=True)

    raw = event.payload['raw']
    from .archive import prepare_archive
    from .projection import project_metadata
    projection_path, render = prepare_archive(service, event.payload, work)
    recorded = checkpoint(store, 'render', dict(event.payload), lambda: render)
    # Retries must reproduce the identical archive before reusing stage receipts.
    comparable = {k: recorded[k] for k in render}
    comparable['projection'] = {k: recorded['projection'][k] for k in ('sha256', 'bytes')}
    if comparable != render:
        raise RuntimeError('rendered archive differs from its checkpoint')
    generation = DownstreamOrchestrator(store).run_generation(PipelineEvent('generation', {
        'raw': raw, 'generation_id': event.payload['source_generation'],
        'source_identity': event.payload['source_identity'],
        'renderer_profile': event.payload['renderer_profile'],
        'source_packs': render['source_packs'], 'publication_count': render['publication_count'],
    }))
    # The projection is temporary and shared by metadata and indexing.
    projection = {
        'sha256': render['projection']['sha256'],
        'bytes': render['projection']['bytes'],
    }
    metadata_payload = {
        'raw': raw,
        'source_generation': event.payload['source_generation'],
        'renderer_profile': event.payload['renderer_profile'],
        'source_packs': render['source_packs'],
        'projection': projection,
        'snapshot_id': projection['sha256'],
    }
    metadata = checkpoint(store, 'metadata', metadata_payload,
                          lambda: project_metadata(service, metadata_payload, projection_path, work / 'metadata'))

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
    metadata_count = metadata.get('publication_count', metadata.get('metadata', {}).get('publication_count'))
    if metadata_count != render['publication_count']:
        raise RuntimeError('stage publication counts differ')
    index = index_with_serving_paused(store, index_payload,
                                      lambda: indexer(index_payload, projection_path, work))
    if index['publication_count'] != render['publication_count']:
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
    return DownstreamOrchestrator(store).run_release(release, canary)
