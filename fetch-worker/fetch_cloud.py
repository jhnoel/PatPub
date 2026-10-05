"""Daily USPTO ODP fetch and blob-only rerender of retained archives.

Immutable fetch/v1 markers deduplicate archives before downloading. Archives
landed outside this pipeline require verified deduplication markers before
enabling scheduled fetch.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
from pathlib import Path
import tempfile
import time
from urllib.parse import urlparse
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

try:
    from azure.core.exceptions import ResourceExistsError
except ImportError:  # pragma: no cover - unit tests without the Azure SDK
    class ResourceExistsError(Exception):
        """Stand-in so event publishing can be tested without azure-storage-blob."""

PROFILE = 'uspto-xml-markdown-v2:zstd-9-checksum'
HOSTS = {'api.uspto.gov', 'data.uspto.gov'}


def canonical(value: object) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(',', ':')) + '\n').encode()


def required(name: str) -> str:
    value = os.environ.get(name, '').strip()
    if not value:
        raise RuntimeError(f'{name} is required')
    return value


def entries(manifest: dict) -> list[dict]:
    bags = manifest.get('bulkDataProductBag', [])
    if not isinstance(bags, list) or not bags:
        raise RuntimeError('ODP manifest has no product bag')
    files = bags[0].get('productFileBag', {}).get('fileDataBag', [])
    if not isinstance(files, list):
        raise RuntimeError('ODP file list is invalid')
    return sorted((item for item in files if isinstance(item, dict) and
                   str(item.get('fileTypeText') or 'Data').lower() == 'data' and
                   str(item.get('fileName') or '').lower().endswith('.zip')),
                  key=lambda item: (str(item.get('fileDate') or item.get('fileReleaseDate') or ''), str(item['fileName'])),
                  reverse=True)


def checked_url(url: str) -> str:
    parsed = urlparse(url)
    if parsed.scheme != 'https' or parsed.hostname not in HOSTS:
        raise RuntimeError('ODP download URL is not an approved HTTPS host')
    return url


def archive_identity(product: str, item: dict, validator: str = '') -> str:
    # The filename alone is not a revision: ODP can replace the bytes behind it.
    return hashlib.sha256(canonical({'product': product, 'revision': item, 'validator': validator})).hexdigest()[:32]


def remote_validator(url: str, api_key: str) -> str:
    try:
        with urlopen(Request(url, headers={'X-API-KEY': api_key}, method='HEAD'), timeout=30) as response:
            return response.headers.get('ETag') or response.headers.get('Last-Modified') or ''
    except (HTTPError, URLError, TimeoutError):
        return ''


def download_archive(url: str, api_key: str, path: Path, max_bytes: int | None = None) -> tuple[str, int]:
    """Resume a failed HTTP transfer with Range and verify its byte boundary."""
    attempts = 0
    while attempts < 5:
        offset = path.stat().st_size if path.exists() else 0
        headers = {'X-API-KEY': api_key}
        if offset:
            headers['Range'] = f'bytes={offset}-'
        try:
            with urlopen(Request(url, headers=headers), timeout=120) as response:
                status = getattr(response, 'status', 200)
                if offset and status == 206:
                    content_range = response.headers.get('Content-Range', '')
                    if not content_range.startswith(f'bytes {offset}-'):
                        raise RuntimeError('resumed archive has incorrect Content-Range')
                elif offset and status == 200:
                    offset = 0  # Origin ignored Range; restart safely.
                elif offset:
                    raise RuntimeError(f'origin rejected archive resume: HTTP {status}')
                mode = 'ab' if offset else 'wb'
                size = offset
                with path.open(mode) as output:
                    while block := response.read(8 * 1024 * 1024):
                        size += len(block)
                        if max_bytes is not None and size > max_bytes:
                            raise RuntimeError('archive exceeds configured size cap')
                        output.write(block)
            hasher = hashlib.sha256()
            with path.open('rb') as stream:
                for block in iter(lambda: stream.read(8 * 1024 * 1024), b''):
                    hasher.update(block)
            return hasher.hexdigest(), size
        except (OSError, URLError, TimeoutError):
            attempts += 1
            if attempts >= 5:
                raise
            time.sleep(min(2 ** attempts, 16))
    raise RuntimeError('archive download retry limit exceeded')


def source_generation(raw_sha256: str, profile: str | None = None) -> str:
    chosen = PROFILE if profile is None else profile
    return 'sha256:' + hashlib.sha256(canonical({
        'raw_sha256': raw_sha256, 'renderer_profile': chosen,
    })).hexdigest()


def publication_source_identity(product: str, name: str) -> str:
    """Corrections named _rN replace the same weekly publication source."""
    canonical_name = re.sub(r'_r\d+(?=\.zip$)', '', name)
    return hashlib.sha256(f'{product}\0{canonical_name}'.encode()).hexdigest()


def publish_render_event(blob_service, queue, raw_ref: dict, source_identity: str) -> str:
    """Write an immutable render event for a raw ZIP already in Blob."""
    payload = {
        'raw': raw_ref,
        'source_generation': source_generation(str(raw_ref['sha256'])),
        'renderer_profile': PROFILE,
        'source_identity': source_identity,
    }
    fingerprint = hashlib.sha256(canonical({
        'schema_version': 1, 'stage': 'render', 'payload': payload,
    })).hexdigest()
    event_id = f'render-{fingerprint[:32]}'
    event = {
        'schema_version': 1, 'event_id': event_id, 'event_fingerprint': fingerprint,
        'stage': 'render', 'attempt': 0, 'payload': payload,
    }
    event_key = f'events/{event_id}.json'
    events = blob_service.get_blob_client('releases', event_key)
    try:
        events.upload_blob(canonical(event), overwrite=False, if_none_match='*')
    except ResourceExistsError:
        if json.loads(events.download_blob().readall()) != event:
            raise RuntimeError('immutable fetch event conflict')
    queue.send_message(json.dumps({'event_key': event_key}))
    return event_key


def active_generations(blob_service) -> set[str]:
    """Return generations the current release still serves."""
    current = blob_service.get_blob_client('releases', 'releases/current.json')
    if not current.exists():
        return set()
    selector = json.loads(current.download_blob().readall())
    manifest_key = str(selector.get('manifest_key') or '')
    if not manifest_key.startswith('releases/'):
        raise RuntimeError('active selector manifest key is invalid')
    manifest = json.loads(blob_service.get_blob_client('releases', manifest_key).download_blob().readall())
    superseded = set(manifest.get('source', {}).get('superseded_generation_ids', []))
    generations = manifest.get('source_generations', [])
    if not isinstance(generations, list):
        raise RuntimeError('active release generations are invalid')
    return {str(generation) for generation in generations if str(generation) not in superseded}


def _marker_is_newer(modified, name: str, current_modified, current_name: str) -> bool:
    if modified is None and current_modified is None:
        return name >= current_name
    if modified is None:
        return False
    if current_modified is None:
        return True
    if modified != current_modified:
        return modified > current_modified
    return name >= current_name


def rerender_from_blob(blob_service, queue) -> int:
    """Queue render events for landed ZIPs without calling USPTO.

    A renderer-profile change produces a new source generation for the same
    publisher filename. The newest fetch marker wins when several revisions
    share that filename. Generations already active are left alone.
    """
    container = blob_service.get_container_client('releases')
    newest: dict[str, tuple] = {}
    for item in container.list_blobs(name_starts_with='fetch/v1/'):
        payload = json.loads(container.get_blob_client(item.name).download_blob().readall())
        product = str(payload.get('product') or '')
        name = str(payload.get('fileName') or '')
        raw = payload.get('raw')
        if not product or not name or not isinstance(raw, dict):
            raise RuntimeError(f'fetch marker {item.name} is incomplete')
        identity = publication_source_identity(product, name)
        modified = getattr(item, 'last_modified', None)
        current = newest.get(identity)
        if current is None or _marker_is_newer(modified, item.name, current[0], current[1]):
            newest[identity] = (modified, item.name, raw, identity)
    serving = active_generations(blob_service)
    queued = 0
    for _modified, marker_name, raw, identity in newest.values():
        key = str(raw.get('key') or '')
        if not key.startswith('raw/') or not raw.get('sha256'):
            raise RuntimeError(f'fetch marker {marker_name} has no raw archive')
        blob_name = key.split('/', 1)[1]
        raw_blob = blob_service.get_blob_client('raw', blob_name)
        if not raw_blob.exists():
            raise RuntimeError(f'raw archive {key} is missing')
        if source_generation(str(raw['sha256'])) in serving:
            continue
        publish_render_event(blob_service, queue, {
            'key': key, 'sha256': raw['sha256'], 'bytes': int(raw['bytes']),
        }, identity)
        queued += 1
    return queued


def fetch_one(blob_service, queue, item: dict, product: str, api_key: str) -> bool:
    name = str(item['fileName'])
    url = checked_url(str(item.get('fileDownloadURI') or ''))
    validator = remote_validator(url, api_key)
    source_id = archive_identity(product, item, validator)
    marker = blob_service.get_blob_client('releases', f'fetch/v1/{product}/{source_id}.json')
    # Dedupe runs before the download and does not require a validator, so
    # verified markers seeded out of band skip
    # archives that landed outside this job.
    if marker.exists():
        return False
    with tempfile.TemporaryDirectory(prefix='odp-fetch-') as folder:
        path = Path(folder) / 'archive.zip'
        archive_sha, size = download_archive(url, api_key, path)
        expected = item.get('fileSize')
        if expected is not None and int(expected) != size:
            raise RuntimeError('ODP archive size differs from manifest')
        raw_key = f'raw/v1/{product}/{archive_sha}.zip'
        raw = blob_service.get_blob_client('raw', raw_key.split('/', 1)[1])
        with path.open('rb') as stream:
            try:
                raw.upload_blob(stream, length=size, overwrite=False, if_none_match='*')
            except ResourceExistsError:
                properties = raw.get_blob_properties()
                if properties.size != size:
                    raise RuntimeError('immutable archive size conflict')
                actual = hashlib.sha256()
                for block in raw.download_blob(max_concurrency=4).chunks():
                    actual.update(block)
                if actual.hexdigest() != archive_sha:
                    raise RuntimeError('immutable archive hash conflict')
    if not validator:
        # Legacy archive markers embed archive_sha in the key and could only
        # be checked after the download. Kept for markers written before the
        # validator-independent check above existed.
        archive_marker = blob_service.get_blob_client(
            'releases', f'fetch/v1/{product}/{source_id}/{archive_sha}.json')
        if archive_marker.exists():
            return False
    raw_ref = {'key': raw_key, 'sha256': archive_sha, 'bytes': size}
    # Stable across corrections to the same publisher filename, while the
    # generation ID changes with the archive bytes and renderer profile.
    source_identity = publication_source_identity(product, name)
    # Enqueue before recording completion so a crash cannot strand an archive.
    event_key = publish_render_event(blob_service, queue, raw_ref, source_identity)
    marker_payload = {'product': product, 'fileName': name, 'url': url, 'validator': validator, 'raw': raw_ref,
                      'event_key': event_key}
    # The validator-independent marker is always written, so the next run
    # dedupes before downloading. The legacy per-archive marker is not needed
    # once this one exists.
    try:
        marker.upload_blob(canonical(marker_payload), overwrite=False, if_none_match='*')
    except ResourceExistsError:
        if json.loads(marker.download_blob().readall()) != marker_payload:
            raise RuntimeError('immutable fetch marker conflict')
    return True


def main() -> int:
    from azure.identity import DefaultAzureCredential
    from azure.storage.blob import BlobServiceClient
    from azure.storage.queue import QueueClient

    account = required('AZURE_STORAGE_ACCOUNT')
    credential = DefaultAzureCredential()
    blobs = BlobServiceClient(f'https://{account}.blob.core.windows.net', credential=credential)
    queue = QueueClient(account_url=f'https://{account}.queue.core.windows.net',
                        queue_name=required('PIPELINE_EVENT_QUEUE'), credential=credential)
    if os.environ.get('RERENDER_FROM_BLOB', '').strip() == '1':
        queued = rerender_from_blob(blobs, queue)
        print(json.dumps({'rerendered_archives': queued}))
        return 0
    key = required('USPTO_API_KEY')
    limit = int(os.environ.get('MAX_ARCHIVES_PER_RUN', '1'))
    if limit < 1 or limit > 2:
        raise RuntimeError('MAX_ARCHIVES_PER_RUN must be 1 or 2')
    total = 0
    for product in ('PTGRXML', 'APPXML'):
        endpoint = f'https://api.uspto.gov/api/v1/datasets/products/{product}'
        with urlopen(Request(endpoint, headers={'X-API-KEY': key}), timeout=60) as response:
            manifest = json.load(response)
        for item in entries(manifest):
            if fetch_one(blobs, queue, item, product, key):
                total += 1
                break
        if total >= limit:
            break
    result = {'fetched_archives': total}
    result_key = os.environ.get('FETCH_RESULT_KEY', '').strip()
    if result_key:
        blobs.get_blob_client('releases', result_key).upload_blob(canonical(result), overwrite=False, if_none_match='*')
    print(json.dumps(result))
    return 0

if __name__ == '__main__':
    raise SystemExit(main())
