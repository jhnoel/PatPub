"""USPTO discovery and durable archive acquisition, shared by backfill and steady state."""
from __future__ import annotations

import hashlib
import json
import os
import re
from pathlib import Path
import tempfile
import time
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor
from email.utils import parsedate_to_datetime
from urllib.parse import urlparse
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from .config import load_config, archive_year, archive_date

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
    candidates = sorted((item for item in files if isinstance(item, dict) and
                         str(item.get('fileTypeText') or 'Data').lower() == 'data' and
                         str(item.get('fileName') or '').lower().endswith('.zip')),
                        key=lambda item: (str(item.get('fileDataFromDate') or item.get('fileDate') or item.get('fileReleaseDate') or ''),
                                          int(re.search(r'_r(\d+)(?=\.zip$)', item['fileName']).group(1))
                                          if re.search(r'_r(\d+)(?=\.zip$)', item['fileName']) else 0,
                                          str(item['fileName'])), reverse=True)
    # Backfill each weekly source's latest revision, never reintroduce an older correction.
    seen = set()
    result = []
    for item in candidates:
        identity = re.sub(r'_r\d+(?=\.zip$)', '', item['fileName'])
        if identity not in seen:
            seen.add(identity)
            result.append(item)
    return list(reversed(result))


def checked_url(url: str) -> str:
    parsed = urlparse(url)
    if parsed.scheme != 'https' or parsed.hostname not in HOSTS:
        raise RuntimeError('ODP download URL is not an approved HTTPS host')
    return url


def retry_delay(error, attempt):
    value = (getattr(error, 'headers', None) or {}).get('Retry-After')
    if value:
        try:
            return max(0, float(value))
        except ValueError:
            try:
                return max(0, (parsedate_to_datetime(value) - datetime.now(timezone.utc)).total_seconds())
            except (TypeError, ValueError):
                pass
    return min(15 * 2 ** (attempt - 1), 60)


def catalog(endpoint, api_key):
    for attempt in range(1, 6):
        try:
            with urlopen(Request(endpoint, headers={'X-API-KEY': api_key}), timeout=60) as response:
                return json.load(response)
        except (HTTPError, URLError, TimeoutError) as exc:
            if isinstance(exc, HTTPError) and exc.code not in {429, 500, 502, 503, 504}:
                raise
            if attempt == 5:
                raise
            delay = retry_delay(exc, attempt)
            # A longer provider window is retried by the next scheduled run.
            if delay > 300:
                raise
            print(f'uspto_catalog_retry attempt={attempt} delay_seconds={delay:g}', flush=True)
            time.sleep(delay)


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
        except (OSError, URLError, TimeoutError) as exc:
            attempts += 1
            if attempts >= 5:
                raise
            delay = retry_delay(exc, attempts)
            if delay > 300:
                raise
            time.sleep(delay)
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


def publish_render_event(blob_service, raw_ref: dict, source_identity: str, *, profile=None, category=None, publication_year=None, publication_date=None) -> str:
    """Write an immutable render event for a raw ZIP already in Blob."""
    profile = PROFILE if profile is None else profile
    payload = {
        'raw': raw_ref,
        'source_generation': source_generation(str(raw_ref['sha256']), profile),
        'renderer_profile': profile,
        'source_identity': source_identity,
    }
    if category is not None:
        payload.update(category=category.name, product=category.product, publication_year=publication_year, publication_date=publication_date)
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


def published_archives(service) -> dict:
    """Recognize validated legacy archives even when fetch markers are absent.

    Only small generation manifests and Blob listings are read. The derived
    inventory is cached against the exact selected manifest, so ordinary runs
    do not repeatedly scan the historical controls or change renderer profiles.
    """
    releases = service.get_container_client('releases')
    current = releases.get_blob_client('releases/current.json')
    if not current.exists():
        return {}
    selector = json.loads(current.download_blob().readall())
    digest = str(selector['manifest_sha256'])
    cache = releases.get_blob_client(f'fetch/published-v1/{digest}.json')
    if cache.exists():
        value = json.loads(cache.download_blob().readall())
        if value.get('manifest_sha256') != digest:
            raise RuntimeError('published archive inventory identity mismatch')
        return value['archives']
    data = releases.get_blob_client(selector['manifest_key']).download_blob().readall()
    from .models import canonical_json
    manifest = json.loads(data)
    if hashlib.sha256(canonical_json(manifest)).hexdigest() != digest:
        raise RuntimeError('selected release manifest digest mismatch')
    superseded = set(manifest.get('source', {}).get('superseded_generation_ids', []))
    refs = [ref for ref in manifest['source']['generation_manifests']
            if ref['generation_id'] not in superseded]
    source = service.get_container_client('source')
    properties = {item.name: item for item in source.list_blobs()}
    def read(ref):
        artifact = ref['artifact']
        container, name = artifact['key'].split('/', 1)
        if container != 'source':
            raise RuntimeError('generation manifest must use the source container')
        contents = source.get_blob_client(name).download_blob().readall()
        if len(contents) != artifact['bytes'] or hashlib.sha256(contents).hexdigest() != artifact['sha256']:
            raise RuntimeError('published generation manifest integrity mismatch')
        generation = json.loads(contents)
        raw = generation['raw']
        parts = raw['key'].split('/')
        product = next((p for p in parts if p in {'PTGRAPS', 'PTGRXML', 'APPXML'}), None)
        filename = parts[-1]
        # Content-addressed raw names use normal fetch markers instead.
        if product is None or filename == raw['sha256'] + '.zip':
            return None
        return product + '/' + filename, {'bytes': raw['bytes'],
            'published_at': properties[name].last_modified.isoformat(),
            'generation_id': ref['generation_id']}
    archives = {}
    with ThreadPoolExecutor(max_workers=16) as pool:
        for result in pool.map(read, refs):
            if result is not None:
                name, value = result
                if name not in archives or value['published_at'] > archives[name]['published_at']:
                    archives[name] = value
    value = {'schema_version': 1, 'manifest_sha256': digest, 'archives': archives}
    try:
        cache.upload_blob(canonical(value), overwrite=False, if_none_match='*')
    except ResourceExistsError:
        if json.loads(cache.download_blob().readall()) != value:
            raise RuntimeError('immutable published archive inventory conflict')
    return archives


def archive_already_published(item, product, published) -> bool:
    existing = published.get(product + '/' + str(item['fileName']))
    if existing is None or item.get('fileSize') != existing['bytes']:
        return False
    changed = item.get('fileLastModifiedDateTime')
    if not changed:
        return False
    try:
        modified = datetime.fromisoformat(str(changed).replace('Z', '+00:00'))
        if modified.tzinfo is None:
            modified = modified.replace(tzinfo=timezone.utc)
        captured = datetime.fromisoformat(existing['published_at'])
    except (ValueError, TypeError):
        return False
    # Allow a day for the publisher's timezone and coarse timestamp precision.
    # Recent or changed archives take the normal acquisition/validation path.
    return modified + timedelta(days=1) < captured


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


def rerender_from_blob(blob_service, config=None) -> int:
    """Create render events for landed ZIPs without calling USPTO.

    A renderer-profile change produces a new source generation for the same
    publisher filename. The newest fetch marker wins when several revisions
    share that filename. Generations already active are left alone.
    """
    config = config or load_config()
    categories = {c.product: c for c in config.categories if c.enabled}
    container = blob_service.get_container_client('releases')
    newest: dict[str, tuple] = {}
    for item in container.list_blobs(name_starts_with='fetch/v1/'):
        payload = json.loads(container.get_blob_client(item.name).download_blob().readall())
        product = str(payload.get('product') or '')
        name = str(payload.get('fileName') or '')
        raw = payload.get('raw')
        if not product or not name or not isinstance(raw, dict):
            raise RuntimeError(f'fetch marker {item.name} is incomplete')
        if product not in categories:
            continue
        category = categories[product]
        try:
            year = archive_year({'fileDownloadURI': payload.get('url', '')})
        except RuntimeError:
            year = None
        if year is not None and not category.accepts(year):
            continue
        identity = publication_source_identity(product, name)
        modified = getattr(item, 'last_modified', None)
        current = newest.get(identity)
        if current is None or _marker_is_newer(modified, item.name, current[0], current[1]):
            newest[identity] = (modified, item.name, raw, identity, category, year)
    serving = active_generations(blob_service)
    queued = 0
    for _modified, marker_name, raw, identity, category, year in newest.values():
        key = str(raw.get('key') or '')
        if not key.startswith('raw/') or not raw.get('sha256'):
            raise RuntimeError(f'fetch marker {marker_name} has no raw archive')
        blob_name = key.split('/', 1)[1]
        raw_blob = blob_service.get_blob_client('raw', blob_name)
        if not raw_blob.exists():
            raise RuntimeError(f'raw archive {key} is missing')
        profile = category.profile if category.product == 'PTGRAPS' else PROFILE
        if source_generation(str(raw['sha256']), profile) in serving:
            continue
        publish_render_event(blob_service, {
            'key': key, 'sha256': raw['sha256'], 'bytes': int(raw['bytes']),
        }, identity, profile=profile, category=category, publication_year=year)
        queued += 1
    return queued


def fetch_one(blob_service, item: dict, product: str, api_key: str, *, category=None) -> bool:
    name = str(item['fileName'])
    url = checked_url(str(item.get('fileDownloadURI') or ''))
    catalog_id = archive_identity(product, item, '')
    catalog_marker = blob_service.get_blob_client('releases', f'fetch/catalog-v1/{product}/{catalog_id}.json')
    if catalog_marker.exists():
        return False
    old_marker = blob_service.get_blob_client('releases', f'fetch/v1/{product}/{catalog_id}.json')
    if old_marker.exists():
        return False
    validator = remote_validator(url, api_key)
    source_id = archive_identity(product, item, validator)
    marker = blob_service.get_blob_client('releases', f'fetch/v1/{product}/{source_id}.json')
    # Dedupe runs before the download and does not require a validator, so
    # verified markers seeded out of band skip
    # archives that landed outside this job.
    if marker.exists():
        catalog_marker.upload_blob(marker.download_blob().readall(), overwrite=False, if_none_match='*')
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
    # Persist the event before recording fetch completion so a crash cannot strand an archive.
    event_key = publish_render_event(blob_service, raw_ref, source_identity,
                                     profile=category.profile if category else PROFILE,
                                     category=category, publication_year=archive_year(item) if category else None,
                                     publication_date=archive_date(item) if category else None)
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
    catalog_marker.upload_blob(canonical(marker_payload), overwrite=False, if_none_match='*')
    return True


def fetch_archives(blobs, config=None, *, limit=None) -> int:
    """Backfill the oldest missing archive across all enabled document categories."""
    config = config or load_config()
    if os.environ.get('RERENDER_FROM_BLOB', '').strip() == '1':
        return rerender_from_blob(blobs, config)
    limit = config.archives_per_run if limit is None else limit
    categories = [c for c in config.categories if c.enabled]
    if not categories or limit == 0:
        return 0
    key = required('USPTO_API_KEY')
    published = published_archives(blobs)
    total = 0
    catalogs = {}
    for category in categories:
        endpoint = f'https://api.uspto.gov/api/v1/datasets/products/{category.product}'
        items = entries(catalog(endpoint, key))
        catalogs[category.name] = (category, iter([item for item in items if category.accepts(archive_year(item))]))
    # Merge catalogs chronologically across every enabled document category.
    candidates = {}
    for name, (category, items) in catalogs.items():
        item = next(items, None)
        if item is not None:
            candidates[name] = item
    while candidates and (limit is None or total < limit):
        name = min(candidates, key=lambda name: (archive_date(candidates[name]), str(candidates[name]['fileName']), name))
        item = candidates[name]
        category, items = catalogs[name]
        if not archive_already_published(item, category.product, published) and fetch_one(blobs, item, category.product, key, category=category):
            total += 1
        following = next(items, None)
        if following is None:
            del candidates[name]
        else:
            candidates[name] = following
    return total
