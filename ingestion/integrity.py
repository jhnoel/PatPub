"""Reuse hashes for exact Blob versions; generate evidence during normal ingestion."""
from datetime import datetime
import hashlib
import json
import re
from azure.core.exceptions import ResourceNotFoundError, ResourceExistsError


def evidence_key(logical_key, etag):
    identity = hashlib.sha256((logical_key + '\0' + str(etag)).encode()).hexdigest()
    return f'integrity/v1/{identity}.json'


def optional_json(cache, key):
    try:
        return json.loads(cache.get_blob_client(key).download_blob().readall())
    except ResourceNotFoundError:
        return None


def legacy_versions(cache):
    """Automatically adopt old verified receipts when upgrading an existing stack.

    These optional compatibility records are absent in a new deployment. Their
    hashes are reusable only after the original graph proof and capture order
    are checked, and only for the same artifact version.
    """
    if hasattr(cache, '_patpub_legacy_versions'):
        return cache._patpub_legacy_versions
    adopted = optional_json(cache, 'integrity/v1/adopted-versions.json')
    if adopted is not None:
        cache._patpub_legacy_versions = adopted['versions']
        return cache._patpub_legacy_versions
    capture = optional_json(cache, 'migration/v1/artifact-version-capture.json')
    versions = {}
    if capture is not None:
        graph_key = 'migration/v1/full-release-graph-validation.json'
        graph = optional_json(cache, graph_key)
        candidate = optional_json(cache, 'migration/v1/full-release-candidate.json')
        if (not graph or not candidate or capture.get('release_id') != graph.get('release_id')
                or capture.get('manifest_sha256') != candidate.get('manifest', {}).get('sha256')
                or graph.get('status') != 'graph-validated-pending-search-canary'):
            raise RuntimeError('existing artifact cache lacks its completed graph proof')
        captured = datetime.fromisoformat(capture['capture_finished_utc'])
        completed = cache.get_blob_client(graph_key).get_blob_properties().last_modified
        if captured >= completed:
            raise RuntimeError('existing artifact cache was captured after graph validation')
        versions = capture['versions']
        save_proof(cache, 'integrity/v1/adopted-versions.json',
                   {'schema_version': 1, 'release_id': capture['release_id'],
                    'manifest_sha256': capture['manifest_sha256'], 'versions': versions})
    cache._patpub_legacy_versions = versions
    return versions


def matching(proof, key, properties):
    return (isinstance(proof, dict) and proof.get('key') == key
            and str(proof.get('etag')).strip('"') == str(properties.etag).strip('"')
            and proof.get('bytes') == properties.size
            and re.fullmatch(r'[0-9a-f]{64}', str(proof.get('sha256', ''))))


def save_proof(cache, key, proof):
    try:
        cache.get_blob_client(key).upload_blob(json.dumps(proof).encode(), overwrite=False, if_none_match='*')
    except ResourceExistsError:
        if json.loads(cache.get_blob_client(key).download_blob().readall()) != proof:
            raise RuntimeError('immutable integrity evidence conflict')


def published_version_proof(cache, logical_key, properties):
    """Bind a validated published digest to a Blob that has never changed.

    Azure changes Last-Modified on every content or metadata mutation. Equal
    creation and modification times, strictly before release publication,
    identify the original immutable version checked by that release. This
    covers later legacy appends absent from an older aggregate capture.
    """
    published = getattr(cache, '_patpub_published_before', None)
    artifact = getattr(cache, '_patpub_protected_artifacts', {}).get(logical_key)
    created = getattr(properties, 'creation_time', None)
    modified = getattr(properties, 'last_modified', None)
    if (published is None or artifact is None or created is None or modified is None
            or created != modified or not modified < published
            or properties.size != artifact['bytes']
            or not re.fullmatch(r'[0-9a-f]{64}', str(artifact.get('sha256', '')))):
        return None
    return {k: artifact[k] for k in ('key', 'sha256', 'bytes')} | {'etag': str(properties.etag)}


def artifact_ref(blob, logical_key, cache=None):
    batch_properties = getattr(cache, '_patpub_properties', {}) if cache is not None else {}
    before = batch_properties.get(logical_key)
    batched = before is not None
    if before is None and cache is not None:
        before = blob.get_blob_properties()
    key = evidence_key(logical_key, before.etag) if before else None
    if cache is not None:
        proof = legacy_versions(cache).get(logical_key)
        if not matching(proof, logical_key, before):
            proof = optional_json(cache, key)
        if proof is None:
            old_key = hashlib.sha256(logical_key.encode()).hexdigest()
            old = optional_json(cache, f'migration/v1/integrity/{old_key}.json')
            if old is not None:
                proof = dict(old, key=f"{old.get('container')}/{old.get('key')}")
                if matching(proof, logical_key, before):
                    proof = {k: proof[k] for k in ('key', 'sha256', 'bytes')} | {'etag': str(before.etag)}
                    save_proof(cache, key, proof)
        if matching(proof, logical_key, before):
            if not batched and blob.get_blob_properties().etag != before.etag:
                raise RuntimeError('artifact changed during checkpoint verification')
            return {k: proof[k] for k in ('key', 'sha256', 'bytes')}
        protected = getattr(cache, '_patpub_protected_artifacts', {})
        if logical_key in protected and proof is None:
            proof = published_version_proof(cache, logical_key, before)
            if proof is None:
                raise RuntimeError(f'published artifact lacks reusable hash evidence; refusing rehash: {logical_key}')
            if str(blob.get_blob_properties().etag).strip('"') != str(before.etag).strip('"'):
                raise RuntimeError(f'artifact changed during published-version adoption: {logical_key}')
            save_proof(cache, key, proof)
            return {k: proof[k] for k in ('key', 'sha256', 'bytes')}
    digest = hashlib.sha256()
    size = 0
    for block in blob.download_blob(max_concurrency=4).chunks():
        digest.update(block)
        size += len(block)
    result = {'key': logical_key, 'sha256': digest.hexdigest(), 'bytes': size}
    if cache is not None:
        if blob.get_blob_properties().etag != before.etag or size != before.size:
            raise RuntimeError('artifact changed during hash verification')
        save_proof(cache, key, dict(result, etag=str(before.etag)))
    return result
