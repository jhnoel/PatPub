"""Container integration: real ZIP renderer, Parquet and Quickwit from empty disk.

Run using the ingestion image, mounting this directory at /tests and the XML
fixtures at /fixtures. Blob transport is file-backed; no Azure credentials or
migration files are present. This is separate from pytest's fast unit suite.
"""
import argparse
import io
import zipfile
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile

from blob_fixture import FileService
from ingestion.acquisition import PROFILE, source_generation
from ingestion.config import APS_PROFILE
from ingestion.orchestration import PipelineEvent
from ingestion import pipeline
from ingestion.release import ReleasePublisher
from ingestion.store import LocalBlobStore
from fts.production.indexer import indexer


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--aps', action='store_true')
    args = parser.parse_args()
    profile = APS_PROFILE if args.aps else PROFILE
    fixture = '/aps-fixtures/pack_fixture.zip' if args.aps else '/fixtures/fixture.zip'
    with tempfile.TemporaryDirectory(prefix='bootstrap-', dir='/work') as folder:
        root = Path(folder)
        store = LocalBlobStore(root / 'blob')
        service = FileService(store)
        if args.aps:
            # The pack-only Rust fixture is intentionally not a complete APS record.
            output = io.BytesIO()
            with zipfile.ZipFile(output, 'w', zipfile.ZIP_DEFLATED) as archive:
                archive.writestr('pftaps19830308_wk10.txt', 'PATN\nWKU  099999991\nAPN  17123456\nAPD  19820101\nISD  19830308\nTTL  Synthetic bootstrap fixture\nABST\nPAL  A synthetic archive for pipeline testing.\nDETD\nPAL  A fixture device with a testing mechanism.\nCLMS\nNUM  1\nPAL  A synthetic testing device.\n')
            raw = output.getvalue()
        else:
            raw = Path(fixture).read_bytes()
        raw_sha = hashlib.sha256(raw).hexdigest()
        service.get_blob_client('raw', 'fixture.zip').upload_blob(raw)
        event = PipelineEvent('render', {'raw': {'key': 'raw/fixture.zip', 'sha256': raw_sha, 'bytes': len(raw)},
                                       'renderer_profile': profile, 'source_generation': source_generation(raw_sha, profile),
                                       'source_identity': 'a' * 64})
        config = Path('/quickwit/node.yaml').read_text()
        config = re.sub(r'^storage:\n  azure:\n    account:.*\n    access_key:.*\n', '', config, flags=re.MULTILINE)
        config = re.sub(r'^metastore_uri:.*', f'metastore_uri: file://{store.root}/quickwit/metastores/production-v1', config, flags=re.MULTILINE)
        config = re.sub(r'^default_index_root_uri:.*', f'default_index_root_uri: file://{store.root}/quickwit/indexes', config, flags=re.MULTILINE)
        config = re.sub(r'^data_dir:.*', f'data_dir: {root}/qwdata', config, flags=re.MULTILINE)
        node = root / 'node.yaml'
        node.write_text(config)
        os.environ['QUICKWIT_CONFIG_PATH'] = str(node)
        os.environ['QW_INDEX_ROOT_URI'] = f'file://{store.root}/quickwit/indexes'
        os.environ['QW_ENABLE_INGEST_V2'] = 'false'
        os.environ['METADATA_BUCKET_COUNT'] = '2'
        readers = []
        def append(payload, projection, work):
            result = indexer.index_projection(service, service, payload, projection, work)
            # The synchronous writer has exited before a reader opens the metastore.
            reader = subprocess.Popen(['quickwit', 'run', '--config', str(node)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            readers.append(reader)
            indexer.wait_for_quickwit(reader)
            indexer.wait_for_searcher('patents')
            return result
        original_pause = pipeline.index_with_serving_paused
        pipeline.index_with_serving_paused = lambda store, payload, compute: pipeline.checkpoint(store, 'index', payload, compute)
        try:
            def canary(manifest):
                expected = manifest['publication_count']
                assert indexer.search_count('patents', '*') == expected
                response = json.loads(indexer.quickwit_api('/api/v1/patents/search', 'POST', {'query': '*', 'max_hits': 1}))
                hit = response['hits'][0]
                pack = store.read(hit['source_pack_key'])
                frame = pack[hit['offset']:hit['offset'] + hit['length']]
                import zstandard
                markdown = zstandard.ZstdDecompressor().decompress(frame, max_output_size=hit['uncompressed_length'])
                assert hashlib.sha256(markdown).hexdigest() == hit['source_revision'].removeprefix('sha256:')
                assert hashlib.blake2b(frame, digest_size=64).hexdigest()[:32] == hit['source_fingerprint'].removeprefix('blake2b-128:')
                assert markdown.startswith(f"<!-- publication_uid: {hit['publication_uid']} -->\n".encode())
                return True
            result = pipeline.run_pipeline(store, event, root / 'archive', service=service, indexer=append, canary=canary)
            manifest = ReleasePublisher(store).verify_selector(store.read_json('releases/current.json'))
            assert manifest.publication_count > 0
            assert service.reads['raw/fixture.zip'] == 1
            repeated = pipeline.run_pipeline(store, event, root / 'retry', service=service, indexer=append, canary=lambda _: False)
            assert repeated['release_id'] == result['release_id']
            assert service.reads['raw/fixture.zip'] == 1
            print(json.dumps({'bootstrap_status': 'passed', 'publication_count': manifest.publication_count,
                              'raw_downloads': 1, 'renderer_profile': profile, 'release_id': result['release_id']}))
        finally:
            pipeline.index_with_serving_paused = original_pause
            for reader in readers:
                reader.terminate()
                try:
                    reader.wait(timeout=20)
                except subprocess.TimeoutExpired:
                    reader.kill()
                    reader.wait()


if __name__ == '__main__':
    main()
