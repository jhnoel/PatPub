"""Focused fetch identity and transfer recovery checks."""

import hashlib
import io
import json
from urllib.error import HTTPError
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from ingestion import acquisition as fetch_cloud


class Response(io.BytesIO):
    def __init__(self, data: bytes, status: int, headers: dict):
        super().__init__(data)
        self.status = status
        self.headers = headers

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()


class FetchTests(unittest.TestCase):
    def test_catalog_retries_throttling_after_provider_delay(self):
        error = HTTPError('https://api.uspto.gov/catalog', 429, 'throttled', {'Retry-After': '2'}, None)
        response = Response(json.dumps({'ok': True}).encode(), 200, {})
        with patch.object(fetch_cloud, 'urlopen', side_effect=[error, response]), \
             patch.object(fetch_cloud.time, 'sleep') as sleep:
            self.assertEqual(fetch_cloud.catalog('https://api.uspto.gov/catalog', 'key'), {'ok': True})
            sleep.assert_called_once_with(2)

    def test_catalog_does_not_retry_invalid_credentials_or_long_provider_windows(self):
        for status, headers in [(401, {}), (429, {'Retry-After': '3600'})]:
            error = HTTPError('https://api.uspto.gov/catalog', status, 'rejected', headers, None)
            with patch.object(fetch_cloud, 'urlopen', side_effect=error) as request, \
                 patch.object(fetch_cloud.time, 'sleep') as sleep:
                with self.assertRaises(HTTPError):
                    fetch_cloud.catalog('https://api.uspto.gov/catalog', 'key')
                request.assert_called_once()
                sleep.assert_not_called()

    def test_same_filename_new_revision_has_new_identity(self):
        first = {'fileName': 'same.zip', 'fileSize': 10}
        second = {'fileName': 'same.zip', 'fileSize': 11}
        self.assertNotEqual(fetch_cloud.archive_identity('PTGRXML', first),
                            fetch_cloud.archive_identity('PTGRXML', second))
        self.assertNotEqual(fetch_cloud.archive_identity('PTGRXML', first, 'etag-one'),
                            fetch_cloud.archive_identity('PTGRXML', first, 'etag-two'))

    def test_resumes_partial_response_at_verified_boundary(self):
        responses = [Response(b'abc', 200, {}),
                     Response(b'def', 206, {'Content-Range': 'bytes 3-5/6'})]

        def fetch(request, timeout):
            response = responses.pop(0)
            if response.status == 200:
                original = response.read

                def disconnect_after_first_chunk(size=-1):
                    if response.tell() == 3:
                        raise OSError('connection lost')
                    return original(size)

                response.read = disconnect_after_first_chunk
            else:
                self.assertEqual(request.get_header('Range'), 'bytes=3-')
            return response

        with tempfile.TemporaryDirectory() as folder, \
             patch.object(fetch_cloud, 'urlopen', side_effect=fetch), \
             patch.object(fetch_cloud.time, 'sleep'):
            path = Path(folder) / 'archive.zip'
            sha, size = fetch_cloud.download_archive('https://api.uspto.gov/file.zip', 'key', path, 100)
            self.assertEqual(size, 6)
            self.assertEqual(sha, hashlib.sha256(b'abcdef').hexdigest())
            self.assertEqual(path.read_bytes(), b'abcdef')


if __name__ == '__main__':
    unittest.main()
