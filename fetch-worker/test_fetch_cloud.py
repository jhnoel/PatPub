"""Focused fetch identity and transfer recovery checks."""

import hashlib
import io
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import fetch_cloud


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
