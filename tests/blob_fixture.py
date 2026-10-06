"""File-backed fixture for the production Azure Blob SDK boundary."""
import hashlib
from datetime import datetime, timezone
from types import SimpleNamespace
from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError

class FileService:
    """Exercise real uploads/projections with a file-backed Azure SDK boundary."""
    def __init__(self, store):
        self.store = store
        self.reads = {}

    def get_blob_client(self, container, name):
        key = name if container == 'releases' else f'{container}/{name}'
        return FileBlob(self, key)

    def get_container_client(self, name):
        return SimpleNamespace(get_blob_client=lambda key: self.get_blob_client(name, key),
                               list_blobs=lambda name_starts_with='': self.list_blobs(name, name_starts_with))

    def list_blobs(self, container, prefix):
        folder = self.store.root if container == 'releases' else self.store.root / container
        return [SimpleNamespace(name=p.relative_to(folder).as_posix(),
                                last_modified=datetime.fromtimestamp(p.stat().st_mtime, timezone.utc))
                for p in folder.rglob('*') if p.is_file() and p.relative_to(folder).as_posix().startswith(prefix)]


class FileBlob:
    def __init__(self, service, key):
        self.service, self.key = service, key

    def exists(self):
        return self.service.store.exists(self.key)

    def upload_blob(self, data, **kwargs):
        if self.exists():
            raise ResourceExistsError('exists')
        path = self.service.store._safe(self.key)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data.read() if hasattr(data, 'read') else data)

    def download_blob(self, **kwargs):
        if not self.exists():
            raise ResourceNotFoundError('missing')
        self.service.reads[self.key] = self.service.reads.get(self.key, 0) + 1
        data = self.service.store.read(self.key)
        return SimpleNamespace(readall=lambda: data, chunks=lambda: iter([data]))

    def get_blob_properties(self):
        data = self.service.store.read(self.key)
        return SimpleNamespace(size=len(data), etag=hashlib.sha256(data).hexdigest())
