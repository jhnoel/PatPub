"""Create-only artifact uploads shared by the ingestion stages."""
import hashlib
from azure.core.exceptions import ResourceExistsError


def upload_file(blob, path):
    with path.open("rb") as handle:
        try:
            blob.upload_blob(handle, length=path.stat().st_size, overwrite=False, if_none_match="*")
            return
        except ResourceExistsError:
            pass
    digest = hashlib.sha256()
    for chunk in blob.download_blob(max_concurrency=4).chunks():
        digest.update(chunk)
    expected = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            expected.update(chunk)
    if digest.digest() != expected.digest() or blob.get_blob_properties().size != path.stat().st_size:
        raise RuntimeError("immutable artifact conflict")
