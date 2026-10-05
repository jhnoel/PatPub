"""Deterministic Parquet metadata projections for an immutable release.

The projection consumes the Rust full-text NDJSON output.  It deliberately
does not copy canonical text: the metadata rows retain the source pointer
needed to hydrate the independently compressed source frame.  A small SQLite
staging table is used only as an external-sort and uniqueness guard; the
published artifacts are immutable Parquet files.
"""

from __future__ import annotations

from dataclasses import dataclass
import argparse
import hashlib
import json
from pathlib import Path
import sqlite3
import tempfile
from typing import Any, Iterator

from .models import canonical_json


class MetadataProjectionError(ValueError):
    """Raised when a metadata row or immutable output is invalid."""


_TEXT_COLUMNS = (
    "publication_uid",
    "publication_number",
    "patent_number",
    "application_number",
    "publication_date",
    "filing_date",
    "doc_kind",
    "title",
    "source_generation",
    "source_pack",
    "source_pack_key",
    "source_revision",
    "source_fingerprint",
)
_LIST_COLUMNS = ("applicant", "assignee", "ipc", "us_class")
_REQUIRED = (
    "publication_uid",
    "source_generation",
    "source_pack",
    "source_pack_key",
    "source_revision",
    "source_fingerprint",
    "offset",
    "length",
    "uncompressed_length",
)


@dataclass(frozen=True)
class MetadataProjection:
    snapshot_id: str
    publication_count: int
    bucket_count: int
    parts: tuple[dict[str, Any], ...]
    manifest_key: str
    manifest_sha256: str
    manifest_bytes: int

    def as_dict(self) -> dict[str, Any]:
        return {
            "schema_version": 1,
            "snapshot_id": self.snapshot_id,
            "publication_count": self.publication_count,
            "bucket_count": self.bucket_count,
            "parts": list(self.parts),
        }


def _require_pyarrow():
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError as exc:  # pragma: no cover - optional deployment dependency
        raise RuntimeError(
            "install pyarrow to build Parquet metadata projections"
        ) from exc
    return pa, pq


def _validate_digest(value: Any, prefix: str, name: str, hex_length: int = 64) -> str:
    text = str(value or "")
    if not text.startswith(prefix):
        raise MetadataProjectionError(
            f"{name} must use {prefix}<{hex_length} lowercase hex>"
        )
    digest = text[len(prefix) :]
    if len(digest) != hex_length or any(char not in "0123456789abcdef" for char in digest):
        raise MetadataProjectionError(
            f"{name} must use {prefix}<{hex_length} lowercase hex>"
        )
    return text


def _normalise_row(raw: Any, line_number: int) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise MetadataProjectionError(f"line {line_number}: document is not an object")
    document = raw.get("document", raw)
    if not isinstance(document, dict):
        raise MetadataProjectionError(f"line {line_number}: document is not an object")
    missing = [name for name in _REQUIRED if name not in document]
    if missing:
        raise MetadataProjectionError(
            f"line {line_number}: missing {', '.join(missing)}"
        )
    publication_uid = str(document["publication_uid"] or "")
    if not publication_uid:
        raise MetadataProjectionError(f"line {line_number}: empty publication_uid")
    _validate_digest(document["source_generation"], "sha256:", "source_generation")
    _validate_digest(document["source_pack"], "sha256:", "source_pack")
    _validate_digest(document["source_revision"], "sha256:", "source_revision")
    _validate_digest(
        document["source_fingerprint"],
        "blake2b-128:",
        "source_fingerprint",
        hex_length=32,
    )
    row: dict[str, Any] = {name: str(document.get(name) or "") for name in _TEXT_COLUMNS}
    for name in _LIST_COLUMNS:
        value = document.get(name, [])
        if value is None:
            value = []
        if not isinstance(value, list) or any(not isinstance(item, str) for item in value):
            raise MetadataProjectionError(f"line {line_number}: {name} must be a string array")
        row[name] = list(dict.fromkeys(value))
    for name in ("offset", "length", "uncompressed_length"):
        try:
            number = int(document[name])
        except (TypeError, ValueError) as exc:
            raise MetadataProjectionError(f"line {line_number}: {name} is not an integer") from exc
        if number < 0 or (name != "offset" and number == 0):
            raise MetadataProjectionError(f"line {line_number}: invalid {name}")
        row[name] = number
    if row["source_pack_key"].startswith("/") or ".." in row["source_pack_key"].split("/"):
        raise MetadataProjectionError(f"line {line_number}: unsafe source_pack_key")
    return row


def _schema(pa):
    return pa.schema(
        [
            *[(name, pa.string()) for name in _TEXT_COLUMNS],
            *[(name, pa.list_(pa.string())) for name in _LIST_COLUMNS],
            ("offset", pa.uint64()),
            ("length", pa.uint64()),
            ("uncompressed_length", pa.uint64()),
        ]
    )


def _artifact(path: Path, key: str, rows: int, **extra: Any) -> dict[str, Any]:
    value = {"key": key, "sha256": _sha256_file(path), "bytes": path.stat().st_size, "rows": rows}
    value.update(extra)
    return value


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _write_part(path: Path, rows: Iterator[dict[str, Any]], schema, pq, pa, batch_size: int) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.partial")
    count = 0
    writer = None
    try:
        batch: list[dict[str, Any]] = []
        for row in rows:
            batch.append(row)
            if len(batch) < batch_size:
                continue
            table = pa.Table.from_pylist(batch, schema=schema)
            if writer is None:
                writer = pq.ParquetWriter(
                    temporary,
                    schema,
                    compression="zstd",
                    use_dictionary=True,
                    write_statistics=True,
                )
            writer.write_table(table)
            count += len(batch)
            batch = []
        if batch:
            table = pa.Table.from_pylist(batch, schema=schema)
            if writer is None:
                writer = pq.ParquetWriter(
                    temporary,
                    schema,
                    compression="zstd",
                    use_dictionary=True,
                    write_statistics=True,
                )
            writer.write_table(table)
            count += len(batch)
        if writer is None:
            return 0
        writer.close()
        if path.exists():
            if _sha256_file(path) != _sha256_file(temporary):
                raise MetadataProjectionError(f"immutable Parquet conflict: {path}")
            temporary.unlink(missing_ok=True)
        else:
            temporary.replace(path)
        return count
    finally:
        if writer is not None:
            try:
                writer.close()
            except Exception:
                pass
        temporary.unlink(missing_ok=True)


def project_ndjson_to_parquet(
    input_path: str | Path,
    output_root: str | Path,
    snapshot_id: str,
    *,
    bucket_count: int = 256,
    batch_size: int = 10_000,
    analytics_rows_per_part: int = 250_000,
) -> MetadataProjection:
    """Build point-lookup and analytics Parquet projections.

    Input rows are staged in a temporary SQLite database to keep duplicate
    detection and key ordering bounded by disk rather than Python heap.  The
    final files are written atomically and are safe to rerun only when their
    bytes are identical.
    """

    if not snapshot_id or "/" in snapshot_id or ".." in snapshot_id.split("/"):
        raise MetadataProjectionError("snapshot_id must be path-safe")
    if bucket_count < 1 or bucket_count > 4096 or bucket_count & (bucket_count - 1):
        raise MetadataProjectionError("bucket_count must be a power of two between 1 and 4096")
    if batch_size < 1:
        raise MetadataProjectionError("batch_size must be positive")
    if analytics_rows_per_part < 1:
        raise MetadataProjectionError("analytics_rows_per_part must be positive")
    pa, pq = _require_pyarrow()
    schema = _schema(pa)
    root = Path(output_root)
    root.mkdir(parents=True, exist_ok=True)
    snapshot_root = root / "metadata" / "v1" / snapshot_id
    snapshot_root.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="metadata-stage-", dir=root) as temporary_dir:
        database = Path(temporary_dir) / "rows.sqlite"
        connection = sqlite3.connect(database)
        try:
            connection.execute(
                "CREATE TABLE rows (publication_uid TEXT PRIMARY KEY, bucket INTEGER NOT NULL, payload TEXT NOT NULL)"
            )
            count = 0
            with Path(input_path).open(encoding="utf-8") as handle:
                for line_number, line in enumerate(handle, 1):
                    if not line.strip():
                        continue
                    row = _normalise_row(json.loads(line), line_number)
                    digest = hashlib.sha256(row["publication_uid"].encode()).digest()
                    bucket = int.from_bytes(digest[:2], "big") & (bucket_count - 1)
                    try:
                        connection.execute(
                            "INSERT INTO rows VALUES (?, ?, ?)",
                            (row["publication_uid"], bucket, json.dumps(row, separators=(",", ":"))),
                        )
                    except sqlite3.IntegrityError as exc:
                        raise MetadataProjectionError(
                            f"duplicate publication_uid: {row['publication_uid']}"
                        ) from exc
                    count += 1
                    if count % 10_000 == 0:
                        connection.commit()
            connection.commit()
            connection.execute("CREATE INDEX rows_bucket_uid ON rows (bucket, publication_uid)")

            parts: list[dict[str, Any]] = []
            for bucket in range(bucket_count):
                rows = (
                    json.loads(payload)
                    for (payload,) in connection.execute(
                        "SELECT payload FROM rows WHERE bucket = ? ORDER BY publication_uid",
                        (bucket,),
                    )
                )
                bucket_width = len(f"{bucket_count - 1:X}")
                path = snapshot_root / "point" / "publications" / f"bucket={bucket:0{bucket_width}x}" / "part-00000.parquet"
                written = _write_part(path, rows, schema, pq, pa, batch_size)
                if written:
                    parts.append(
                        _artifact(
                            path,
                            path.relative_to(root).as_posix(),
                            written,
                            projection="point",
                            table="publications",
                            bucket=bucket,
                        )
                    )

            cursor = connection.execute("SELECT payload FROM rows ORDER BY publication_uid")
            analytics_part = 0
            while True:
                records = cursor.fetchmany(analytics_rows_per_part)
                if not records:
                    break
                analytics_path = (
                    snapshot_root / "analytics" / "publications"
                    / f"part-{analytics_part:05d}.parquet"
                )
                written = _write_part(
                    analytics_path,
                    (json.loads(payload) for (payload,) in records),
                    schema,
                    pq,
                    pa,
                    batch_size,
                )
                parts.append(
                    _artifact(
                        analytics_path,
                        analytics_path.relative_to(root).as_posix(),
                        written,
                        projection="analytics",
                        table="publications",
                    )
                )
                analytics_part += 1
        finally:
            connection.close()

    manifest = {
        "schema_version": 1,
        "snapshot_id": snapshot_id,
        "publication_count": count,
        "bucket_count": bucket_count,
        "schema": [field.name + ":" + str(field.type) for field in schema],
        "parts": parts,
    }
    manifest_path = snapshot_root / "manifest.json"
    manifest_bytes = canonical_json(manifest)
    if manifest_path.exists() and manifest_path.read_bytes() != manifest_bytes:
        raise MetadataProjectionError(f"immutable metadata manifest conflict: {manifest_path}")
    if not manifest_path.exists():
        temporary_manifest = manifest_path.with_name(f".{manifest_path.name}.partial")
        temporary_manifest.write_bytes(manifest_bytes)
        temporary_manifest.replace(manifest_path)
    return MetadataProjection(
        snapshot_id=snapshot_id,
        publication_count=count,
        bucket_count=bucket_count,
        parts=tuple(parts),
        manifest_key=manifest_path.relative_to(root).as_posix(),
        manifest_sha256=hashlib.sha256(manifest_bytes).hexdigest(),
        manifest_bytes=len(manifest_bytes),
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input_ndjson", type=Path)
    parser.add_argument("output_root", type=Path)
    parser.add_argument("snapshot_id")
    parser.add_argument("--bucket-count", type=int, default=256)
    parser.add_argument("--batch-size", type=int, default=10_000)
    args = parser.parse_args()
    projection = project_ndjson_to_parquet(
        args.input_ndjson,
        args.output_root,
        args.snapshot_id,
        bucket_count=args.bucket_count,
        batch_size=args.batch_size,
    )
    print(json.dumps(projection.as_dict(), sort_keys=True))
    return 0


if __name__ == "__main__":  # pragma: no cover - exercised by deployment image
    raise SystemExit(main())
