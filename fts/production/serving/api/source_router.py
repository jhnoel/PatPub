"""Release-aware FTS gateway with exact source-pack range hydration.

The gateway resolves ``releases/current.json`` for every request, routes the
query to the index named by that immutable manifest, and hydrates each hit
from its validated source-pack byte range.  It is intentionally stateless;
the release selector is the only mutable control-plane object.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import re
import threading
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib import error as urlerror
from urllib import request as urlrequest

from ingestion.release import AzureBlobReleaseStore, ReleaseError, ReleasePublisher
from ingestion.search_query import generation_filter
from .snippets import sections_from_canonical_markdown, snippets_for_document


MAX_REQUEST_BYTES = 1 * 1024 * 1024
MAX_HITS = 100
MAX_SOURCE_BYTES = 64 * 1024 * 1024
MAX_TOTAL_SOURCE_BYTES = 128 * 1024 * 1024
OPENAPI_SPEC_PATH = Path(__file__).with_name("openapi.json")
DEFAULT_UPSTREAM_TIMEOUT_SECONDS = 65.0


class SearchQueryError(ValueError):
    """A query the caller can correct, rather than an upstream outage."""


def validate_search_query(query: str) -> None:
    # Skip quoted phrases and escaped characters: text mentioning an inventor
    # field is not itself a fielded query. Inventors are not independently
    # indexed; applicant sometimes contains inventor names, but is not an alias.
    for token in re.finditer(r'"(?:\\.|[^"\\])*"|\\.|\b(inventor)\s*:', query, re.I):
        if token.group(1):
            raise SearchQueryError(
                "The inventor field is not indexed. Use applicant:Vaswani to "
                "search applicant names (some records include inventor names), "
                "or search a name in the patent text. This is not a complete "
                "inventor search."
            )


def search_http_error(exc: urlerror.HTTPError) -> Exception:
    if exc.code == 400:
        try:
            detail = json.loads(exc.read(8192)).get("message", "")
        except (ValueError, AttributeError):
            detail = ""
        return SearchQueryError(f"Invalid search query: {detail or 'check the query syntax and indexed field names'}")
    return ReleaseError(f"search upstream failed: HTTP {exc.code}")


def _required(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(f"{name} is required")
    return value


class Router:
    def __init__(self) -> None:
        account = _required("AZURE_STORAGE_ACCOUNT")
        self.search_url = _required("QUICKWIT_URL").rstrip("/")
        self.release_container = os.environ.get("RELEASE_CONTAINER", "releases")
        self.source_container = os.environ.get("SOURCE_CONTAINER", "source")
        # Quickwit permits 60s searches. Do not abandon those requests at 30s.
        self.timeout = float(os.environ.get("UPSTREAM_TIMEOUT_SECONDS", str(DEFAULT_UPSTREAM_TIMEOUT_SECONDS)))
        self.max_hydration_bytes = int(
            os.environ.get("MAX_TOTAL_HYDRATION_BYTES", str(MAX_TOTAL_SOURCE_BYTES))
        )
        if self.max_hydration_bytes < 1 or self.max_hydration_bytes > 1 << 30:
            raise RuntimeError("MAX_TOTAL_HYDRATION_BYTES must be between 1 and 1GiB")
        try:
            from azure.identity import DefaultAzureCredential
            from azure.storage.blob import BlobServiceClient
        except ImportError as exc:  # pragma: no cover - deployment-only path
            raise RuntimeError("install the azure optional dependency") from exc
        self.service = BlobServiceClient(
            f"https://{account}.blob.core.windows.net",
            credential=DefaultAzureCredential(),
        )
        self.releases = AzureBlobReleaseStore(
            f"https://{account}.blob.core.windows.net",
            self.release_container,
        )
        self.publisher = ReleasePublisher(self.releases)
        self._validated_selector_sha256: str | None = None
        self._validated_active: tuple[dict[str, Any], Any, str] | None = None
        self._source_pairs_cache: dict[str, dict[str, set[tuple[str, str]]]] = {}
        self._release_lock = threading.RLock()

    def _active(self):
        selector_bytes = self.releases.read("releases/current.json")
        selector_sha256 = hashlib.sha256(selector_bytes).hexdigest()
        with self._release_lock:
            if (
                self._validated_selector_sha256 == selector_sha256
                and self._validated_active is not None
            ):
                return self._validated_active
            selector = json.loads(selector_bytes)
            # Publication verifies and hashes the large immutable artifacts.
            # A serving request checks the selected manifest's digest, then
            # verifies only the source frames it actually hydrates. Re-reading
            # every split and pack here makes cold search O(corpus size).
            manifest = self.publisher.verify_selector(selector, validate_artifacts=False)
            if manifest.fulltext.get("index_id") is None and not manifest.fulltext.get("shards"):
                raise ReleaseError("active release has no fulltext search shard")
            self._validated_selector_sha256 = selector_sha256
            self._validated_active = (selector, manifest, selector_sha256)
            self._source_pairs_cache = {}
            return self._validated_active

    def _source_pairs(self, manifest, generation: str | None = None) -> dict[str, set[tuple[str, str]]]:
        """Return source-pack identities grouped by their generation.

        A release can contain several correction generations. Keeping the
        generation in this binding prevents a valid pack from one generation
        being paired with a hit that claims another generation.
        """

        cache = getattr(self, "_source_pairs_cache", {})
        cache_key = getattr(manifest, "digest", repr(manifest.source))
        cached = cache.get(cache_key)
        if cached is not None and (generation is not None and generation in cached):
            return cached
        source = manifest.source
        packs = source.get("source_packs") or [source.get("source_pack")]
        all_pairs = {
            (f"sha256:{item.get('sha256')}", str(item.get("key")))
            for item in packs
            if isinstance(item, dict)
        }
        refs = source.get("generation_manifests")
        if refs is None:
            refs = []
        elif isinstance(refs, dict):
            refs = [
                {"generation_id": generation, "artifact": artifact}
                for generation, artifact in refs.items()
            ]
        pairs_by_generation: dict[str, set[tuple[str, str]]] = dict(cached or {})
        if refs and hasattr(self, "releases"):
            for ref in refs:
                artifact = ref.get("artifact") if isinstance(ref, dict) else None
                if not isinstance(artifact, dict):
                    continue
                ref_generation = str(ref.get("generation_id") or "")
                if not ref_generation or ref_generation in pairs_by_generation:
                    continue
                if generation is not None and ref_generation != generation:
                    continue
                raw = self.releases.read(str(artifact["key"]))
                if (
                    len(raw) != int(artifact["bytes"])
                    or hashlib.sha256(raw).hexdigest() != artifact["sha256"]
                ):
                    raise ReleaseError("source generation manifest digest mismatch")
                payload = json.loads(raw)
                if payload.get("generation_id") != ref_generation:
                    raise ReleaseError("source generation manifest identity mismatch")
                generation_pairs = pairs_by_generation.setdefault(ref_generation, set())
                for item in payload.get("source_packs", []):
                    if isinstance(item, dict):
                        generation_pairs.add(
                            (f"sha256:{item.get('sha256')}", str(item.get("key")))
                        )
        if not pairs_by_generation and not refs:
            # A single-generation release predating generation_manifests is
            # still safe because ReleasePublisher validated its source pack.
            if len(manifest.source_generations) != 1:
                raise ReleaseError("active release has no generation-bound source packs")
            pairs_by_generation[str(manifest.source_generations[0])] = all_pairs
        if (not pairs_by_generation or
                any(not pairs or not pairs <= all_pairs for pairs in pairs_by_generation.values())):
            raise ReleaseError("active release has no source pack identities")
        cache[cache_key] = pairs_by_generation
        self._source_pairs_cache = cache
        return pairs_by_generation

    @staticmethod
    def _range_bytes(downloader, offset: int, length: int) -> bytes:
        """Read a requested Blob range and validate response metadata when exposed."""

        properties = getattr(downloader, "properties", None) or {}
        response = getattr(downloader, "_response", None)
        headers = getattr(response, "headers", None) or {}
        status = getattr(downloader, "status_code", None)
        if status is None:
            status = getattr(response, "status_code", None)
        if status is not None and status != 206:
            raise ReleaseError(f"source range returned HTTP {status}, expected 206")
        content_range = properties.get("content_range") or properties.get("content-range")
        content_range = content_range or headers.get("content-range") or headers.get("Content-Range")
        if content_range:
            text = str(content_range)
            if not text.startswith("bytes ") or "/" not in text:
                raise ReleaseError("source range returned invalid Content-Range")
            span = text[6:].split("/", 1)[0]
            try:
                start_text, end_text = span.split("-", 1)
                start, end = int(start_text), int(end_text)
            except (ValueError, TypeError) as exc:
                raise ReleaseError("source range returned invalid Content-Range") from exc
            if start != offset or end != offset + length - 1:
                raise ReleaseError("source range returned an unexpected Content-Range")
        value = downloader.readall()
        if len(value) != length:
            raise ReleaseError(f"source range returned {len(value)} bytes, expected {length}")
        return value

    def _hydrate(self, hit: dict[str, Any], manifest) -> str:
        required = (
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
        if any(name not in hit for name in required):
            raise ReleaseError("search hit is missing a source pointer field")
        source_artifacts = manifest.source.get("source_packs")
        if source_artifacts is None:
            source_artifacts = [manifest.source.get("source_pack")]
        generation = str(hit["source_generation"])
        pair = (str(hit["source_pack"]), str(hit["source_pack_key"]))
        pairs_by_generation = self._source_pairs(manifest, generation=generation)
        if pair not in pairs_by_generation.get(generation, set()):
            raise ReleaseError(
                "search hit points outside the active release generation source pack"
            )
        expected_sha = pair[0].removeprefix("sha256:")
        expected_key = pair[1]
        if generation not in manifest.source_generations:
            raise ReleaseError("search hit points outside the active source generation")
        try:
            offset = int(hit["offset"])
            length = int(hit["length"])
            output_length = int(hit["uncompressed_length"])
        except (TypeError, ValueError) as exc:
            raise ReleaseError("search hit has invalid source range") from exc
        if (
            offset < 0
            or length <= 0
            or output_length <= 0
            or length > MAX_SOURCE_BYTES
            or output_length > MAX_SOURCE_BYTES
        ):
            raise ReleaseError("search hit source range exceeds safety limits")
        if not expected_key.startswith(f"{self.source_container}/"):
            raise ReleaseError("source pack must use the configured source container")
        blob_key = expected_key.split("/", 1)[1]
        blob = self.service.get_blob_client(self.source_container, blob_key)
        compressed = self._range_bytes(blob.download_blob(offset=offset, length=length), offset, length)
        # The Rust producer uses Blake2b-512 and truncates the first 128 bits;
        # hashlib's digest_size=16 is a different BLAKE2 parameterization.
        fingerprint = hashlib.blake2b(compressed, digest_size=64).hexdigest()[:32]
        if hit["source_fingerprint"] != f"blake2b-128:{fingerprint}":
            raise ReleaseError("source frame fingerprint mismatch")
        try:
            import zstandard

            markdown = zstandard.ZstdDecompressor().decompress(
                compressed, max_output_size=output_length
            )
        except Exception as exc:  # pragma: no cover - deployment-only path
            raise ReleaseError(f"source frame decompression failed: {exc}") from exc
        if len(markdown) != output_length:
            raise ReleaseError("source frame decompressed size mismatch")
        revision = str(hit["source_revision"])
        if not revision.startswith("sha256:") or len(revision) != len("sha256:") + 64:
            raise ReleaseError("search hit has invalid source revision")
        if hashlib.sha256(markdown).hexdigest() != revision[len("sha256:") :]:
            raise ReleaseError("canonical source revision mismatch")
        text = markdown.decode("utf-8")
        identity = f"<!-- publication_uid: {hit['publication_uid']} -->"
        first_line = text.splitlines()[0] if text.splitlines() else ""
        if first_line != identity:
            raise ReleaseError("canonical source publication identity mismatch")
        return text

    def search(self, body: bytes) -> dict[str, Any]:
        try:
            query = json.loads(body)
        except json.JSONDecodeError as exc:
            raise ReleaseError(f"request body is not JSON: {exc}") from exc
        if not isinstance(query, dict):
            raise ReleaseError("request body must be a JSON object")
        original_query = str(query.get("query") or "").strip()
        if not original_query:
            raise SearchQueryError("query is required")
        validate_search_query(original_query)
        selector, manifest, selector_sha256 = self._active()
        query.pop("index_id", None)
        try:
            max_hits = int(query.get("max_hits", 10))
        except (TypeError, ValueError) as exc:
            raise ReleaseError("max_hits must be an integer") from exc
        if max_hits < 1:
            raise ReleaseError("max_hits must be positive")
        query["max_hits"] = min(max_hits, MAX_HITS)
        generations = [
            generation for generation in manifest.source_generations
            if generation not in set(manifest.source.get("superseded_generation_ids", []))
        ]
        if not generations:
            raise ReleaseError("active release has no searchable source generations")
        selected_generations = generation_filter(generations, manifest.source.get("selected_publication_uids"))
        query["query"] = f"({original_query}) AND ({selected_generations})"
        try:
            start_offset = int(query.get("start_offset", 0))
        except (TypeError, ValueError) as exc:
            raise ReleaseError("start_offset must be an integer") from exc
        if start_offset < 0 or start_offset > 900:
            raise ReleaseError("start_offset must be between 0 and 900")
        shards = manifest.fulltext.get("shards")
        if shards is None:
            index_ids = [str(manifest.fulltext["index_id"])]
        else:
            if not isinstance(shards, list) or not shards:
                raise ReleaseError("active release has no search shards")
            index_ids = [str(item["index_id"]) for item in shards]
        if len(set(index_ids)) != len(index_ids) or any(
            not re.fullmatch(r"[a-z0-9][a-z0-9_-]{2,100}", item) for item in index_ids
        ):
            raise ReleaseError("active release has invalid search shard IDs")
        # Quickwit 0.9 supports explicit comma-separated multi-index targets.
        # Never use a wildcard: it could expose an unvalidated candidate index.
        groups: list[list[str]] = []
        for index_id in index_ids:
            if not groups or len(",".join(groups[-1] + [index_id])) > 3000:
                groups.append([])
            groups[-1].append(index_id)
        upstream_query = dict(query)
        if len(groups) > 1:
            upstream_query["start_offset"] = 0
            upstream_query["max_hits"] = start_offset + query["max_hits"]
        responses = []
        for group in groups:
            target = f"{self.search_url}/api/v1/{','.join(group)}/search"
            request = urlrequest.Request(
                target,
                data=json.dumps(upstream_query, separators=(",", ":")).encode(),
                headers={"content-type": "application/json"},
                method="POST",
            )
            try:
                with urlrequest.urlopen(request, timeout=self.timeout) as response:
                    responses.append(json.loads(response.read()))
            except urlerror.HTTPError as exc:
                raise search_http_error(exc) from exc
            except TimeoutError as exc:
                raise ReleaseError(
                    f"Search timed out after {self.timeout:g} seconds. Narrow the query "
                    "with title:, abstract:, claims:, or an assignee filter and retry."
                ) from exc
            except (urlerror.URLError, json.JSONDecodeError) as exc:
                raise ReleaseError(f"search upstream failed: {exc}") from exc
        if len(responses) == 1:
            result = responses[0]
        else:
            # Each request searches a disjoint, explicitly selected set of
            # shards. Merge returned hits by score and aggregate match counts.
            hits_by_group = [item.get("hits", []) for item in responses]
            if any(not isinstance(hits, list) for hits in hits_by_group):
                raise ReleaseError("search upstream returned invalid hits")
            all_hits = [hit for hits in hits_by_group for hit in hits]
            all_hits.sort(key=lambda hit: float(hit.get("_score", 0)), reverse=True)
            result = {
                "hits": all_hits[start_offset:start_offset + query["max_hits"]],
                "num_hits": sum(int(item.get("num_hits", 0)) for item in responses),
                "elapsed_time_micros": sum(int(item.get("elapsed_time_micros", 0)) for item in responses),
            }
        hits = result.get("hits", [])
        if not isinstance(hits, list):
            raise ReleaseError("search upstream returned invalid hits")
        if len(hits) > query["max_hits"]:
            raise ReleaseError("search upstream returned more hits than requested")
        requested_bytes = 0
        for hit in hits:
            if not isinstance(hit, dict):
                raise ReleaseError("search upstream returned an invalid hit")
            try:
                requested_bytes += int(hit.get("length", 0)) + int(
                    hit.get("uncompressed_length", 0)
                )
            except (TypeError, ValueError) as exc:
                raise ReleaseError("search upstream returned invalid source lengths") from exc
            if requested_bytes > self.max_hydration_bytes:
                raise ReleaseError("search response exceeds the hydration budget")
            canonical = self._hydrate(hit, manifest)
            sections = sections_from_canonical_markdown(canonical)
            hit["snippets"] = (
                snippets_for_document(original_query, sections)
                if sections is not None
                else []
            )
            hit["active_release_id"] = manifest.release_id
        result["active_release_id"] = manifest.release_id
        result["active_selector_sha256"] = selector_sha256
        result["active_manifest_sha256"] = selector["manifest_sha256"]
        return result

    def candidate_search(self, index_id: str, body: bytes) -> dict[str, Any]:
        """Query a candidate index for the internal pre-activation canary."""
        if not re.fullmatch(r"[a-z0-9][a-z0-9_-]{2,100}", index_id):
            raise ReleaseError("invalid candidate index ID")
        query = json.loads(body)
        if not isinstance(query, dict):
            raise ReleaseError("candidate query must be a JSON object")
        query["max_hits"] = min(max(int(query.get("max_hits", 1)), 1), MAX_HITS)
        target = f"{self.search_url}/api/v1/{index_id}/search"
        req = urlrequest.Request(
            target,
            data=json.dumps(query, separators=(",", ":")).encode(),
            headers={"content-type": "application/json"},
            method="POST",
        )
        try:
            with urlrequest.urlopen(req, timeout=self.timeout) as response:
                return json.loads(response.read())
        except (urlerror.URLError, TimeoutError, json.JSONDecodeError) as exc:
            raise ReleaseError(f"candidate upstream failed: {exc}") from exc


class Handler(BaseHTTPRequestHandler):
    router: Router

    def _json(self, status: int, value: Any) -> None:
        payload = json.dumps(value, separators=(",", ":")).encode()
        self.send_response(status)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def do_GET(self) -> None:  # noqa: N802
        if self.path == "/openapi.json":
            expected_token = os.environ.get("ROUTER_BEARER_TOKEN", "").strip()
            supplied = self.headers.get("authorization", "")
            if not expected_token or not hmac.compare_digest(
                supplied.encode(), f"Bearer {expected_token}".encode()
            ):
                self._json(HTTPStatus.UNAUTHORIZED, {"error": "unauthorized"})
                return
            payload = OPENAPI_SPEC_PATH.read_bytes()
            self.send_response(HTTPStatus.OK)
            self.send_header("content-type", "application/json; charset=utf-8")
            self.send_header("content-length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
        elif self.path == "/healthz":
            self._json(HTTPStatus.OK, {"status": "ok"})
        elif self.path == "/readyz":
            try:
                # The metastore listing can work before Quickwit's search
                # service is ready. Use its own readiness endpoint on cold
                # start, without running a corpus search for every probe.
                with urlrequest.urlopen(f"{self.router.search_url}/health/readyz", timeout=5) as response:
                    if json.loads(response.read()) is not True:
                        raise RuntimeError("Quickwit is not ready")
                self._json(HTTPStatus.OK, {"status": "ready"})
            except Exception:
                self._json(HTTPStatus.SERVICE_UNAVAILABLE, {"status": "starting"})
        else:
            self._json(HTTPStatus.NOT_FOUND, {"error": "not found"})

    def do_POST(self) -> None:  # noqa: N802
        candidate = re.fullmatch(r"/api/v1/([a-z0-9][a-z0-9_-]{2,100})/search", self.path)
        if self.path != "/api/search" and candidate is None:
            self._json(HTTPStatus.NOT_FOUND, {"error": "not found"})
            return
        expected_token = os.environ.get("ROUTER_BEARER_TOKEN", "").strip()
        if expected_token:
            supplied = self.headers.get("authorization", "")
            if not hmac.compare_digest(
                supplied.encode(), f"Bearer {expected_token}".encode()
            ):
                self._json(HTTPStatus.UNAUTHORIZED, {"error": "unauthorized"})
                return
        try:
            length = int(self.headers.get("content-length", "-1"))
        except ValueError:
            length = -1
        if length < 0 or length > MAX_REQUEST_BYTES:
            self._json(HTTPStatus.REQUEST_ENTITY_TOO_LARGE, {"error": "request too large"})
            return
        try:
            body = self.rfile.read(length)
            result = (
                self.router.candidate_search(candidate.group(1), body)
                if candidate else self.router.search(body)
            )
            self._json(HTTPStatus.OK, result)
        except SearchQueryError as exc:
            self._json(HTTPStatus.BAD_REQUEST, {"error": str(exc)})
        except (ReleaseError, ValueError, KeyError) as exc:
            self._json(HTTPStatus.BAD_GATEWAY, {"error": str(exc)})
        except Exception as exc:  # pragma: no cover - deployment failure boundary
            self._json(HTTPStatus.INTERNAL_SERVER_ERROR, {"error": str(exc)})

    def log_message(self, format: str, *args: Any) -> None:
        print("router", format % args, flush=True)


def main() -> int:
    host = os.environ.get("LISTEN_HOST", "0.0.0.0")
    port = int(os.environ.get("PORT", "8080"))
    Handler.router = Router()
    server = ThreadingHTTPServer((host, port), Handler)
    print(f"source_router_listening={host}:{port}", flush=True)
    server.serve_forever()
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
