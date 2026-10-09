from __future__ import annotations

import hashlib
import json
from types import SimpleNamespace
import threading
from io import BytesIO
from urllib.error import HTTPError

import pytest

from ingestion.release import ReleaseError
from fts.production.serving.api.source_router import Router
from ingestion.models import canonical_json
from fts.production.serving.api import source_router


@pytest.mark.parametrize("query", ["inventor:Vaswani", "Inventor : (Vaswani OR Smith)"])
def test_inventor_query_explains_missing_index_without_reading_release(query):
    router = Router.__new__(Router)
    router._active = lambda: pytest.fail("unsupported fields should fail before release access")
    with pytest.raises(source_router.SearchQueryError, match="not a complete inventor search"):
        router.search(json.dumps({"query": query}).encode())


@pytest.mark.parametrize("query", ['"inventor:Vaswani"', r'inventor\:Vaswani', 'applicant:Vaswani'])
def test_inventor_mentions_and_applicant_queries_are_allowed(query):
    source_router.validate_search_query(query)


def test_upstream_bad_query_preserves_actionable_message():
    exc = HTTPError("http://quickwit", 400, "Bad Request", {}, BytesIO(
        b'{"message":"Field does not exist: inventro"}'
    ))
    error = source_router.search_http_error(exc)
    assert isinstance(error, source_router.SearchQueryError)
    assert "Field does not exist: inventro" in str(error)


def test_upstream_outage_stays_a_release_error():
    exc = HTTPError("http://quickwit", 503, "Unavailable", {}, BytesIO(b"unavailable"))
    assert isinstance(source_router.search_http_error(exc), ReleaseError)


def test_router_timeout_exceeds_search_engine_budget():
    config = (source_router.OPENAPI_SPEC_PATH.parent.parent / "searcher-node.yaml").read_text()
    import re
    budgets = re.findall(r"(?:leaf_)?request_timeout_secs: (\d+)", config)
    assert budgets
    assert source_router.DEFAULT_UPSTREAM_TIMEOUT_SECONDS > max(map(int, budgets))


class _Downloader:
    def __init__(self, value: bytes, *, status_code: int | None = None, content_range: str | None = None):
        self.value = value
        self.status_code = status_code
        self.properties = {"content_range": content_range} if content_range else {}

    def readall(self) -> bytes:
        return self.value


class _Blob:
    def __init__(self, value: bytes, *, status_code: int | None = None, content_range: str | None = None):
        self.value = value
        self.status_code = status_code
        self.content_range = content_range

    def download_blob(self, *, offset: int, length: int) -> _Downloader:
        return _Downloader(
            self.value[offset : offset + length],
            status_code=self.status_code,
            content_range=self.content_range,
        )


class _Service:
    def __init__(self, value: bytes):
        self.value = value

    def get_blob_client(self, container: str, blob: str) -> _Blob:
        assert container == "source"
        assert blob == "v1/packs/test.zst"
        return _Blob(self.value)


@pytest.mark.parametrize("quickwit_ready", [True, False, {"status": "starting"}])
def test_readiness_uses_quickwits_service_health(monkeypatch, quickwit_ready):
    calls = []

    class Response:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def read(self):
            return json.dumps(quickwit_ready).encode()

    def urlopen(url, **_kwargs):
        calls.append(url)
        return Response()

    monkeypatch.setattr(source_router.urlrequest, "urlopen", urlopen)
    handler = source_router.Handler.__new__(source_router.Handler)
    handler.path = "/readyz"
    handler.router = SimpleNamespace(search_url="http://quickwit")
    statuses = []
    handler._json = lambda status, _body: statuses.append(status)
    handler.do_GET()
    assert calls == ["http://quickwit/health/readyz"]
    assert statuses == [200 if quickwit_ready is True else 503]


def test_source_binding_loads_only_the_requested_generation():
    generations = ["sha256:" + char * 64 for char in ("a", "b")]
    packs = [{"key": f"source/v1/packs/{char}.zst", "sha256": char * 64, "bytes": 1}
             for char in ("a", "b")]
    payloads = [canonical_json({"generation_id": generation, "source_packs": [pack]})
                for generation, pack in zip(generations, packs)]
    refs = [{"generation_id": generation, "artifact": {
        "key": f"source/gen-{index}.json", "sha256": hashlib.sha256(payload).hexdigest(),
        "bytes": len(payload),
    }} for index, (generation, payload) in enumerate(zip(generations, payloads))]
    reads = []

    class Store:
        def read(self, key):
            reads.append(key)
            return payloads[int(key.split("gen-")[1].split(".")[0])]

    router = Router.__new__(Router)
    router.releases = Store()
    router._source_pairs_cache = {}
    manifest = SimpleNamespace(source={"source_packs": packs, "generation_manifests": refs},
                               source_generations=generations)
    assert generations[0] in router._source_pairs(manifest, generation=generations[0])
    assert reads == ["source/gen-0.json"]
    router._source_pairs(manifest, generation=generations[0])
    assert reads == ["source/gen-0.json"]
    assert len(router._source_pairs(manifest)) == 2
    assert reads == ["source/gen-0.json", "source/gen-1.json"]


def test_router_hydrates_a_matching_source_pack_frame():
    zstandard = pytest.importorskip("zstandard")
    text = "<!-- publication_uid: US:1:B2 -->\n# Fixture\n"
    compressed = zstandard.ZstdCompressor(level=9).compress(text.encode())
    pack_sha = hashlib.sha256(compressed).hexdigest()
    frame_fingerprint = hashlib.blake2b(compressed, digest_size=64).hexdigest()[:32]
    hit = {
        "publication_uid": "US:1:B2",
        "source_generation": "sha256:" + "a" * 64,
        "source_pack": "sha256:" + pack_sha,
        "source_pack_key": "source/v1/packs/test.zst",
        "source_revision": "sha256:" + hashlib.sha256(text.encode()).hexdigest(),
        "source_fingerprint": "blake2b-128:" + frame_fingerprint,
        "offset": 0,
        "length": len(compressed),
        "uncompressed_length": len(text.encode()),
    }
    router = Router.__new__(Router)
    router.source_container = "source"
    router.service = _Service(compressed)
    manifest = SimpleNamespace(
        source={
            "source_pack": {
                "key": "source/v1/packs/test.zst",
                "sha256": pack_sha,
                "bytes": len(compressed),
            }
        },
        source_generations=("sha256:" + "a" * 64,),
    )

    assert router._hydrate(hit, manifest) == text

    hit["source_pack_key"] = "source/v1/packs/other.zst"
    try:
        router._hydrate(hit, manifest)
    except ReleaseError as exc:
        assert "outside" in str(exc)
    else:  # pragma: no cover
        raise AssertionError("router accepted a source pack outside the release")


def test_router_rejects_bad_range_metadata_and_supports_nonzero_offsets():
    zstandard = pytest.importorskip("zstandard")
    text = "<!-- publication_uid: US:2:B2 -->\n# Second\n"
    compressed = zstandard.ZstdCompressor(level=9).compress(text.encode())
    prefix = b"pad!"
    pack = prefix + compressed
    pack_sha = hashlib.sha256(pack).hexdigest()
    frame_fingerprint = hashlib.blake2b(compressed, digest_size=64).hexdigest()[:32]
    hit = {
        "publication_uid": "US:2:B2",
        "source_generation": "sha256:" + "b" * 64,
        "source_pack": "sha256:" + pack_sha,
        "source_pack_key": "source/v1/packs/second.zst",
        "source_revision": "sha256:" + hashlib.sha256(text.encode()).hexdigest(),
        "source_fingerprint": "blake2b-128:" + frame_fingerprint,
        "offset": len(prefix),
        "length": len(compressed),
        "uncompressed_length": len(text.encode()),
    }
    router = Router.__new__(Router)
    router.source_container = "source"
    router._source_pairs_cache = {}
    router.service = _Service(pack)
    router.service.get_blob_client = lambda _container, _blob: _Blob(
        pack,
        status_code=206,
        content_range=f"bytes {len(prefix)}-{len(pack)-1}/{len(pack)}",
    )
    manifest = SimpleNamespace(
        source={"source_packs": [{"key": hit["source_pack_key"], "sha256": pack_sha, "bytes": len(pack)}]},
        source_generations=(hit["source_generation"],),
    )
    assert router._hydrate(hit, manifest) == text

    router.service.get_blob_client = lambda _container, _blob: _Blob(
        pack, status_code=200, content_range=f"bytes {len(prefix)}-{len(pack)-1}/{len(pack)}"
    )
    with pytest.raises(ReleaseError, match="HTTP 200"):
        router._hydrate(hit, manifest)


def test_router_verifies_small_selector_without_rehashing_all_artifacts():
    selector = {"manifest_sha256": "a" * 64}
    manifest = SimpleNamespace(fulltext={"index_id": "patents-r"})

    class _Releases:
        def read(self, key):
            assert key == "releases/current.json"
            return canonical_json(selector)

    class _Publisher:
        calls = []

        def verify_selector(self, value, *, validate_artifacts):
            assert value == selector
            self.calls.append(validate_artifacts)
            return manifest

    router = Router.__new__(Router)
    router.releases = _Releases()
    router.publisher = _Publisher()
    router._release_lock = threading.RLock()
    router._validated_selector_sha256 = None
    router._validated_active = None
    router._source_pairs_cache = {}
    assert router._active()[1] is manifest
    assert router._active()[1] is manifest
    assert router.publisher.calls == [False]


def test_incremental_search_targets_only_active_shards(monkeypatch):
    router = Router.__new__(Router)
    router.search_url = "http://quickwit"
    router.timeout = 1
    router.max_hydration_bytes = 10000
    manifest = SimpleNamespace(
        release_id="release-two",
        source_generations=("sha256:" + "a" * 64, "sha256:" + "b" * 64),
        source={"superseded_generation_ids": []},
        fulltext={"shards": [
            {"index_id": "patents-old", "generation_id": "old"},
            {"index_id": "patents-new", "generation_id": "new"},
        ]},
    )
    router._active = lambda: ({"manifest_sha256": "a" * 64}, manifest, "b" * 64)
    router._hydrate = lambda hit, _manifest: hit["publication_uid"]
    seen = []

    class Response:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def read(self):
            return json.dumps({
                "hits": [{"publication_uid": "US:1:B2", "length": 1, "uncompressed_length": 1}],
                "num_hits": 1,
            }).encode()

    def urlopen(request, timeout):
        seen.append((request.full_url, json.loads(request.data)))
        return Response()

    monkeypatch.setattr(source_router.urlrequest, "urlopen", urlopen)
    result = router.search(b'{"query":"patent","max_hits":1}')
    assert seen[0][0] == "http://quickwit/api/v1/patents-old,patents-new/search"
    assert "source_generation" in seen[0][1]["query"]
    hit = result["hits"][0]
    assert hit["snippets"] == []
    assert "canonical_text" not in hit


def test_search_returns_snippets_from_hydrated_markdown(monkeypatch):
    markdown = (
        "<!-- publication_uid: US:1:B2 -->\n"
        "# Hydrophone damper\n"
        "\n"
        "## Abstract\n"
        "\n"
        "A hydrophone damper assembly.\n"
        "\n"
        "## Claims\n"
        "\n"
        "The assembly comprises a damper.\n"
    )
    router = Router.__new__(Router)
    router.search_url = "http://quickwit"
    router.timeout = 1
    router.max_hydration_bytes = 10000
    manifest = SimpleNamespace(
        release_id="release-one",
        source_generations=("sha256:" + "a" * 64,),
        source={"superseded_generation_ids": []},
        fulltext={"index_id": "patents-r"},
    )
    router._active = lambda: ({"manifest_sha256": "a" * 64}, manifest, "b" * 64)
    router._hydrate = lambda _hit, _manifest: markdown

    class Response:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def read(self):
            return json.dumps({
                "hits": [{"publication_uid": "US:1:B2", "length": 1, "uncompressed_length": 1}],
                "num_hits": 1,
            }).encode()

    monkeypatch.setattr(source_router.urlrequest, "urlopen", lambda *_args, **_kwargs: Response())
    result = router.search(b'{"query":"hydrophone damper","max_hits":1}')
    hit = result["hits"][0]
    assert "canonical_text" not in hit
    fields = {item["field"] for item in hit["snippets"]}
    assert fields & {"title", "abstract"}
    highlighted = " ".join(item["text"] for item in hit["snippets"])
    assert "<b>" in highlighted
    assert "hydrophone" in highlighted.casefold() or "damper" in highlighted.casefold()


def test_router_rejects_modified_generation_manifest():
    generation = "sha256:" + "a" * 64
    payload = {
        "generation_id": generation,
        "source_packs": [{"key": "source/v1/packs/x.zst", "sha256": "b" * 64, "bytes": 1}],
    }
    raw = canonical_json(payload)
    router = Router.__new__(Router)
    router.releases = SimpleNamespace(read=lambda _key: raw.replace(b"x.zst", b"y.zst"))
    router._source_pairs_cache = {}
    manifest = SimpleNamespace(
        digest="release-digest",
        source_generations=(generation,),
        source={
            "source_packs": payload["source_packs"],
            "generation_manifests": [
                {
                    "generation_id": generation,
                    "artifact": {
                        "key": "source/v1/generations/a.json",
                        "sha256": hashlib.sha256(raw).hexdigest(),
                        "bytes": len(raw),
                    },
                }
            ],
        },
    )
    with pytest.raises(ReleaseError, match="digest mismatch"):
        router._source_pairs(manifest)


@pytest.mark.parametrize("uid", ["", 'US:1:B2" OR *', "US:1:B2/extra"])
def test_point_read_rejects_invalid_ids_before_release_access(uid):
    router = Router.__new__(Router)
    router._active = lambda: pytest.fail("invalid ID accessed the release")
    with pytest.raises(source_router.SearchQueryError):
        router.get_patent(uid)


@pytest.mark.parametrize("returned_uids", [[], ["US:1:B2"], ["US:2:B2"], ["US:1:B2", "US:1:B2"]])
def test_point_read_returns_complete_text_and_checks_identity(monkeypatch, returned_uids):
    router = Router.__new__(Router)
    router.search_url = "http://quickwit"
    router.timeout = 65
    router.max_hydration_bytes = 1024
    generation = "sha256:" + "a" * 64
    manifest = SimpleNamespace(
        release_id="release-1", fulltext={"index_id": "patents-r"},
        source_generations=[generation], source={},
    )
    router._active = lambda: ({"manifest_sha256": "b" * 64}, manifest, "c" * 64)
    markdown = "<!-- publication_uid: US:1:B2 -->\n# Patent\n\n## Claims\n\nComplete claims.\n"
    hydrated = []
    def hydrate(hit, selected_manifest):
        assert selected_manifest is manifest
        hydrated.append(hit["publication_uid"])
        return markdown
    router._hydrate = hydrate
    class Response:
        def __enter__(self):
            return self
        def __exit__(self, *_args):
            pass
        def read(self):
            return json.dumps({"hits": [{"publication_uid": uid, "source_pack_key": "internal"} for uid in returned_uids], "num_hits": len(returned_uids)}).encode()
    def urlopen(request, **kwargs):
        query = json.loads(request.data)
        assert '(publication_uid:"US:1:B2")' in query["query"]
        assert generation in query["query"]
        assert query["max_hits"] == 2
        return Response()
    monkeypatch.setattr(source_router.urlrequest, "urlopen", urlopen)
    if not returned_uids:
        with pytest.raises(source_router.PublicationNotFoundError):
            router.get_patent("US:1:B2")
    elif returned_uids == ["US:1:B2"]:
        result = router.get_patent("US:1:B2")
        assert result["markdown"] == markdown
        assert result["active_release_id"] == "release-1"
        assert "source_pack_key" not in result
        assert hydrated == ["US:1:B2"]
    else:
        with pytest.raises(ReleaseError):
            router.get_patent("US:1:B2")
        assert not hydrated


@pytest.mark.parametrize("uids", [[], ["US:1:B2"] * 101, "US:1:B2", ["US:1:B2", None]])
def test_batch_rejects_invalid_list_before_release_access(uids):
    router = Router.__new__(Router)
    router._active = lambda: pytest.fail("invalid batch accessed release")
    with pytest.raises(source_router.SearchQueryError):
        router.get_patents(uids)


def test_batch_order_missing_ids_deduplication_and_shared_budget(monkeypatch):
    router = Router.__new__(Router)
    router.search_url = "http://quickwit"
    router.timeout = 65
    router.max_hydration_bytes = 20
    manifest = SimpleNamespace(release_id="r1", fulltext={"index_id": "patents-r"}, source_generations=["sha256:" + "a" * 64], source={})
    releases = []
    def active():
        releases.append(1)
        return {"manifest_sha256": "b" * 64}, manifest, "c" * 64
    router._active = active
    router._hydrate = lambda hit, selected: "full " + hit["publication_uid"]
    class Response:
        def __enter__(self):
            return self
        def __exit__(self, *_args):
            pass
        def read(self):
            return json.dumps({"hits": [{"publication_uid": uid, "length": 5, "uncompressed_length": 5} for uid in ["US:2:B2", "US:1:B2"]], "num_hits": 2}).encode()
    def urlopen(req, **kwargs):
        query = json.loads(req.data)
        assert query["query"].count('publication_uid:"US:1:B2"') == 1
        return Response()
    monkeypatch.setattr(source_router.urlrequest, "urlopen", urlopen)
    result = router.get_patent(["US:1:B2", "US:3:B2", "US:2:B2", "US:1:B2"])
    assert releases == [1]
    assert [doc["publication_uid"] for doc in result["documents"]] == ["US:1:B2", "US:2:B2"]
    assert [doc["markdown"] for doc in result["documents"]] == ["full US:1:B2", "full US:2:B2"]
    assert result["missing_publication_uids"] == ["US:3:B2"]
    router.max_hydration_bytes = 19
    with pytest.raises(ReleaseError, match="hydration budget"):
        router.get_patents(["US:1:B2", "US:2:B2"])
