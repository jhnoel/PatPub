"""REST query failures and responsiveness while an upstream search is waiting."""

import asyncio
import threading
from types import SimpleNamespace

import pytest

pytest.importorskip("mcp")
from fts.production.serving.api import asgi
from fts.production.serving.api.source_router import SearchQueryError


def request(body=b'{"query":"patent"}'):
    async def read_body():
        return body

    return SimpleNamespace(
        headers={"authorization": "Bearer test", "content-length": str(len(body))},
        body=read_body,
    )


def test_rest_invalid_query_returns_400(monkeypatch):
    def search(_body):
        raise SearchQueryError("Field does not exist: inventro")

    monkeypatch.setenv("ROUTER_BEARER_TOKEN", "test")
    monkeypatch.setattr(asgi, "_router", SimpleNamespace(search=search))
    result = asyncio.run(asgi.search(request()))
    assert result.status_code == 400
    assert b"Field does not exist" in result.body


def test_rest_search_does_not_block_health_checks(monkeypatch):
    started = threading.Event()
    release = threading.Event()

    def search(_body):
        started.set()
        assert release.wait(2), "event loop blocked behind the search"
        return {"hits": [], "num_hits": 0}

    monkeypatch.setenv("ROUTER_BEARER_TOKEN", "test")
    monkeypatch.setattr(asgi, "_router", SimpleNamespace(search=search))

    async def check():
        pending = asyncio.create_task(asgi.search(request()))
        try:
            for _ in range(100):
                if started.is_set():
                    break
                await asyncio.sleep(0.005)
            assert started.is_set()
            assert (await asgi.healthz(None)).status_code == 200
            assert not pending.done()
        finally:
            release.set()
            result = await pending
        assert result.status_code == 200

    asyncio.run(check())


@pytest.mark.parametrize("error,status", [(None, 200), (SearchQueryError("bad ID"), 400), (asgi.PublicationNotFoundError("missing"), 404), (asgi.ReleaseError("corrupt source"), 502)])
def test_rest_point_read_status_and_mcp_operation(monkeypatch, error, status):
    def get_patent(uid):
        assert uid == "US:1:B2"
        if error:
            raise error
        return {"publication_uid": uid, "markdown": "Full text", "active_release_id": "r1"}
    monkeypatch.setenv("ROUTER_BEARER_TOKEN", "test")
    monkeypatch.setattr(asgi, "_router", SimpleNamespace(get_patent=get_patent))
    req = request()
    req.path_params = {"publication_uid": "US:1:B2"}
    result = asyncio.run(asgi.get_publication(req))
    assert result.status_code == status
    if error is None:
        assert asgi.get_patent("US:1:B2")["markdown"] == "Full text"
    req.headers["authorization"] = "Bearer wrong"
    assert asyncio.run(asgi.get_publication(req)).status_code == 401


@pytest.mark.parametrize("body,status", [(b'{"publication_uids":["US:1:B2","US:2:B2"]}', 200), (b'[]', 400), (b'{', 400), (b'{"other":[]}', 400)])
def test_rest_batch_input_and_mcp(monkeypatch, body, status):
    calls = []
    def get_patents(uids):
        calls.append(uids)
        return {"documents": [{"publication_uid": uid, "markdown": "complete text", "active_release_id": "r1"} for uid in uids], "missing_publication_uids": [], "active_release_id": "r1"}
    monkeypatch.setenv("ROUTER_BEARER_TOKEN", "test")
    monkeypatch.setattr(asgi, "_router", SimpleNamespace(get_patents=get_patents, get_patent=get_patents))
    req = request(body)
    result = asyncio.run(asgi.get_publications(req))
    assert result.status_code == status
    if status == 200:
        assert len(asgi.get_patents(["US:1:B2", "US:2:B2"])["documents"]) == 2
        assert len(asgi.get_patent(["US:1:B2", "US:2:B2"])["documents"]) == 2
    else:
        assert calls == []
    req.headers["authorization"] = "Bearer wrong"
    assert asyncio.run(asgi.get_publications(req)).status_code == 401
