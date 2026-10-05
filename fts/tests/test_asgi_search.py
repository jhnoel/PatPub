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
