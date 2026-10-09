"""ASGI application for the PatPub REST and Streamable HTTP MCP routes."""

from __future__ import annotations

import hmac
import json
import os
import re
from contextlib import asynccontextmanager
from typing import Any
from urllib import request as urlrequest

from mcp.server.fastmcp import FastMCP
from mcp.server.transport_security import TransportSecuritySettings
from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.routing import Mount, Route
from starlette.concurrency import run_in_threadpool

from .source_router import MAX_REQUEST_BYTES, OPENAPI_SPEC_PATH, Router, SearchQueryError, PublicationNotFoundError
from ingestion.release import ReleaseError


MCP_PATH = "/patpub/mcp"
_INTERNAL_HIT_FIELDS = {
    "offset",
    "length",
    "shard",
    "source_fingerprint",
    "source_generation",
    "source_pack",
    "source_pack_key",
    "source_revision",
    "uncompressed_length",
}
_router: Router | None = None


def _bearer_authorized(request: Request) -> bool:
    expected = os.environ.get("ROUTER_BEARER_TOKEN", "").strip()
    supplied = request.headers.get("authorization", "")
    return bool(expected) and hmac.compare_digest(
        supplied.encode(), f"Bearer {expected}".encode()
    )


def _router_or_error() -> Router:
    if _router is None:
        raise RuntimeError("PatPub router is not ready")
    return _router


def _public_hit(value: Any) -> Any:
    if isinstance(value, list):
        return [_public_hit(item) for item in value]
    if isinstance(value, dict):
        return {
            key: _public_hit(item)
            for key, item in value.items()
            if key not in _INTERNAL_HIT_FIELDS
        }
    return value


class MCPKeyAuthMiddleware:
    """Require the same function-key header convention as PatRecords."""

    def __init__(self, app, path: str = MCP_PATH) -> None:
        self.app = app
        self.path = path

    async def __call__(self, scope, receive, send) -> None:
        if scope["type"] == "http" and scope.get("path") == self.path:
            expected = os.environ.get("PATPUB_MCP_KEY", "").strip()
            supplied = dict(scope.get("headers", [])).get(b"x-functions-key", b"").decode(
                "latin-1"
            )
            if not expected:
                response = JSONResponse(
                    {"error": "MCP authentication is not configured"}, status_code=503
                )
                await response(scope, receive, send)
                return
            if not hmac.compare_digest(supplied.encode(), expected.encode()):
                response = JSONResponse({"error": "unauthorized"}, status_code=401)
                await response(scope, receive, send)
                return
        await self.app(scope, receive, send)


async def openapi(request: Request) -> Response:
    if not _bearer_authorized(request):
        return JSONResponse({"error": "unauthorized"}, status_code=401)
    return Response(
        OPENAPI_SPEC_PATH.read_bytes(),
        status_code=200,
        media_type="application/json",
    )


async def healthz(_: Request) -> Response:
    return JSONResponse({"status": "ok"})


def readyz(_: Request) -> Response:
    try:
        router = _router_or_error()
        with urlrequest.urlopen(f"{router.search_url}/health/readyz", timeout=5) as response:
            if json.loads(response.read()) is not True:
                raise RuntimeError("Quickwit is not ready")
        return JSONResponse({"status": "ready"})
    except Exception:
        return JSONResponse({"status": "starting"}, status_code=503)


async def _request_body(request: Request) -> bytes | Response:
    try:
        length = int(request.headers.get("content-length", "-1"))
    except ValueError:
        length = -1
    if length < 0 or length > MAX_REQUEST_BYTES:
        return JSONResponse({"error": "request too large"}, status_code=413)
    body = await request.body()
    if len(body) != length:
        return JSONResponse({"error": "request too large"}, status_code=413)
    return body


async def _search(request: Request, index_id: str | None = None) -> Response:
    if not _bearer_authorized(request):
        return JSONResponse({"error": "unauthorized"}, status_code=401)
    body = await _request_body(request)
    if isinstance(body, Response):
        return body
    try:
        router = _router_or_error()
        result = (
            await run_in_threadpool(router.candidate_search, index_id, body)
            if index_id is not None
            else await run_in_threadpool(router.search, body)
        )
        return JSONResponse(result)
    except SearchQueryError as exc:
        return JSONResponse({"error": str(exc)}, status_code=400)
    except (ReleaseError, ValueError, KeyError) as exc:
        return JSONResponse({"error": str(exc)}, status_code=502)
    except Exception as exc:  # pragma: no cover - deployment failure boundary
        return JSONResponse({"error": str(exc)}, status_code=500)


async def search(request: Request) -> Response:
    return await _search(request)


async def get_publication(request: Request) -> Response:
    if not _bearer_authorized(request):
        return JSONResponse({"error": "unauthorized"}, status_code=401)
    try:
        result = await run_in_threadpool(
            _router_or_error().get_patent, request.path_params["publication_uid"]
        )
        return JSONResponse(result)
    except SearchQueryError as exc:
        return JSONResponse({"error": str(exc)}, status_code=400)
    except PublicationNotFoundError as exc:
        return JSONResponse({"error": str(exc)}, status_code=404)
    except (ReleaseError, ValueError, KeyError) as exc:
        return JSONResponse({"error": str(exc)}, status_code=502)
    except Exception as exc:
        return JSONResponse({"error": str(exc)}, status_code=500)


async def candidate_search(request: Request) -> Response:
    index_id = request.path_params.get("index_id", "")
    if not re.fullmatch(r"[a-z0-9][a-z0-9_-]{2,100}", index_id):
        return JSONResponse({"error": "not found"}, status_code=404)
    return await _search(request, index_id=index_id)


allowed_hosts = [
    "api.jamienoel.com",
    "patpub-searcher.mangowave-a2671481.eastus2.azurecontainerapps.io",
    "localhost:*",
    "127.0.0.1:*",
]
allowed_hosts.extend(
    item.strip()
    for item in os.environ.get("PATPUB_MCP_ALLOWED_HOSTS", "").split(",")
    if item.strip()
)
allowed_origins = [
    item.strip()
    for item in os.environ.get("PATPUB_MCP_ALLOWED_ORIGINS", "").split(",")
    if item.strip()
]

mcp = FastMCP(
    "PatPub",
    instructions=(
        "Search the published patent corpus. Queries use Quickwit syntax and can "
        "target indexed fields such as title, abstract, claims, description, "
        "publication_number, applicant, and assignee."
        " The inventor field is not indexed. Applicant names can include inventor "
        "names in some records, but do not provide complete inventor coverage. "
        "For broad phrase queries, target title, abstract, or claims to reduce latency."
    ),
    stateless_http=True,
    json_response=True,
    streamable_http_path="/mcp",
    transport_security=TransportSecuritySettings(
        enable_dns_rebinding_protection=True,
        allowed_hosts=allowed_hosts,
        allowed_origins=allowed_origins,
    ),
)


@mcp.tool(
    name="search_patents",
    title="Search published patents",
    description=(
        "Search PatPub's active published-patent corpus. Supports fielded "
        "Quickwit syntax such as assignee:Acme AND claims:battery. Results "
        "include publication metadata and text snippets."
        " The inventor field is unsupported; applicant contains applicant names "
        "and sometimes inventor names. Broad searches across the entire patent "
        "text can be slow; use title:, abstract:, or claims: to narrow them."
    ),
)
def search_patents(
    query: str,
    max_hits: int = 10,
    start_offset: int = 0,
) -> dict[str, Any]:
    """Search the active PatPub release and return public publication fields."""
    body = json.dumps(
        {"query": query, "max_hits": max_hits, "start_offset": start_offset},
        separators=(",", ":"),
    ).encode()
    result = _router_or_error().search(body)
    return {
        "hits": _public_hit(result.get("hits", [])),
        "num_hits": result.get("num_hits", 0),
        "elapsed_time_micros": result.get("elapsed_time_micros"),
        "active_release_id": result.get("active_release_id"),
    }


@mcp.tool(
    name="get_patent",
    title="Read full patent text",
    description=(
        "Fetch a publication's complete canonical Markdown, including abstract, "
        "description, and claims, from the active release. Pass the publication_uid "
        "returned by search_patents, for example US:9999999:B2. "
        "Returns an error if the ID is invalid or absent from the active release."
    ),
)
def get_patent(publication_uid: str) -> dict[str, Any]:
    return _router_or_error().get_patent(publication_uid)


mcp_app = mcp.streamable_http_app()


@asynccontextmanager
async def lifespan(_: Starlette):
    global _router
    _router = Router()
    async with mcp_app.router.lifespan_context(mcp_app):
        yield


app = Starlette(
    routes=[
        Route("/openapi.json", openapi, methods=["GET"]),
        Route("/healthz", healthz, methods=["GET"]),
        Route("/readyz", readyz, methods=["GET"]),
        Route("/api/search", search, methods=["POST"]),
        Route("/api/publications/{publication_uid}", get_publication, methods=["GET"]),
        Route("/api/v1/{index_id}/search", candidate_search, methods=["POST"]),
        Mount("/patpub", app=mcp_app),
    ],
    middleware=[Middleware(MCPKeyAuthMiddleware)],
    lifespan=lifespan,
)
