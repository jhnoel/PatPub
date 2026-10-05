#!/usr/bin/env python3
"""Check deployed REST/MCP access and release-aware search on demand."""

import argparse
import json
import os
from urllib.error import HTTPError
from urllib.request import Request, urlopen


def call(base, path, *, headers=None, body=None, timeout=120):
    request = Request(
        base.rstrip("/") + path,
        data=json.dumps(body).encode() if body is not None else None,
        headers={"content-type": "application/json", **(headers or {})},
        method="POST" if body is not None else "GET",
    )
    try:
        with urlopen(request, timeout=timeout) as response:
            data = response.read()
            return response.status, json.loads(data) if data else {}
    except HTTPError as exc:
        # Do not echo response bodies or request headers containing credentials.
        return exc.code, {}


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default=os.environ.get("PATPUB_URL"))
    parser.add_argument("--origin-url", default=os.environ.get("SERVING_URL"))
    parser.add_argument("--query", required=True, help="query known to return hits")
    args = parser.parse_args()
    token = os.environ.get("ROUTER_BEARER_TOKEN", "").strip()
    if not args.url or not token:
        parser.error("set PATPUB_URL (or --url) and ROUTER_BEARER_TOKEN")
    if args.origin_url:
        status, _ = call(args.origin_url, "/readyz")
        require(status == 200, "origin readiness failed")
    query = {"query": args.query, "max_hits": 1}
    for path, body in [("/openapi.json", None), ("/api/search", query)]:
        status, _ = call(args.url, path, body=body)
        require(status == 401, f"missing REST token accepted at {path}")
        status, _ = call(args.url, path, headers={"authorization": "Bearer invalid"}, body=body)
        require(status == 401, f"invalid REST token accepted at {path}")
    auth = {"authorization": f"Bearer {token}"}
    status, spec = call(args.url, "/openapi.json", headers=auth)
    require(status == 200 and "/api/search" in spec.get("paths", {}), "OpenAPI check failed")
    status, result = call(args.url, "/api/search", headers=auth, body=query)
    require(status == 200, "authenticated search failed")
    hits = result.get("hits", [])
    require(bool(hits), "check query returned no hits")
    for hit in hits:
        require(bool(hit.get("publication_uid")) and bool(hit.get("active_release_id")),
                "search hit lacks publication/release identity")
        require(isinstance(hit.get("snippets"), list), "search hit lacks hydrated snippets")
    print("REST authentication, OpenAPI, and release-aware search passed")
    key = os.environ.get("PATPUB_MCP_KEY", "").strip()
    if key:
        headers = {"accept": "application/json, text/event-stream"}
        initialize = {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {
            "protocolVersion": "2025-03-26", "capabilities": {},
            "clientInfo": {"name": "patpub-maintenance", "version": "1"}}}
        status, _ = call(args.url, "/mcp", headers=headers, body=initialize)
        require(status == 401, "missing MCP key accepted")
        headers["x-functions-key"] = key
        status, response = call(args.url, "/mcp", headers=headers, body=initialize)
        require(status == 200 and "result" in response, "MCP initialization failed")
        request = {"jsonrpc": "2.0", "id": 2, "method": "tools/call", "params": {
            "name": "search_patents", "arguments": {"query": args.query, "max_hits": 1}}}
        status, response = call(args.url, "/mcp", headers=headers, body=request)
        require(status == 200 and "result" in response
                and not response["result"].get("isError"), "MCP search failed")
        print("MCP authentication and search passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
