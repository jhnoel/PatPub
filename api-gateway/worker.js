const MCP_PATH = "/patpub/mcp";
const MCP_METHODS = new Set(["GET", "POST", "DELETE"]);
const MCP_HEADERS = [
  "accept",
  "content-type",
  "origin",
  "mcp-protocol-version",
  "x-functions-key",
];

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const isSpec = url.pathname === "/patpub/openapi.json";
    const isSearch = url.pathname === "/patpub/api/search" || url.pathname === "/patpub/api/publications";
    const isPublication = /^\/patpub\/api\/publications\/[^/]+$/.test(url.pathname);
    if (url.pathname !== MCP_PATH && !isSpec && !isSearch && !isPublication) {
      return Response.json({ error: "not_found" }, { status: 404 });
    }
    const cors = (isSpec || isSearch || isPublication) ? {
      "access-control-allow-origin": "*",
      "access-control-allow-methods": (isSpec || isPublication) ? "GET, OPTIONS" : "POST, OPTIONS",
      "access-control-allow-headers": "authorization, content-type, accept",
      "access-control-max-age": "600",
    } : {};
    if ((isSpec || isSearch || isPublication) && request.method === "OPTIONS") {
      return new Response(null, { status: 204, headers: cors });
    }
    const methods = (isSpec || isPublication) ? new Set(["GET"]) : isSearch ? new Set(["POST"]) : MCP_METHODS;
    if (!methods.has(request.method)) {
      return new Response(null, { status: 405, headers: { ...cors, allow: [...methods].join(", ") } });
    }

    let upstreamOrigin;
    try {
      const configuredUpstream = new URL(env.UPSTREAM_URL);
      if (
        !["http:", "https:"].includes(configuredUpstream.protocol) ||
        configuredUpstream.pathname !== "/" ||
        configuredUpstream.search ||
        configuredUpstream.hash
      ) {
        throw new TypeError("UPSTREAM_URL must be an origin");
      }
      upstreamOrigin = configuredUpstream.origin;
    } catch {
      return Response.json({
        error: "gateway_not_configured",
        message: "Set UPSTREAM_URL to your serving app's HTTP or HTTPS origin.",
      }, { status: 500, headers: cors });
    }

    const headers = new Headers();
    for (const name of [...MCP_HEADERS, "authorization"]) {
      const value = request.headers.get(name);
      if (value) headers.set(name, value);
    }

    const path = url.pathname === MCP_PATH ? MCP_PATH : url.pathname.slice("/patpub".length);
    const upstream = new URL(path + url.search, upstreamOrigin);
    const options = { method: request.method, headers, redirect: "manual" };
    if (request.method === "POST") {
      options.body = request.body;
      options.duplex = "half";
    }

    const response = await fetch(new Request(upstream, options), {
      cf: { cacheTtl: 0, cacheEverything: false },
    });
    if (isSpec && response.ok) {
      const spec = await response.json();
      spec.servers = [{ url: `${url.origin}/patpub` }];
      return Response.json(spec, { headers: { ...cors, "cache-control": "no-store" } });
    }
    const responseHeaders = new Headers(response.headers);
    responseHeaders.set("cache-control", "no-store");
    for (const [name, value] of Object.entries(cors)) responseHeaders.set(name, value);
    return new Response(response.body, {
      status: response.status,
      statusText: response.statusText,
      headers: responseHeaders,
    });
  },
};
