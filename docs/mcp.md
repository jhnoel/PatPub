# PatPub MCP

PatPub exposes a stateless Streamable HTTP MCP server at
`https://api.jamienoel.com/patpub/mcp`. The Cloudflare gateway forwards this
route to the PatPub search service. It requires a shared MCP key in the
`X-Functions-Key` header; this is separate from the REST API's bearer token.

## Tool

- `search_patents`: searches the active published-patent release. Supports the
  same Quickwit query syntax as the REST endpoint, with `max_hits` up to 100
  and `start_offset` up to 900. Results include publication metadata and
  snippets, while omitting internal source-pack pointers.

The `inventor` field is not indexed. `applicant:Vaswani` searches applicant
names, which include inventor names in some records; it does not provide
complete inventor coverage. Invalid field names and syntax return the search
engine's query explanation instead of a generic upstream failure.

Broad phrase searches scan title, abstract, claims, and description by default.
Use `claims:"large language model"` or
`assignee:Google AND (title:transformer OR abstract:attention)` when those
restrictions fit the research question. The router waits up to 65 seconds by
default, exceeding Quickwit's 60-second search budget. Override this with
`UPSTREAM_TIMEOUT_SECONDS` only in coordination with the search engine budget.

The MCP server uses JSON Streamable HTTP responses and stateless requests.
The spec is available at `https://api.jamienoel.com/patpub/openapi.json`
with the REST bearer token. Its server base URL is
`https://api.jamienoel.com/patpub`, with search at `/api/search`.

## Open WebUI

Create an MCP server connection with:

```text
URL: https://api.jamienoel.com/patpub/mcp
Header: X-Functions-Key: <PatPub MCP key>
```

The key is set as the Terraform `patpub_mcp_key` sensitive variable and as the
Container App secret `PATPUB_MCP_KEY`. Supply browser Origins in the optional
Terraform `patpub_mcp_allowed_origins` variable if the MCP client sends an
Origin header. Requests without Origin are allowed.

Cloudflare has an enabled custom Skip rule (`patpub_skip_bic`) matching only
`api.jamienoel.com` paths `/patpub/mcp`, `/patpub/openapi.json`, and
`/patpub/api/search`.
It skips only Browser Integrity Check (`bic`) so machine clients can connect.
Other security products, the deployed Worker, and origin key checks remain
active.

For an operator check after deployment, see [operations](operations.md).
