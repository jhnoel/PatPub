# Launch and update

PatPub currently deploys to Azure. A fresh stack needs no existing index, seeded
fetch markers, migration output, or pre-provisioned Terraform backend.

## Launch an empty deployment

Install Python 3.11+, Azure CLI and Terraform 1.6+. Sign in with `az login` and
use an Azure subscription where you can provision resources and assign roles.
Supply a USPTO Open Data Portal API key in your environment. Choose a globally
unique 5–10 character lowercase alphanumeric suffix:

```sh
cp ingestion.example.toml ingestion.toml
# Edit ingestion.toml before launching.
export USPTO_API_KEY='your USPTO ODP key'
python scripts/launch_azure.py --suffix mypub123
```

This command provisions billable resources. It registers the required Azure
providers, creates storage and the application environment, installs the supplied
key in Key Vault, builds two Linux AMD64 images using Azure Container Registry,
deploys their immutable digests, and immediately starts the ingestion worker.
Docker is not required on your machine. Use `--subscription` and `--location` to
choose a subscription and region; `--config` selects your saved TOML file.
`archives_per_run = 0` (the default) drains the selected backlog in one run.
Set a positive number only when you deliberately want a smaller batch; Azure
stops any single run after 12 hours, and the hourly schedule resumes remaining
work from saved receipts.

Private credentials, configuration, and local Terraform state are retained in
`.patpub/<suffix>/`, excluded from Git and Docker build contexts. Back up this
directory privately. Reuse the same suffix, subscription and region to resume
a failed launch. Read the generated `router_bearer_token` and `patpub_mcp_key`
from `terraform.tfvars.json` for REST and MCP clients; do not publish that file.
REST is at `<serving-url>/api/search`; MCP is at `<serving-url>/patpub/mcp`.
REST sends `Authorization: Bearer <router_bearer_token>`; MCP sends
`X-Functions-Key: <patpub_mcp_key>`.

The launcher leaves your existing `terraform/` backend and private inputs alone.
A team can later move this local state to a protected remote backend using
Terraform's standard state migration.

The worker creates the missing Quickwit index, renders its first ZIP, projects
metadata, verifies artifacts and a search/hydration canary, then publishes
`releases/current.json`. Search is available after that first release. Before
then the service can report that no release is available; readiness only checks
that Quickwit is running.

Hourly runs drain pending retries and fetch/process all missing selected archives
until caught up. Acquisition merges enabled catalogs chronologically: the oldest
missing archive is processed first, and backfill continues as far back as each
selected product exists. Newly published weeks wait behind an unfinished
historical backlog. Completed fetch receipts prevent downloading or inspecting
remote headers for every old archive on every run. If Azure's 12-hour execution
limit ends a run, a later scheduled run resumes from saved receipts.

## Choose document categories

`ingestion.example.toml` is the public template; your edited `ingestion.toml` is
excluded from Git and image build contexts. The launcher validates it and saves
its contents in the deployment inputs, then passes them to the worker. Keep it
locally and rerun the launcher to deploy changed preferences. Existing image
digests are reused; pass `--rebuild-images` when deploying changed application
code.

| Category | Product | Available full-text format period | Default |
| --- | --- | --- | --- |
| `aps_grants` | PTGRAPS | 1976–2001 | Enabled |
| `xml_grants` | PTGRXML | 2002 onward | Enabled |
| `application_publications` | APPXML | 2001 onward | Enabled |
| `pre_1976_grants` | OCR adapter needed | Not implemented | Disabled |

The [USPTO format documentation](https://www.uspto.gov/learning-and-resources/xml-resources)
describes these format periods. The catalogs use publication coverage dates;
upload and modification dates can be decades later and are not used for year
filtering. The grant XML boundary is 2002: to include grants from 2001 while
skipping earlier APS grants, keep APS enabled with `from_year = 2001`.

Each supported category has `enabled`, `from_year`, and optional `through_year`.
For only modern XML grants and application publications, set
`categories.aps_grants.enabled = false`. For your full 1976-onward selection,
keep all three supported categories enabled and leave pre-1976 disabled.
Unsupported pre-1976 ingestion fails clearly if enabled; there is no silent
pretend backfill of image-only documents.

Changing a selection stops future acquisition and leaves disabled pending events
untouched for a possible later re-enable. It does not delete previously published
data. Existing releases and immutable source generations remain compatible.
A new deployment creates its own fetch receipts, source packs, metadata, index
and release without external seed or migration tools.

## Build images locally

From the repository root:

```sh
docker build --platform linux/amd64 -f ingestion-worker/Dockerfile -t patpub-ingestion .
docker build --platform linux/amd64 -f fts/production/serving/combined-app/Dockerfile -t patpub-serving .
```

Both renderers compile from the checked-in Rust sources and lockfiles. No private
base image or sibling repository is required. Push the images to your registry
and supply their immutable digests as `ingestion_image` and `container_image`.

## Update an existing deployment

Keep the existing backend, subscription, resource names, suffix, index ID and
metastore namespace. Do not replace your inputs with the fresh-stack example.
Back up state and the current selector before changing the deployment.

```sh
terraform -chdir=terraform init -backend-config=backend.local.hcl
terraform -chdir=terraform fmt -check
terraform -chdir=terraform validate
terraform -chdir=terraform plan -out=tfplan
```

Review and apply the plan while old workers are idle. The single ingestion job
replaces the five old jobs, queue, dispatch permissions, and unused identities.
The controller identity is retained automatically through a Terraform `moved`
block. Replace the five image inputs with `ingestion_image`; retain the serving
image input and existing corpus namespaces. Stored render events are recovered
from Blob Storage, so pending work does not depend on retaining the old queue.

`compute_enabled=false` removes compute; it is not a pause switch.
`ingestion_enabled=false` creates a manual job. Changing its trigger may replace
the job; do this while idle. An upgraded corpus automatically adopts existing verified hash receipts when
keys, ETags and sizes match. This copies evidence into the normal cache without
rereading corpus bytes. Missing evidence for an already published artifact stops
the run instead of triggering a historical rehash. Fresh stacks need none of
these optional compatibility records.

State, generated plans, backend inputs and credentials stay private. Remove a
saved plan after applying or abandoning it. Public ingress requires both REST
and MCP authentication keys.

## Optional gateway

The serving URL exposes REST and MCP directly with authentication. A separate
custom-domain gateway is optional. `api-gateway/worker.js` supplies the existing
Cloudflare gateway; configure its upstream and routes for your deployment.
See [client access](mcp.md) for headers and paths.
