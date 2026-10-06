# Azure infrastructure

This directory is one Terraform root. File boundaries organize resources;
they do not create modules or change resource addresses.

## Deployment paths

For a new deployment, use the [launcher](../docs/deployment.md#create-a-deployment).
It copies the root into `.patpub/<suffix>/`, removes the remote-backend
declaration from that copy, and manages isolated local state.

For an existing deployment, retain its backend and private inputs. Copy
`backend.hcl.example` to the ignored `backend.local.hcl` only when configuring
a backend; it must point to an existing state location. The
`terraform.tfvars.example` file describes deployment inputs and is not a
replacement for an existing deployment's values.

Run from the repository root:

```sh
terraform -chdir=terraform init -backend-config=backend.local.hcl
terraform -chdir=terraform fmt -check
terraform -chdir=terraform validate
terraform -chdir=terraform plan -out=tfplan
```

Review the plan before applying and make compute changes while ingestion is
idle. Preserve resource names, index IDs, and storage namespaces when updating
an existing corpus. See [deployment](../docs/deployment.md#update-a-deployment)
for image builds and upgrade procedures.

## Resource layout

| File | Responsibility |
| --- | --- |
| `platform.tf` | Resource group, Key Vault, registry, logging, Container Apps environment, and budget |
| `storage.tf` | Publication storage, Quickwit storage, and protected state container |
| `identities.tf` | Deployment context and ingestion/serving identities |
| `permissions.tf` | Container access, registry pulls, vault access, and permission to stop/start serving |
| `ingestion.tf` | Hourly or manual ingestion job |
| `serving.tf` | Combined Quickwit/API app, probes, scaling, and authentication |
| `variables.tf`, `outputs.tf` | Deployment inputs and operator outputs |
| `versions.tf`, `.terraform.lock.hcl` | Backend declaration and provider constraints/lockfile |

## Compute controls

`compute_enabled = false` removes the serving app and ingestion job while
retaining storage and identities. It is not a pause operation.

`ingestion_enabled = false` changes the ingestion job from hourly to manual.
Changing triggers may replace the job; make this change while it is idle.

Serving scales from zero to one replica. Public ingress requires both
`router_bearer_token` and `patpub_mcp_key`. The ingestion worker stops serving
during index writes; see [operations](../docs/operations.md).

## State and credentials

Keep state, plans, backend settings, credentials, and deployment-specific inputs
private. Remove saved plans after applying or abandoning them. The state
container has `prevent_destroy`, and workload identities have no access to it.
