# PatPub infrastructure

This is one Azure Terraform root for steady-state operation. File boundaries do
not create modules or change resource addresses.

| File | Responsibility |
| --- | --- |
| `platform.tf` | Resource group, Key Vault, registry, logging, environment, budget |
| `storage.tf` | Publication and Quickwit storage, pipeline queue, protected state container |
| `identities.tf` | Deployment context and six workload identities |
| `permissions.tf` | Container-scoped access, registry pulls, queue/vault access, controller roles |
| `ingestion.tf` | Daily fetch, render, metadata, indexer, and queue-driven orchestrator jobs |
| `serving.tf` | Single combined Quickwit/API app, probes, scale to zero, authentication |
| `variables.tf`, `outputs.tf` | Deployment inputs and operator outputs |
| `versions.tf`, `.terraform.lock.hcl` | Backend/provider configuration and locked provider versions |

Copy `backend.hcl.example` to ignored `backend.local.hcl` and configure the
existing state location. Copy `terraform.tfvars.example` to ignored
`terraform.tfvars` only for a new deployment; preserve existing inputs when
maintaining the running stack.

```sh
terraform init -backend-config=backend.local.hcl
terraform fmt -check
terraform validate
terraform plan -out=tfplan
```

Review the saved plan before applying. State, plan files, backend settings,
credentials, and deployment-specific inputs remain private. The state container
has `prevent_destroy`; workload identities have no access to it.

Persisted corpus compatibility records remain in storage; see
[operations](../docs/operations.md).

Image build contexts and the update procedure are in
[deployment](../docs/deployment.md).
