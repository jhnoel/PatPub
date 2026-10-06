"""Fresh-launch preparation must never copy existing deployment inputs."""
import importlib.util
import json
from pathlib import Path


def launcher():
    path = Path(__file__).resolve().parents[1] / 'scripts/launch_azure.py'
    spec = importlib.util.spec_from_file_location('launch_azure', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_launcher_prepares_isolated_backend_without_private_inputs(tmp_path):
    module = launcher()
    config = {'subscription_id': 'new-subscription', 'resource_suffix': 'new123',
              'router_bearer_token': 'private-key'}
    work = tmp_path / 'new123'
    module.prepare(work, config)
    assert 'backend "azurerm"' not in (work / 'versions.tf').read_text()
    assert json.loads((work / 'terraform.tfvars.json').read_text()) == config
    assert not (work / 'terraform.tfvars').exists()
    assert not (work / 'backend.local.hcl').exists()
    assert (work / 'terraform.tfvars.json').stat().st_mode & 0o777 == 0o600
    assert work.stat().st_mode & 0o777 == 0o700
