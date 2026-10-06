#!/usr/bin/env python3
"""Provision a fresh PatPub stack, build its images, and start automatic backfill.

Uses isolated local Terraform state under .patpub/<suffix>; never copies private
inputs from terraform/. Requires Python 3.11+, Azure CLI, Terraform, an Azure
subscription where you can create role assignments, and USPTO_API_KEY in the
environment. Running this script provisions billable Azure resources.
"""
from __future__ import annotations
import argparse
import json
import os
from pathlib import Path
import re
import secrets
import shutil
import subprocess
import tempfile
import time
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ingestion.config import load_config

ROOT = Path(__file__).resolve().parents[1]
PROVIDERS = ('Microsoft.App', 'Microsoft.ContainerRegistry', 'Microsoft.Storage',
             'Microsoft.KeyVault', 'Microsoft.ManagedIdentity', 'Microsoft.OperationalInsights',
             'Microsoft.Insights', 'Microsoft.Consumption')


def run(*args, capture=False, **kwargs):
    return subprocess.run(args, check=True, text=True, capture_output=capture, **kwargs).stdout


def output(work, name):
    return run('terraform', f'-chdir={work}', 'output', '-raw', name, capture=True).strip()


def save_config(path, value):
    path.write_text(json.dumps(value, indent=2) + '\n')
    path.chmod(0o600)


def prepare(work, config):
    work.mkdir(parents=True, exist_ok=True, mode=0o700)
    work.chmod(0o700)
    for source in (ROOT / 'terraform').glob('*.tf'):
        text = source.read_text()
        if source.name == 'versions.tf':
            # A clone needs no separately provisioned remote backend.
            text = text.replace('  backend "azurerm" {}\n', '')
        (work / source.name).write_text(text)
    shutil.copyfile(ROOT / 'terraform/.terraform.lock.hcl', work / '.terraform.lock.hcl')
    save_config(work / 'terraform.tfvars.json', config)


def install_uspto_key(vault, api_key, subscription):
    with tempfile.NamedTemporaryFile(mode='w', prefix='patpub-secret-', delete=False) as handle:
        path = Path(handle.name)
        path.chmod(0o600)
        handle.write(api_key)
    try:
        for attempt in range(12):
            result = subprocess.run(['az', 'keyvault', 'secret', 'set', '--vault-name', vault,
                                     '--name', 'uspto-odp-api-key', '--file', str(path),
                                     '--subscription', subscription, '--output', 'none', '--only-show-errors'], capture_output=True)
            if result.returncode == 0:
                return
            # Fresh vault RBAC can take time to become effective.
            if b'Forbidden' not in result.stderr or attempt == 11:
                raise RuntimeError('Unable to install USPTO key; check Key Vault permissions and retry.')
            time.sleep(10)
    finally:
        path.unlink(missing_ok=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--suffix', required=True, help='Unique 5–10 lowercase letters/digits; reuse to resume this stack.')
    parser.add_argument('--subscription', help='Defaults to the Azure CLI active subscription.')
    parser.add_argument('--location', default='eastus2')
    parser.add_argument('--rebuild-images', action='store_true', help='Build new images when updating an existing launch.')
    parser.add_argument('--config', type=Path, default=ROOT / 'ingestion.toml', help='Saved local document-category configuration.')
    args = parser.parse_args()
    if not re.fullmatch(r'[a-z0-9]{5,10}', args.suffix):
        parser.error('Use a valid deployment suffix.')
    if not args.config.exists():
        args.config.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(ROOT / 'ingestion.example.toml', args.config)
    try:
        ingestion = load_config(args.config)
    except (ValueError, OSError) as exc:
        parser.error(str(exc))
    api_key = os.environ.get('USPTO_API_KEY', '').strip()
    if not api_key:
        parser.error('Set USPTO_API_KEY in the environment before launching.')
    for command in ('az', 'terraform'):
        if shutil.which(command) is None:
            parser.error(f'{command} is required.')
    subscription = args.subscription or run('az', 'account', 'show', '--query', 'id', '-o', 'tsv', capture=True).strip()
    work = ROOT / '.patpub' / args.suffix
    config_path = work / 'terraform.tfvars.json'
    if config_path.exists():
        config = json.loads(config_path.read_text())
        if config['subscription_id'] != subscription or config['location'] != args.location:
            parser.error('Existing launch inputs differ; use the original subscription and region.')
    else:
        config = {'subscription_id': subscription, 'location': args.location,
                  'resource_suffix': args.suffix, 'resource_group_name': f'patpub-{args.suffix}',
                  'compute_enabled': False, 'ingestion_enabled': False,
                  'external_ingress_enabled': True,
                  'router_bearer_token': secrets.token_urlsafe(32),
                  'patpub_mcp_key': secrets.token_urlsafe(32)}
    config.pop('archives_per_run', None)
    config['ingestion_config_json'] = json.dumps(ingestion.as_dict())
    prepare(work, config)
    for provider in PROVIDERS:
        run('az', 'provider', 'register', '--namespace', provider, '--subscription', subscription,
            '--wait', '--output', 'none')
    run('terraform', f'-chdir={work}', 'init', '-input=false')
    if not config.get('ingestion_image'):
        run('terraform', f'-chdir={work}', 'apply', '-input=false', '-auto-approve')
    install_uspto_key(output(work, 'uspto_key_vault_name'), api_key, subscription)
    if args.rebuild_images or not all(config.get(key) for key in ('ingestion_image', 'container_image')):
        registry_server = output(work, 'acr_login_server')
        registry = registry_server.split('.')[0]
        tag = f'launch-{int(time.time())}'
        images = (('ingestion', 'ingestion-worker/Dockerfile', 'ingestion_image'),
                  ('serving', 'fts/production/serving/combined-app/Dockerfile', 'container_image'))
        for image, dockerfile, variable in images:
            run('az', 'acr', 'build', '--registry', registry, '--subscription', subscription,
                '--platform', 'linux/amd64', '--image', f'{image}:{tag}', '--file', dockerfile, str(ROOT))
            digest = run('az', 'acr', 'repository', 'show', '--name', registry,
                         '--image', f'{image}:{tag}', '--subscription', subscription,
                         '--query', 'digest', '-o', 'tsv', capture=True).strip()
            if not re.fullmatch(r'sha256:[0-9a-f]{64}', digest):
                raise RuntimeError('Registry did not return an immutable image digest.')
            config[variable] = f'{registry_server}/{image}@{digest}'
    config.update(compute_enabled=True, ingestion_enabled=True)
    save_config(config_path, config)
    run('terraform', f'-chdir={work}', 'apply', '-input=false', '-auto-approve')
    job = output(work, 'ingestion_job_resource_id')
    run('az', 'containerapp', 'job', 'start', '--ids', job, '--output', 'none')
    print(f'PatPub deployed: {output(work, "searcher_url")}')
    print(f'Private credentials and Terraform state: {work}')
    print('The first archive is processing; search becomes available after its validated release.')


if __name__ == '__main__':
    main()
