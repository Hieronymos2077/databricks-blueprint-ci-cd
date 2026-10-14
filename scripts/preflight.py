"""Read-only operator, catalog and bundle checks. Does not deploy or run compute."""
import argparse
import json
import os
from pathlib import Path
import re
import subprocess
import sys
from urllib.parse import urlparse

def approved_token(args):
    if not args.credential_key:
        token = os.environ.get('DATABRICKS_TOKEN', '')
        if not token:
            raise ValueError('Use an existing authorized access token through the broker or private process environment')
        return token
    broker = Path(args.broker)
    names = subprocess.run([sys.executable, str(broker), 'list'], capture_output=True, text=True, encoding='utf-8', check=True)
    if args.credential_key not in names.stdout.splitlines():
        raise ValueError('The selected credential key is absent from the broker')
    result = subprocess.run([sys.executable, str(broker), 'get', args.credential_key], capture_output=True, text=True, encoding='utf-8', check=True)
    token = result.stdout.strip()
    if not token:
        raise ValueError('The approved credential provider returned no value')
    return token

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--cli', default='databricks')
    parser.add_argument('--host', default='https://dbc-939dcb75-eac7.cloud.databricks.com')
    parser.add_argument('--catalog', default='workspace')
    parser.add_argument('--schema', default='blueprint_demo')
    parser.add_argument('--expected-operator', required=True)
    parser.add_argument('--credential-key')
    parser.add_argument('--broker', default=str(Path.home()/'.creds/creds.py'))
    args = parser.parse_args()
    host = args.host.rstrip('/').lower()
    parsed = urlparse(host)
    if parsed.scheme != 'https' or not (parsed.hostname or '').endswith('.cloud.databricks.com') or parsed.path or parsed.query or parsed.fragment or parsed.username:
        raise ValueError('Use an approved AWS Databricks workspace origin')
    if not all(re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]{0,62}', value) for value in (args.catalog, args.schema)):
        raise ValueError('Use simple catalog and schema identifiers')
    environment = os.environ.copy()
    environment.update(DATABRICKS_HOST=host, DATABRICKS_TOKEN=approved_token(args), DATABRICKS_AUTH_TYPE='pat')
    # "pat" is the CLI's bearer-token provider name. This script does not create a token.
    environment['DATABRICKS_CONFIG_FILE'] = str(Path(__file__).parent/'no-file-databricks-config')
    root = Path(__file__).resolve().parents[1]

    def read(arguments, directory=root):
        result = subprocess.run([args.cli, *arguments, '--output', 'json'], cwd=directory,
            env=environment, capture_output=True, text=True, encoding='utf-8', timeout=90)
        if result.returncode:
            raise RuntimeError('Read-only preflight failed at '+arguments[0]+'. Check approved access through the private provider; no deployment was attempted.')
        return json.loads(result.stdout) if result.stdout.strip() else {}

    identity = read(['current-user', 'me'])
    if str(identity.get('userName') or '').casefold() != args.expected_operator.casefold() or identity.get('active') is False:
        raise ValueError('The authenticated operator differs from the approved identity')
    catalog = read(['catalogs', 'get', args.catalog])
    if catalog.get('name') != args.catalog:
        raise ValueError('The catalog readback differs from the selected catalog')
    variables = 'workspace_host='+host+',catalog='+args.catalog+',schema='+args.schema
    read(['bundle', 'validate', '--strict', '-t', 'dev', '--var', variables], root/'infrastructure/databricks/asset-bundles')
    print(json.dumps({'operator': identity['userName'], 'workspace_host': host, 'catalog': args.catalog,
        'bundle_validation_passed': True, 'schema_creation_permission_verified': False,
        'trial_allowance_verified': False, 'deployment_started': False, 'compute_started': False}))

if __name__ == '__main__':
    try:
        main()
    except (ValueError, RuntimeError, OSError, subprocess.SubprocessError, json.JSONDecodeError) as error:
        # Never print provider stdout, process environments or secret-bearing command diagnostics.
        print(str(error) if isinstance(error, (ValueError, RuntimeError)) else 'Read-only preflight could not complete', file=sys.stderr)
        sys.exit(1)
