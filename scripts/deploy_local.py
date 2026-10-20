"""One command local setup and deploy: python -B scripts/deploy_local.py

Needs DATABRICKS_TOKEN set in the same terminal (your own workspace token from
Settings, Developer, Access tokens). Writes the ignored .ci-local.json, installs
the post-commit hook once, then deploys the current commit and waits for the
bounded smoke run.
"""
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]


def main():
    if not os.environ.get('DATABRICKS_TOKEN'):
        sys.exit('Set DATABRICKS_TOKEN in this terminal first, then run this again.')
    cli = shutil.which('databricks')
    if not cli:
        sys.exit('The Databricks CLI is not on PATH. Install it, open a new terminal, then run this again.')
    local = ROOT/'.ci-local.json'
    settings = json.loads(local.read_text(encoding='utf-8')) if local.exists() else {}
    settings.update(enabled=True, cli=Path(cli).as_posix())
    local.write_text(json.dumps(settings, indent=2)+'\n', encoding='utf-8')
    dirty = subprocess.run(['git', 'status', '--porcelain'], cwd=ROOT, capture_output=True, text=True).stdout.strip()
    if dirty:
        sys.exit('Commit or remove these first, then run this again:\n'+dirty)
    hook = ROOT/'.git/hooks/post-commit'
    if not hook.exists():
        subprocess.run([sys.executable, '-B', 'scripts/install_git_hook.py'], cwd=ROOT, check=True)
    print('Deploying the current commit and waiting for the smoke run. This takes a few minutes.', flush=True)
    sys.exit(subprocess.run([sys.executable, '-B', 'scripts/ci_cd.py', '--service-role', 'Blueprint-Local-Git-CI'], cwd=ROOT).returncode)


if __name__ == '__main__':
    main()
