"""Install this project's local post-commit hook without changing Git config."""
import argparse
from pathlib import Path
import shlex
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
MARKER = '# Blueprint Local Git CI/CD'


def install():
    top = subprocess.run(['git', 'rev-parse', '--show-toplevel'], cwd=ROOT,
                         capture_output=True, text=True, check=True).stdout.strip()
    if Path(top).resolve() != ROOT:
        raise ValueError('Initialize a separate code-only Git repository at the project root first')
    configured = subprocess.run(['git', 'config', '--get', 'core.hooksPath'], cwd=ROOT,
                                capture_output=True, text=True)
    if configured.returncode == 0 and configured.stdout.strip():
        raise ValueError('An existing hooksPath is configured; preserve it and integrate the runner manually')
    hook = ROOT/'.git/hooks/post-commit'
    if hook.exists() and MARKER not in hook.read_text(encoding='utf-8'):
        raise ValueError('An existing post-commit hook must be preserved')
    text = '#!/bin/sh\n'+MARKER+'\nexec '+shlex.quote(str(Path(sys.executable).resolve()).replace('\\','/'))+' -B scripts/ci_cd.py --service-role Blueprint-Local-Git-CI\n'
    hook.write_text(text, encoding='utf-8', newline='\n')
    hook.chmod(hook.stat().st_mode | 0o111)
    print('Installed the local post-commit hook. No Git configuration changed.')


if __name__ == '__main__':
    argparse.ArgumentParser(description=__doc__).parse_args()
    install()
