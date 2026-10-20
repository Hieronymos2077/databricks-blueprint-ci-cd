"""Local Git CI/CD: checks, identity verification, bundle deployment and smoke run.

Runs on an explicit Git commit, never on a timer. Credentials stay in the broker
or process environment. Receipts and the deployment mutex live outside source.
"""
import argparse
import ast
import base64
from contextlib import contextmanager
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
BUNDLE = ROOT/'infrastructure/databricks/asset-bundles'


def process(arguments, cwd=ROOT, env=None, timeout=90):
    completed = subprocess.run(arguments, cwd=cwd, env=env, capture_output=True,
                               text=True, encoding='utf-8', timeout=timeout)
    if completed.returncode:
        # Never expose provider output or a process environment in failure messages.
        detail = ''
        if env and env.get('DATABRICKS_TOKEN'):
            detail = completed.stderr.replace(env['DATABRICKS_TOKEN'], '[private credential]')[-3000:]
            detail = re.sub(r'(?i)Bearer\s+\S+|dapi[a-f0-9]{16,}', '[private credential]', detail)
        raise RuntimeError('Command failed: '+Path(str(arguments[0])).name+' '+str(arguments[1:3])+' '+detail)
    return completed.stdout


def config():
    value = json.loads((ROOT/'ci/config.json').read_text(encoding='utf-8'))
    private = ROOT/'.ci-local.json'
    if private.exists():
        local = json.loads(private.read_text(encoding='utf-8'))
        if set(local)-{'enabled', 'cli', 'credential_key', 'broker', 'runtime_dir', 'authorization'}:
            raise ValueError('Local config contains unsupported fields; never put credentials in it')
        value.update(local)
    return value


def local_checks():
    for file in ROOT.rglob('*.py'):
        if '.git' not in file.parts and '.databricks' not in file.parts:
            ast.parse(file.read_text(encoding='utf-8'), filename=str(file.relative_to(ROOT)))
    if (ROOT/'.github/workflows').exists():
        raise ValueError('GitHub Actions are prohibited for this delivery')
    definition = json.loads((BUNDLE/'databricks.yml').read_text(encoding='utf-8'))
    jobs = definition['resources']['jobs']
    if set(jobs) != {'ml_pipeline', 'drift_monitor', 'platform_pipeline'}:
        raise ValueError('Only the reviewed pipeline, its drift monitor and the platform pipeline may be deployed')
    for name, job in jobs.items():
        if any(key in job for key in ('continuous', 'trigger', 'job_clusters')):
            raise ValueError('Continuous compute, triggers and classic clusters are disabled')
        if 'schedule' in job and (name != 'platform_pipeline' or not paused_schedule(job['schedule'])):
            raise ValueError('Only the platform pipeline may define a schedule, and it must stay PAUSED')
        if job.get('max_concurrent_runs') != 1 or not 0 < job.get('timeout_seconds', 0) <= 900:
            raise ValueError('Each job must have one bounded active run')
        for task in job['tasks']:
            if task.get('max_retries') != 0 or task.get('timeout_seconds', 0) > 600:
                raise ValueError('Tasks must have finite timeouts and no automatic retry')
            source = (BUNDLE/task['spark_python_task']['python_file']).resolve()
            if not source.is_relative_to(ROOT) or not source.is_file():
                raise ValueError('A bundle source path escapes the project or is missing')
    output = process([sys.executable, '-B', '-m', 'unittest', 'discover', '-s', 'tests'], timeout=90)
    return {'python_syntax': True, 'focused_checks': 'passed', 'cloud_schedules': False,
            'paused_schedules': [name for name, job in jobs.items() if 'schedule' in job],
            'declared_jobs': list(jobs), 'test_output': output.strip()}


def paused_schedule(schedule):
    return isinstance(schedule, dict) and schedule.get('pause_status') == 'PAUSED' and bool(schedule.get('quartz_cron_expression'))


@contextmanager
def deployment_lock(runtime):
    runtime.mkdir(parents=True, exist_ok=True)
    with (runtime/'deployment.lock').open('a+b') as handle:
        handle.seek(0, 2)
        if handle.tell() == 0:
            handle.write(b'0'); handle.flush()
        handle.seek(0)
        if os.name == 'nt':
            import msvcrt
            msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        try:
            yield
        finally:
            handle.seek(0)
            if os.name == 'nt':
                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def save(path, value):
    temporary = path.with_suffix('.tmp')
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True)+'\n', encoding='utf-8')
    os.replace(temporary, path)


def runtime_path(cfg):
    if cfg.get('runtime_dir'):
        return Path(cfg['runtime_dir']).expanduser().resolve()
    base = Path(os.environ.get('LOCALAPPDATA', str(Path.home()/'.local/state')))
    key = hashlib.sha256((cfg['host']+'|'+cfg['bundle_name']).encode()).hexdigest()[:16]
    return base/'BlueprintCI'/key


def authorized_environment(cfg):
    environment = os.environ.copy()
    token = environment.get('DATABRICKS_TOKEN')
    if not token:
        broker = Path(cfg.get('broker') or Path.home()/'.creds/creds.py')
        names = process([sys.executable, str(broker), 'list']).splitlines()
        if cfg['credential_key'] not in names:
            raise ValueError('Selected credential key is absent from the broker')
        token = process([sys.executable, str(broker), 'get', cfg['credential_key']]).strip()
    if not token:
        raise ValueError('Authorized credential provider returned no value')
    environment.update(DATABRICKS_TOKEN=token, DATABRICKS_HOST=cfg['host'], DATABRICKS_AUTH_TYPE='pat',
                       DATABRICKS_CONFIG_FILE=str(runtime_path(cfg)/'no-file-config'), PYTHONDONTWRITEBYTECODE='1')
    return environment, token


def api(cfg, token, method, path, data=None, query=None):
    url = cfg['host'].rstrip('/')+path
    if query:
        url += '?'+urllib.parse.urlencode(query)
    body = json.dumps(data).encode() if data is not None else None
    request = urllib.request.Request(url, data=body, method=method,
        headers={'Authorization': 'Bearer '+token, 'Content-Type': 'application/json'})
    try:
        with urllib.request.urlopen(request, timeout=45) as response:
            return json.load(response)
    except urllib.error.HTTPError as error:
        raise RuntimeError('Databricks '+method+' '+path+' returned HTTP '+str(error.code)) from None


def verify_identity(cfg, token):
    identity = api(cfg, token, 'GET', '/api/2.0/preview/scim/v2/Me')
    if identity.get('userName', '').casefold() != cfg['expected_operator'].casefold() or str(identity.get('id')) != str(cfg['expected_operator_id']):
        raise ValueError('Authenticated operator differs from the approved identity')
    original = api(cfg, token, 'GET', '/api/2.2/jobs/get', query={'job_id': cfg['original_job_id']})
    if int(original.get('job_id', 0)) != cfg['original_job_id'] or original.get('run_as_user_name') != cfg['expected_operator']:
        raise ValueError('Original pipeline identity differs')
    active = api(cfg, token, 'GET', '/api/2.2/jobs/runs/list',
                 query={'job_id': cfg['original_job_id'], 'active_only': 'true', 'limit': 25})
    if active.get('runs'):
        raise ValueError('The pipeline has an active run; do not deploy over it')
    return identity


def cli(cfg, environment, arguments, timeout=180):
    text = process([cfg.get('cli') or 'databricks', *arguments, '--output', 'json'],
                   cwd=BUNDLE, env=environment, timeout=timeout)
    return json.loads(text) if text.strip() else {}


def verify_drift(evidence, run_id):
    if not evidence or not evidence.get('self_test', {}).get('controls_passed') or evidence.get('source_tables_modified') is not False:
        raise ValueError('The cloud drift detector controls were not verified')
    if evidence.get('batch_id') != str(run_id):
        raise ValueError('The monitoring receipt belongs to a different native run')
    if evidence.get('alert_fields'):
        raise ValueError('Current data triggered a real drift alert')


def verify_platform(evidence, run_id):
    if not evidence or evidence.get('run_id') != str(run_id) or evidence.get('source_tables_modified') is not False:
        raise ValueError('The platform receipt belongs to a different native run or modified source tables')
    if evidence.get('auto_loader_rows') != evidence.get('landed_rows') or not evidence.get('landed_rows'):
        raise ValueError('Auto Loader did not ingest the landed batch exactly once')
    if not evidence.get('quarantined_rows') or evidence.get('cdc_applied_batch') != evidence.get('batch_no'):
        raise ValueError('Data quality quarantine or the CDC watermark was not verified')
    if evidence.get('scd2_current_rows') != evidence.get('current_rows') or not evidence.get('scd2_closed_rows'):
        raise ValueError('SCD2 history does not match the current table')
    if evidence.get('predictions') != evidence.get('current_rows') or not str(evidence.get('model_uri', '')).endswith('/1'):
        raise ValueError('Batch scoring did not cover every current customer with the pinned model version')
    if len(evidence.get('original_table_sha256') or {}) != 5:
        raise ValueError('Original fixture tables were not verified')


def wait_for_run(cfg, token, run_id, receipt, path, tasks_expected=('drift_monitor',),
                 receipt_root=None, verify=verify_drift, key='drift_receipt'):
    deadline = time.monotonic()+960
    while time.monotonic() < deadline:
        run = api(cfg, token, 'GET', '/api/2.2/jobs/runs/get', query={'run_id': run_id})
        state = run.get('state') or {}
        if state.get('life_cycle_state') in ('TERMINATED', 'SKIPPED', 'INTERNAL_ERROR'):
            if state.get('result_state') != 'SUCCESS':
                receipt.update(state='smoke_failed', result_state=state.get('result_state'), run_state=state)
                save(path, receipt)
                raise RuntimeError('Smoke run '+str(run_id)+' did not succeed; inspect the native job run')
            tasks = run.get('tasks') or []
            if sorted(task.get('task_key') for task in tasks) != sorted(tasks_expected):
                raise ValueError('Smoke run has unexpected tasks')
            if any((task.get('state') or {}).get('result_state') != 'SUCCESS' for task in tasks):
                raise ValueError('A smoke run task did not succeed')
            receipt_path = (receipt_root or cfg['receipt_root']).rstrip('/')+'/'+str(run_id)+'.json'
            output = api(cfg, token, 'GET', '/api/2.0/workspace/export',
                         query={'path': receipt_path, 'format': 'AUTO'})
            evidence = json.loads(base64.b64decode(output['content']))
            verify(evidence, run_id)
            if key == 'drift_receipt':
                receipt.update(drift_receipt=evidence, task_run_id=tasks[0]['run_id'], run_state=state)
            else:
                receipt.update({key: evidence, key+'_task_run_ids': {task['task_key']: task['run_id'] for task in tasks},
                                key+'_run_state': state})
            return
        time.sleep(12)
    receipt['state'] = 'run_still_active'; save(path, receipt)
    raise RuntimeError('The bounded native run has not reached a terminal state; reconcile its recorded run ID')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--check', action='store_true')
    parser.add_argument('--service-role', default='Blueprint-Local-Git-CI')
    args = parser.parse_args()
    checks = local_checks()
    if args.check:
        print(json.dumps(checks)); return 0
    cfg = config()
    print('Blueprint CI/CD: local checks passed, deploying and running the bounded smoke run (this can take several minutes)...', file=sys.stderr, flush=True)
    branch = process(['git', 'symbolic-ref', '--short', 'HEAD']).strip()
    if branch != cfg['deploy_branch']:
        print(json.dumps({'state': 'checks_passed_branch_not_deployed', 'branch': branch})); return 0
    if not cfg.get('enabled'):
        print(json.dumps({'state': 'checks_passed_deployment_disarmed'})); return 0
    dirty = process(['git', 'status', '--porcelain']).strip()
    if dirty:
        raise ValueError('Commit the reviewed source first; a dirty tree cannot deploy. Uncommitted paths: '
                         + ', '.join(line[3:] for line in dirty.splitlines()[:10]))
    commit = process(['git', 'rev-parse', 'HEAD']).strip()
    runtime = runtime_path(cfg)
    if runtime.is_relative_to(ROOT):
        raise ValueError('CI runtime state must stay outside the source repository')
    with deployment_lock(runtime):
        path = runtime/(commit+'.json')
        prior = json.loads(path.read_text(encoding='utf-8')) if path.exists() else {}
        if prior.get('state') == 'verified':
            print(json.dumps({'state': 'already_verified', 'commit': commit, 'run_id': prior['run_id']})); return 0
        receipt = prior or {'commit': commit, 'started_at': datetime.now(timezone.utc).isoformat(),
            'host': cfg['host'], 'role': args.service_role, 'pid': os.getpid(), 'source': str(ROOT),
            'checks': checks, 'state': 'checked', 'cloud_schedule': False}
        save(path, receipt)
        try:
            environment, token = authorized_environment(cfg)
            verify_identity(cfg, token)
            rollback = runtime/'original-job-settings.json'
            if not rollback.exists():
                original = api(cfg, token, 'GET', '/api/2.2/jobs/get', query={'job_id': cfg['original_job_id']})
                save(rollback, original)
            latest_path = runtime/'latest.json'
            latest = json.loads(latest_path.read_text(encoding='utf-8')) if latest_path.exists() else {}
            for prior_job in ('drift_job_id', 'platform_job_id'):
                if latest.get(prior_job) and not receipt.get('run_id'):
                    active = api(cfg, token, 'GET', '/api/2.2/jobs/runs/list', query={
                        'job_id': latest[prior_job], 'active_only': 'true', 'limit': 25})
                    if active.get('runs'):
                        raise ValueError('A previous smoke run is active; reconcile it before deploying')
            if not receipt.get('run_id'):
                receipt['state'] = 'validating'; save(path, receipt)
                cli(cfg, environment, ['bundle', 'validate', '--strict', '-t', 'dev'])
                receipt['state'] = 'deploying'; save(path, receipt)
                cli(cfg, environment, ['bundle', 'deploy', '-t', 'dev'], timeout=300)
                summary = cli(cfg, environment, ['bundle', 'summary', '-t', 'dev'])
                resources = summary['resources']['jobs']
                if int(resources['ml_pipeline']['id']) != cfg['original_job_id']:
                    raise ValueError('Deployment did not preserve the original job identity')
                job_id = int(resources['drift_monitor']['id'])
                definition = api(cfg, token, 'GET', '/api/2.2/jobs/get', query={'job_id': job_id})
                settings = definition['settings']
                if settings.get('schedule') or settings.get('continuous') or settings.get('trigger') or settings.get('max_concurrent_runs') != 1:
                    raise ValueError('Drift job is not bounded and manual')
                platform_id = int(resources['platform_pipeline']['id'])
                platform = api(cfg, token, 'GET', '/api/2.2/jobs/get', query={'job_id': platform_id})['settings']
                if (not paused_schedule(platform.get('schedule')) or platform.get('continuous') or platform.get('trigger')
                        or platform.get('max_concurrent_runs') != 1):
                    raise ValueError('Platform job schedule is not PAUSED or the job is not bounded')
                receipt.update(platform_job_id=platform_id, platform_schedule=platform['schedule'])
                receipt.update(state='starting_smoke', drift_job_id=job_id,
                    idempotency_token=hashlib.sha256((cfg['host']+str(job_id)+commit).encode()).hexdigest())
                save(path, receipt)
                response = api(cfg, token, 'POST', '/api/2.2/jobs/run-now',
                    data={'job_id': job_id, 'idempotency_token': receipt['idempotency_token']})
                receipt.update(state='smoke_running', run_id=response['run_id']); save(path, receipt)
            if not receipt.get('drift_receipt'):
                wait_for_run(cfg, token, receipt['run_id'], receipt, path)
                save(path, receipt)
            if not receipt.get('platform_run_id'):
                receipt['platform_idempotency_token'] = hashlib.sha256(
                    (cfg['host']+str(receipt['platform_job_id'])+commit+'platform').encode()).hexdigest()
                receipt['state'] = 'starting_platform_smoke'; save(path, receipt)
                response = api(cfg, token, 'POST', '/api/2.2/jobs/run-now', data={
                    'job_id': receipt['platform_job_id'], 'idempotency_token': receipt['platform_idempotency_token']})
                receipt.update(state='platform_smoke_running', platform_run_id=response['run_id']); save(path, receipt)
            wait_for_run(cfg, token, receipt['platform_run_id'], receipt, path,
                         tasks_expected=('ingest_changes', 'cdc_scd2', 'batch_score'),
                         receipt_root=cfg['platform_receipt_root'], verify=verify_platform, key='platform_receipt')
            receipt.update(state='verified', verified_at=datetime.now(timezone.utc).isoformat())
            save(path, receipt); save(runtime/'latest.json', receipt)
            print(json.dumps({'state': 'verified', 'commit': commit, 'run_id': receipt['run_id'],
                'drift_job_id': receipt['drift_job_id'], 'receipt': str(path),
                'model_uri': receipt['drift_receipt']['model_uri'], 'cloud_schedule': False,
                'platform_job_id': receipt['platform_job_id'], 'platform_run_id': receipt['platform_run_id'],
                'platform_schedule_paused': True}))
            return 0
        except Exception as error:
            receipt['last_error'] = str(error)[:800]
            receipt['failed_at'] = datetime.now(timezone.utc).isoformat()
            save(path, receipt); save(runtime/'latest.json', receipt)
            raise


if __name__ == '__main__':
    try:
        sys.exit(main())
    except Exception as error:
        print('CI/CD requires attention: '+str(error), file=sys.stderr)
        sys.exit(1)
