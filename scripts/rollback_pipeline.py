"""Explicitly restore preserved job settings while keeping added resources intact."""
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))
import ci_cd


def main():
    cfg = ci_cd.config()
    runtime = ci_cd.runtime_path(cfg)
    with ci_cd.deployment_lock(runtime):
        environment, token = ci_cd.authorized_environment(cfg)
        ci_cd.verify_identity(cfg, token)
        original = json.loads((runtime/'original-job-settings.json').read_text(encoding='utf-8'))
        if original['job_id'] != cfg['original_job_id']:
            raise ValueError('Rollback snapshot belongs to a different job')
        ci_cd.api(cfg, token, 'POST', '/api/2.2/jobs/reset', data={
            'job_id': cfg['original_job_id'], 'new_settings': original['settings']})
        result = ci_cd.api(cfg, token, 'GET', '/api/2.2/jobs/get', query={'job_id': cfg['original_job_id']})
        if result['settings']['tasks'] != original['settings']['tasks']:
            raise ValueError('Rollback task readback differs from the preserved configuration')
        local_path = ci_cd.ROOT/'.ci-local.json'
        local = json.loads(local_path.read_text(encoding='utf-8')) if local_path.exists() else {}
        local['enabled'] = False
        ci_cd.save(local_path, local)
        print(json.dumps({'original_job_restored': cfg['original_job_id'], 'automatic_cd_enabled': False,
                          'added_monitoring_resources_preserved': True}))


if __name__ == '__main__':
    main()
