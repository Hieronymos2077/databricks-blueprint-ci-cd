# Blueprint deployment with local CI/CD and drift monitoring

This extends the existing synthetic customer-churn workflow. The original source
archive remains unchanged. This package contains code and configuration only.

## What runs

1. A commit on `main` invokes the local Git `post-commit` hook.
2. The runner checks Python syntax, focused detector tests, bundle paths and job limits.
3. It verifies the exact workspace, delegated operator and original job identity.
4. It validates and deploys the Databricks bundle, preserving the original pipeline ID.
5. It starts one drift-monitoring smoke run and verifies its native result and controls.
6. A receipt records the commit, job/run IDs and drift results outside the source tree.

Other branches run local checks without deployment. Deployment is disarmed in the
distributed configuration until an operator deliberately enables it locally.
Repeated invocation for an already verified commit returns its existing receipt.
An operating-system mutex prevents competing local deployments. Native run
idempotency prevents duplicate smoke runs after an uncertain start response.

After the drift smoke run, the same commit starts one bounded run of the platform
pipeline and verifies its receipt (see "Platform pipeline" below).

There is no GitHub Actions workflow, hosted CI service, active cloud schedule,
serving endpoint or new standalone AWS infrastructure. The platform pipeline
declares a daily schedule that is deployed PAUSED; the runner refuses any other
schedule state. The local computer must be
available when the commit is made. A failed post-commit check does not undo the Git
commit; it blocks deployment and reports that CI/CD needs attention.

## Platform pipeline

The `platform_pipeline` job (`databricks/src/de_patterns/platform_pipeline.py`) adds the
data-engineering path of the blueprint on the same synthetic fixture, as three tasks:

1. `ingest_changes`: writes one deterministic batch of customer change events (inserts,
   updates, deletes, one out-of-order late event and one invalid row) as JSON files to
   the Unity Catalog Volume `workspace.blueprint_demo.landing`, then Auto Loader
   (`cloudFiles`, `availableNow`) ingests only new files into `raw_customer_changes`.
   The checkpoint lives in the Volume, so each file is ingested exactly once.
2. `cdc_scd2`: invalid rows go to `customer_changes_quarantine`. Valid rows are reduced
   to the latest event per customer and applied by a CDC `MERGE` (update, insert,
   delete, ordered by event time) to `customers_current`, which has Change Data Feed
   enabled. `customers_scd2` is then built from that table's Change Data Feed as an
   SCD Type 2 history (`valid_from`, `valid_to`, `is_current`). Processed batches and
   feed versions are stored in `platform_watermarks`, so reruns do not reapply them.
3. `batch_score`: scores every current customer with the pinned Unity Catalog model
   version `workspace.blueprint_demo.churn_model/1` and writes `churn_predictions`
   keyed by scoring run. It verifies the five original fixture tables are unchanged
   and writes a receipt to `runtime/platform-receipts/<run_id>.json`.

Each run adds one new change batch, up to 30 batches. The schedule (daily 06:00 UTC)
is PAUSED. Enabling it is an explicit operator decision because it bills compute daily.
No online feature store or model serving endpoint is created: the registered model
uses feature lookups, so serving it needs a persistent online store, which is a
recurring cost. Batch scoring covers the scoring requirement without always-on resources.

## Drift monitoring

The main job now runs `prepare_features`, `train_and_register`, then `drift_monitor`.
A separate manual monitoring job allows checks without repeating model training.
The same monitoring code runs in both jobs.

The monitor compares three feature distributions, prediction distribution and label
distribution against the selected READY model's original synthetic training fixture.
It verifies the reference against the original MLflow training-data fingerprint.
PSI at or above 0.2, or a missing-value-rate change of at least 0.05, raises an alert.
Rows with invalid features remain in quality metrics and have missing predictions.

Derived Delta tables in `workspace.blueprint_demo`:

* `drift_baselines`: immutable distribution profiles keyed by model version.
* `drift_metrics`: per-batch PSI, missing rates, row counts and status.
* `drift_alerts`: only actual current-batch alerts.

Writes merge on batch/model/field so retries cannot duplicate metrics. A reused
batch ID with changed results fails rather than claiming a successful readback.
The original five data tables are read-only to the monitor.

Every check also compares unchanged reference data and deliberately shifted
in-memory data. The unchanged control must pass and all three shifted features
must raise drift. These controls never write fake drift to the real alert table.
The receipt and controls are retained in the monitoring MLflow run.

A real alert persists results and fails the native monitoring task with
`DRIFT_ALERT`, making it visible in Jobs & Pipelines. No email, retraining or model
promotion happens automatically. These are synthetic validation results, not proof
of production business performance or integration with an external data source.

## Setup on the local Git runner

Use the existing Python, Git and Databricks CLI installations. Do not copy their
installed dependencies into this project. The runner itself uses Python's standard
library; Databricks supplies the cloud runtime and the declared Feature Engineering
dependency. See the official [bundle documentation](https://docs.databricks.com/aws/en/dev-tools/bundles).

The runner checks the workspace and operator in `ci/config.json`. Review those
identities before using a different operator. Keep access tokens only in the
approved credential broker or process environment. Never save them in this tree.

For a new extracted copy:

```powershell
git init -b main
python -B scripts/ci_cd.py --check
python -B scripts/install_git_hook.py
```

Create `.ci-local.json` containing nonsecret local settings. It is ignored by Git:

```json
{
  "enabled": true,
  "cli": "databricks",
  "credential_key": "databricks-blueprint-workspace-provided"
}
```

The broker defaults to the current user's `.creds/creds.py`. An already authorized
`DATABRICKS_TOKEN` in the process environment is also supported. The config file
contains a broker key name only, never its secret value.

Commit reviewed code on `main`. The hook then completes checks, deployment and
the bounded smoke run. To reconcile or retry the same commit manually:

```powershell
python -B scripts/ci_cd.py --service-role Blueprint-Local-Git-CI
```

Each job allows one active run, a maximum 15-minute job timeout, no automatic
task retries and at most 10,000 current rows. No trial balance is inferred or
guaranteed by this package. Native compute usage can be billed by the provider.

## Stop, recovery and rollback

Set `enabled` to `false` in `.ci-local.json` to stop automatic cloud deployment.
Local checks still run. There is no background daemon to stop.

Receipts default to the current user's local application data under `BlueprintCI`,
keyed by workspace and bundle. `runtime_dir` may reference an existing canonical
runtime directory outside this repository. Inspect `latest.json` for the last
state and native run ID. Reconcile that run before attempting another deployment.

For subsequent source changes, `git revert` followed by a new reviewed commit
uses the same checked deployment path. The first deployment also preserves the
original job settings in the canonical runtime directory. An explicit rollback
restores those settings without deleting the added job, metrics or source:

```powershell
python -B scripts/rollback_pipeline.py
```

Original upstream: [MIT data-platform-blueprint](https://github.com/toretto17/data-platform-blueprint/tree/029a1c20fc666590a0fd93182bb71bf4a583b548).
The adapted training and preparation files retain the original license. Added files
provide the local Git runner, bounded drift monitor and focused checks.
