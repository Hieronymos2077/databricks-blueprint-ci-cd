# Restore guide for the workspace export

Branch `tutorial-export` holds the deployable source (identical to `main` at commit
6cbfbfc) plus `exports/`, a full read-only export of everything this blueprint created
in the Databricks workspace `https://dbc-939dcb75-eac7.cloud.databricks.com` before it
was torn down on 2026-10-17 and redeployed from zero. No credentials are stored here.

## What is in `exports/`

| Folder | Content |
|---|---|
| `jobs/` | Full job definitions (`<job_id>.json`), every run (`<job_id>-runs.json`) and each task output (`run-output/`) for the ml-pipeline 789648897090823, platform-pipeline 106481472574404 and drift-monitor 948924464555049. Session policy strings in the logs are redacted. |
| `bundle-workspace/` | The deployed bundle folder `/Workspace/Users/lawsuser4@gmail.com/.bundle/data-platform-blueprint-demo/` byte for byte: synced source, earlier delivered ZIPs, diagrams and documents, bundle state and the drift and platform receipts. |
| `tables/<name>/` | For each of the 15 managed Delta tables in `workspace.blueprint_demo`: `ddl.sql` (SHOW CREATE TABLE), `tblproperties.json`, `history.json` (DESCRIBE HISTORY), `table-info.json` (Unity Catalog metadata) and `data.csv` (every row). `row-counts.json` lists the counts. |
| `unity-catalog/` | Schema, Volume and function metadata. |
| `volume-landing/` | All files of the Volume `workspace.blueprint_demo.landing`: the landed change batch and the Auto Loader checkpoint. |
| `model/` | Unity Catalog registered model `workspace.blueprint_demo.churn_model` and version 1 metadata (READY, source run a7f9a3ce9ad043a492c1fccd801984ef). |
| `mlflow/` | Experiments `blueprint-churn-demo` (2817437476331785) and `blueprint-drift-monitor` (433161743033883): experiment metadata, runs with params, metrics, tags, metric history and all artifacts, including the full model directory logged with feature lookups. |
| `ci-receipts/` | Local Git CI/CD receipts per commit, `latest.json` and the preserved original job settings. |
| `MANIFEST.json` | Every exported file and a summary. |

## Recommended restore: redeploy, then compare

The pipeline is deterministic on its synthetic fixture, so the cleanest restore is a
fresh deployment, not a byte copy:

```bash
databricks bundle validate --strict -t dev
databricks bundle deploy -t dev
databricks bundle run -t dev ml_pipeline
databricks bundle run -t dev platform_pipeline
databricks bundle run -t dev drift_monitor
```

Run these from `infrastructure/databricks/asset-bundles` with `DATABRICKS_HOST` and
`DATABRICKS_TOKEN` set in the process environment. The jobs recreate the schema, all
tables, the Volume, the experiments and model version 1. Compare the new row counts
with `exports/tables/row-counts.json`. Job, run and experiment IDs will be new.

## Restoring exact table contents

To reload a table exactly as exported (for example for an audit), upload its
`data.csv` to a Volume and recreate it from the saved DDL:

```sql
-- 1. run the statement from exports/tables/<name>/ddl.sql
-- 2. load the rows
COPY INTO workspace.blueprint_demo.<name>
FROM '/Volumes/workspace/blueprint_demo/<volume>/<name>/data.csv'
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'false')
COPY_OPTIONS ('mergeSchema' = 'false');
```

CSV values are strings; Databricks casts them to the DDL column types on load.
Delta history, Change Data Feed versions and feature table registration are not
recreated by a CSV load; use the redeploy path when those matter.

## Restoring the model without retraining

`exports/mlflow/blueprint-churn-demo/runs/<run_id>/artifacts/model/` is a complete
MLflow model logged with `databricks-feature-engineering`. With `mlflow` and
`databricks-feature-engineering` installed and the `customer_features` table present:

```python
import mlflow
mlflow.set_registry_uri("databricks-uc")
mlflow.register_model("file:///<path>/artifacts/model", "workspace.blueprint_demo.churn_model")
```

Retraining through `ml_pipeline` produces an equivalent model from the same fixture.

## Restoring job definitions by hand

Each `exports/jobs/<job_id>.json` has a `settings` object accepted by
`POST /api/2.2/jobs/create`. Prefer the bundle, which owns these jobs and keeps the
schedule PAUSED.
