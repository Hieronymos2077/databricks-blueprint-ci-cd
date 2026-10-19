# Pipelines and MLOps lifecycle: stage by stage

This guide describes what each Databricks job in this repository does, task by task, based on the source code. It covers the three jobs declared in `infrastructure/databricks/asset-bundles/databricks.yml`:

| # | Job (bundle key) | Deployed name | Tasks in order |
|---|---|---|---|
| 1 | `ml_pipeline` | `data-platform-blueprint-demo-ml-pipeline` | `prepare_features`, `train_and_register`, `drift_monitor` |
| 2 | `platform_pipeline` | `data-platform-blueprint-demo-platform-pipeline` | `ingest_changes`, `cdc_scd2`, `batch_score` |
| 3 | `drift_monitor` | `data-platform-blueprint-demo-drift-monitor` | `drift_monitor` |

`scripts/ci_cd.py` (function `local_checks`) refuses to deploy unless the bundle declares exactly these three jobs.

Everything runs on a small, deterministic, synthetic customer churn fixture. No real customer data is used. By default the bundle writes into catalog `workspace`, schema `blueprint_demo` (bundle variables `catalog` and `schema`). Table names below are written as `<catalog>.<schema>.<table>`.

## Settings shared by all three jobs

All values below come from `databricks.yml`.

- Compute: every task is a `spark_python_task` using the environment `demo` (`environment_version` "4", with the dependency `databricks-feature-engineering==0.18.1`). No classic job cluster is declared, and `local_checks` rejects `job_clusters`, `continuous` and `trigger` settings.
- Job limits: `max_concurrent_runs` is 1, job `timeout_seconds` is 900, and the run queue is disabled.
- Task limits: `max_retries` is 0, `retry_on_timeout` is false, and `disable_auto_optimization` is true. `local_checks` rejects any task with a timeout above 600 seconds.
- Safety checks at the start of each script: the runtime workspace host must equal the `--expected-host` argument (and for the AWS scripts must be an `https` URL ending in `.cloud.databricks.com`). The catalog and schema names must match the pattern `[A-Za-z_][A-Za-z0-9_]{0,62}`. The platform and drift scripts also check that the running user equals `--expected-operator`.
- Table ownership: tables written by the platform and drift code carry the table property `blueprint.component` (`blueprint-platform-v1` or `blueprint-drift-v1`). If a table with the same name exists with a different owner value, the code raises an error instead of writing to it. Predictive optimization is disabled on these tables.

---

## Pipeline 1: `ml_pipeline` (features, training, registration, drift check)

**Purpose.** Create the synthetic churn dataset and feature table, train a churn classifier, register it in Unity Catalog through MLflow, and run a drift check against that model.

**Trigger and schedule.** No schedule is defined. The job is started manually (or by the smoke run in `scripts/ci_cd.py`, see the lifecycle section). Job timeout 900 seconds.

**Task limits.** `prepare_features` 300 s, `train_and_register` 300 s, `drift_monitor` 600 s. Dependencies: `train_and_register` waits for `prepare_features`, and `drift_monitor` waits for `train_and_register`. If a task fails, the tasks after it do not run.

### Task 1: `prepare_features`

Source: `databricks/src/feature_store/ingestion/prepare_demo.py`, function `prepare`.

**Data generation (`sample_records`).** A seeded random generator (`random.Random(42)`) builds 500 rows with `customer_id` 0 to 499 and:

- `tenure_months`: integer 1 to 60.
- `usage_score`: uniform 0 to 100, rounded to 3 decimals.
- `support_tickets`: integer 0 to 8.
- `churn`: 1 when a score is above 0, where `score = -2.0 + 0.5*tickets - 0.035*months - 0.025*usage + noise` and the noise is uniform between -0.75 and 0.75.

Because the generator is seeded, every run produces the same 500 rows.

**Pre-flight comparison.** Before creating anything, for each of the five tables below that already exists, `assert_table_matches` compares the stored rows to the expected rows using a SHA-256 digest of the sorted contents. A different row count or digest raises an error and nothing is overwritten.

**Tables written (Delta, Unity Catalog).** Each is created only if it does not already exist, with write mode `error` (never overwrite):

| Table | Layer | Content |
|---|---|---|
| `bronze_customers` | Bronze | All 500 generated rows, schema `customer_id LONG, tenure_months INT, usage_score DOUBLE, support_tickets INT, churn INT` |
| `silver_customers` | Silver | Rows that pass the quality filter |
| `labels` | Labels | `customer_id`, `churn` from the valid rows |
| `customer_features` | Feature table | `customer_id`, `tenure_months`, `usage_score`, `support_tickets`, created with `FeatureEngineeringClient.create_table` and primary key `customer_id` |
| `gold_label_counts` | Gold | Count of rows per `churn` value |

**Cleanup and validation rules.**

- Bronze must contain exactly 500 rows and 500 distinct `customer_id` values, otherwise the task fails ("Existing demo Bronze table differs...").
- The quality filter keeps rows where `tenure_months > 0`, `usage_score >= 0`, `support_tickets >= 0` and `churn` is 0 or 1. If the number of valid rows differs from the Bronze count, the task fails ("Demo input failed its data-quality check"). There is no null imputation: the fixture has no nulls, and any invalid row fails the run rather than being repaired.
- The feature table must have the primary key `['customer_id']`, and the feature and label tables must each have the same row count as Bronze.
- At the end, predictive optimization is disabled on all five tables and the digests are recomputed.

**Output.** A JSON line is printed with `rows`, the table names, `primary_keys`, `table_sha256` per table and `data_quality_verified: true`.

**Failure behaviour.** Any mismatch raises `ValueError` and the job run fails. Existing tables are never modified.

### Task 2: `train_and_register`

Source: `databricks/src/mlops/training/training_pipeline.py`, class `ModelTrainerDatabricks`.

**Inputs.** `<catalog>.<schema>.labels` and `<catalog>.<schema>.customer_features`. The MLflow experiment path is passed from the bundle as `/Users/<your user>/blueprint-churn-demo`. The registry is Unity Catalog (`mlflow.set_registry_uri('databricks-uc')`).

**Stages in `run`.**

1. **Training set (`_get_training_data`).** Reads `labels` (must have exactly 500 rows), then builds a training set with `FeatureEngineeringClient.create_training_set` using one `FeatureLookup` on `customer_features` (features `tenure_months`, `usage_score`, `support_tickets`, lookup key `customer_id`, label `churn`, key column excluded). The result is loaded into pandas.
2. **Split and checks (`_split_frame`).** The frame must have 500 rows and no nulls ("Feature join failed readback"). Rows are sorted on all feature columns and the label for a stable order. Both classes (0 and 1) must be present. The split is `train_test_split` with `test_size=0.2`, `random_state=42` and stratification on the label, giving 400 training rows and 100 holdout rows (logged as `train_rows` and `holdout_rows`). The code also checks that training and holdout indexes do not overlap. A SHA-256 digest of the sorted frame is computed and stored as the MLflow tag `data_sha256`.
3. **Training (`_train_model`).** scikit-learn `GradientBoostingClassifier(n_estimators=100, max_depth=3, learning_rate=0.1, random_state=42)` fitted on the training split only.
4. **Evaluation (`_evaluate`).** Metrics are computed on the holdout split, not on training data: `accuracy` (`accuracy_score`), `f1` (`f1_score`, `zero_division=0`) and `roc_auc` (`roc_auc_score` on the predicted probability of class 1). They are logged to MLflow with `log_metrics`.
5. **Quality gate.** If holdout `f1` is below 0.5, the task writes `deployment_receipt.json` with `registered: false` to the MLflow run, then raises "Holdout gate failed; model was not registered". No model version is created in that case.
6. **Registration (`_log_and_register`).** Otherwise `FeatureEngineeringClient.log_model` logs the model (sklearn flavor, artifact path `model`, input example inferred) together with the training set, so the feature lookups are packaged with the model, and registers it as `<catalog>.<schema>.churn_model`, waiting up to 120 seconds. The code then reads back the registry: exactly one version must belong to this run, its status must be `READY`, and its `run_id` must match.
7. **Receipt.** `deployment_receipt.json` (run id, metrics, row counts, model name, model version, data digest, `registered: true`) is logged as an MLflow artifact, and the run tag `registration_verified` is set to `true`. Other run parameters and tags: `fixture` (`synthetic_churn_500`), `random_seed` (42), `upstream_commit`, `purpose` (`deployment_smoke`).
8. **Final readback (`_verify_final_run`).** After the run closes, the code re-reads the MLflow run and confirms its status is `FINISHED`, that the tag `registration_verified` is `true`, that each stored metric equals the computed one, and that `deployment_receipt.json` and `model/MLmodel` exist as artifacts.

**Outputs.**

- Registered model `<catalog>.<schema>.churn_model` (a new version per successful run).
- MLflow experiment `/Users/<your user>/blueprint-churn-demo` with one run per execution.
- A JSON receipt printed to the task output.

**Failure behaviour.** A wrong row count, null in the joined features, missing class, failed F1 gate, or any registry or MLflow readback mismatch raises an exception and fails the task, which stops `drift_monitor`.

### Task 3: `drift_monitor`

This task runs the same script as Pipeline 3. It is documented once, under Pipeline 3 below. In `ml_pipeline` it runs after `train_and_register` with `--model-version latest`, so it checks the model version that was just registered.

**Where to see Pipeline 1 results in the workspace.**

- Jobs & Pipelines: job `data-platform-blueprint-demo-ml-pipeline`, run page and each task's output.
- Experiments: `blueprint-churn-demo` (under your user folder), run metrics `accuracy`, `f1`, `roc_auc`, artifact `deployment_receipt.json`.
- Catalog Explorer: the five tables above in `<catalog>.<schema>`, and the model `churn_model` under Models.

---

## Pipeline 2: `platform_pipeline` (Auto Loader, CDC, SCD Type 2, batch scoring)

**Purpose.** Demonstrate the data engineering path on the same fixture: land change events as files, ingest them incrementally, apply them as a CDC merge, keep a full history, and score the current customers with the registered model.

**Trigger and schedule.** The job declares `quartz_cron_expression` `0 0 6 * * ?` in timezone UTC (daily at 06:00 UTC) with `pause_status` `PAUSED`. It does not run on its own. `scripts/ci_cd.py` refuses any schedule that is not paused and allows a schedule only on this job. Job timeout 900 seconds, each task 480 seconds.

**Order.** `ingest_changes`, then `cdc_scd2`, then `batch_score`. All three call `databricks/src/de_patterns/platform_pipeline.py` with `--step ingest`, `--step cdc` and `--step score`. The run id passed to all steps is `{{job.run_id}}`, and `--fixture-source` points at `prepare_demo.py` so the same fixture rows are available.

**Prerequisites.** The five tables from Pipeline 1 must exist (`silver_customers` is the seed for the current-state table), and the registered model version 1 must exist for scoring.

**Control tables.** `control_tables` creates two owned Delta tables:

- `platform_watermarks` (`name`, `value`, `updated_at`): stores `seed_done`, `cdc_applied_batch` and `scd2_source_version`, so reruns do not reapply work.
- `platform_batches` (`run_id`, `batch_no`, `landed_rows`, `created_at`): maps each job run to one batch number.

### Task 1: `ingest_changes` (`ingest`)

Function: `ingest`.

1. **Choose the batch number.** If this `run_id` is already recorded in `platform_batches`, its batch number is reused (idempotent rerun). Otherwise the number is the highest recorded plus one. The limit is `MAX_BATCHES = 30`, enforced in `change_batch`.
2. **Generate deterministic change events (`change_batch`).** Seeded with `1000 + batch_no`, with event timestamps starting on 1 October 2026 UTC plus `batch_no` days. Each batch contains:
   - 20 updates (`op = 'U'`) of randomly chosen existing customers (tenure increases by the batch number, usage shifts by up to 15 points and is floored at 0, tickets shift by between -2 and +3 and are floored at 0).
   - 1 late, older update for the first updated customer (tenure 1, usage 0, tickets 0, earliest timestamp offset), placed after the newer one, to test that event order wins.
   - 5 inserts (`op = 'I'`) with new ids `100000 * batch_no + k`.
   - 2 deletes (`op = 'D'`) of customers that were not updated in this batch.
   - 1 invalid update with a negative tenure, to prove the quarantine path.
   Each event has a `change_id` such as `b001-0007`. The schema is `change_id, customer_id, tenure_months, usage_score, support_tickets, op, event_ts, batch_no`.
3. **Land the files.** The events are written as one JSON file to the Unity Catalog Volume `<catalog>.<schema>.landing` at `/Volumes/<catalog>/<schema>/landing/customer_changes/batch_<NNNN>` with write mode `error`. If the folder already exists, its `change_id` values must match the expected ones, otherwise the task fails ("An existing landing batch differs...").
4. **Claim the batch.** A `MERGE` into `platform_batches` inserts the batch number if it is new. The code then checks that the batch is owned by this run.
5. **Auto Loader ingestion.** A streaming read with format `cloudFiles` (JSON, `pathGlobFilter` `*.json`, explicit schema) over `.../customer_changes` adds `source_file` (from `_metadata.file_path`) and `ingested_at` (current timestamp) and writes to the Delta table `raw_customer_changes`. The checkpoint is `/Volumes/<catalog>/<schema>/landing/_checkpoints/raw_customer_changes` and the trigger is `availableNow`, so the stream processes new files and then stops. Nothing keeps running afterwards.
6. **Check.** The rows in `raw_customer_changes` for this `batch_no` must equal the landed event count, with all `change_id` values distinct ("Auto Loader did not ingest the landing batch exactly once").

**Outputs.** Volume files, tables `raw_customer_changes`, `platform_batches`, `platform_watermarks`, the Auto Loader checkpoint in the Volume, and a printed JSON (`batch_no`, `landed_rows`, `landing_path`, `auto_loader_rows`, `raw_table`).

### Task 2: `cdc_scd2` (`cdc`)

Functions: `apply_cdc`, `apply_scd2`.

1. **Tables.** Creates `customers_current` (`customer_id, tenure_months, usage_score, support_tickets, last_event_ts, last_change_id`) with Change Data Feed enabled (`delta.enableChangeDataFeed = true`, verified), `customer_changes_quarantine` (the change schema plus `reason`), and later `customers_scd2`.
2. **Seed.** If the `seed_done` watermark is missing, `customers_current` must be empty (otherwise the task fails rather than guessing) and is filled from `silver_customers` with `last_event_ts` set to 1970-01-01 and `last_change_id` set to `seed`. The watermark `seed_done` is then set.
3. **Pending batches.** Reads `raw_customer_changes` rows with `batch_no` above the `cdc_applied_batch` watermark and processes each batch in order.
4. **Validation rule.** A change is valid when `customer_id` is not null, `op` is one of I, U, D, and, for I and U, `tenure_months > 0`, `usage_score >= 0` and `support_tickets >= 0` (a delete needs only a key). Nulls in a rule evaluate to invalid (`coalesce(rule, false)`).
5. **Quarantine.** Invalid rows get the `reason` "failed range or operation check" and are merged into `customer_changes_quarantine` on `change_id` (insert only if new, so reruns do not duplicate).
6. **Deduplication.** For valid rows, `row_number()` over a window partitioned by `customer_id` and ordered by `event_ts` descending then `change_id` descending keeps only the latest event per customer. `event_ts` is cast to a timestamp (`event_time`).
7. **CDC `MERGE` into `customers_current`**, matched on `customer_id`:
   - Matched, `op = 'D'`, and event time newer than `last_event_ts`: delete.
   - Matched, `op` not D, and event time newer than `last_event_ts`: update the three features, `last_event_ts` and `last_change_id`.
   - Not matched and `op` not D: insert.
   Events older than the stored `last_event_ts` are ignored, which is how the late event loses. The `cdc_applied_batch` watermark advances after each batch.
8. **Integrity checks.** `customers_current` must have no duplicate `customer_id` and no row with `tenure_months <= 0`, `usage_score < 0` or `support_tickets < 0`.
9. **SCD Type 2 (`apply_scd2`).** Builds `customers_scd2` (`customer_id, tenure_months, usage_score, support_tickets, valid_from, valid_to, is_current, source_version`) from the Change Data Feed of `customers_current`:
   - Reads `table_changes` from the version after the `scd2_source_version` watermark (or 0) up to the latest table version, keeping change types `insert`, `update_postimage` and `delete`.
   - For each commit version in order: first closes the current history row for each affected key (`is_current = false`, `valid_to` set to the commit timestamp, only when `source_version` is older), then inserts a new current row (`valid_from` is the commit timestamp, `valid_to` is null, `is_current` true) for every non-delete change. A deleted customer is therefore closed with no new current row.
   - Updates the `scd2_source_version` watermark after each version.
   - Checks: only one current row per key, and a full outer join shows the current SCD2 rows equal `customers_current` on all feature columns. A mismatch fails the task.

**Outputs.** `raw_customer_changes` (read), `customers_current`, `customer_changes_quarantine`, `customers_scd2`, updated watermarks. A printed JSON gives applied batches, `current_rows`, `quarantined_rows`, `scd2_versions_applied`, `scd2_rows`, `scd2_current_rows`, `scd2_closed_rows`.

### Task 3: `batch_score` (`score`)

Function: `score`.

1. **Model selection.** `--model-version` (default and bundle value `1`) must be an explicit number, because Unity Catalog has no "latest" alias. The version `<catalog>.<schema>.churn_model` must have status `READY`. The model URI is `models:/<catalog>.<schema>.churn_model/1`.
2. **Scoring.** Reads `customers_current` (`customer_id` and the three features) and calls `FeatureEngineeringClient.score_batch(..., result_type='double')`. Because the feature columns are supplied explicitly, changed and newly inserted customers are scored on their current values rather than on the training lookups.
3. **Write.** Adds `scoring_run_id`, `model_uri` and `scored_at`, then merges into `churn_predictions` (`customer_id, prediction, scoring_run_id, model_uri, scored_at`) on `scoring_run_id` plus `customer_id`, inserting only new rows, so a retry cannot duplicate predictions.
4. **Checks.** The saved prediction count for this run must equal the number of current customers and every prediction must be 0.0 or 1.0 (no nulls). The five original fixture tables are re-hashed against the fixture and must be unchanged (`original_table_hashes`). The run must have a recorded landing batch, and the quarantined row count for that batch must equal the expected invalid count from `expected_effects`.
5. **Receipt.** A JSON receipt is written to `<bundle root>/runtime/platform-receipts/<run_id>.json` under `/Workspace/Users/<your user>/.bundle/data-platform-blueprint-demo/dev/`. The code refuses any path outside that folder or a filename other than the run id. The receipt includes batch number, landed and Auto Loader row counts, valid and quarantined counts, changed keys, the CDC and SCD2 watermarks, current, SCD2 current and closed row counts, prediction count, number predicted to churn, the model URI, the training run id, the SHA-256 digests of the five original tables, `source_tables_modified: false`, and the list of tables written.

**Failure behaviour.** Any failed check raises an exception and fails the task. Because writes are merge based with watermarks and per-run keys, rerunning a step does not duplicate data.

**Where to see Pipeline 2 results in the workspace.**

- Jobs & Pipelines: job `data-platform-blueprint-demo-platform-pipeline` (schedule shown as paused).
- Catalog Explorer: Volume `landing` (folder `customer_changes/batch_NNNN` and `_checkpoints`), and tables `raw_customer_changes`, `customer_changes_quarantine`, `customers_current`, `customers_scd2`, `churn_predictions`, `platform_batches`, `platform_watermarks`.
- Workspace files: `/Workspace/Users/<your user>/.bundle/data-platform-blueprint-demo/dev/runtime/platform-receipts/<run_id>.json`.

---

## Pipeline 3: `drift_monitor` (feature and prediction drift)

**Purpose.** Measure whether the current data distribution, the model's predictions and the label distribution have moved away from the data the registered model was trained on, using the Population Stability Index (PSI). The same script and arguments also run as the third task of `ml_pipeline`.

**Trigger and schedule.** No schedule. It is started manually, by `ml_pipeline`, or by the CI smoke run. Job timeout 900 seconds, task timeout 600 seconds.

Source: `databricks/src/mlops/monitoring/drift_monitor.py`, function `run`. Bundle arguments: `--model-version latest`, `--self-test`, `--batch-id {{job.run_id}}`, the experiment `/Users/<your user>/blueprint-drift-monitor`, the reference source `prepare_demo.py` and the receipt path under `runtime/drift-receipts/`. Script defaults: `--psi-threshold 0.2`, `--max-rows 10000` (allowed range 100 to 10000).

### Stages

1. **Identity checks.** Workspace host and operator must match the expected values. `--batch-id` must match `[A-Za-z0-9_.:@-]{1,200}`.
2. **Select the model.** With `latest`, the highest-numbered `READY` version of `<catalog>.<schema>.churn_model` is used (error if none). Otherwise the named version must be `READY`.
3. **Verify the reference.** The 500 reference rows are regenerated from `prepare_demo.py:sample_records`. A SHA-256 digest of them (same method as training, `training_digest`) must equal the `data_sha256` tag on the model's training MLflow run, and that run must be `FINISHED`. Otherwise: "The reference fixture does not match this model training run".
4. **Load the current batch.** `customer_features` left-joined to `labels` on `customer_id`, limited to `max_rows`. It fails if the batch is empty, larger than `max_rows`, has duplicate keys, or has a missing key.
5. **Self-test rows (when `--self-test` is set).** A third group, "shifted", is built in memory only: each reference row with 1000 added to tenure, usage and tickets. It is never written to any table.
6. **Score.** Rows with a non-finite or missing feature are excluded from scoring but still count in the quality profiles (their prediction is missing). The rest are scored with `score_batch` against the model URI. The code checks that no scored rows were duplicated or lost and that the reference group has 500 rows.
7. **Profile (`profile`).** For each monitored field (`tenure_months`, `usage_score`, `support_tickets`, `prediction`, `churn`) the reference values are binned. For features, bin edges are decile cut points (the values at 10 percent steps up to 90 percent, de-duplicated), with open outer bins so values beyond the training range are counted. For `prediction` and `churn` the single edge is 0.5. A profile stores edges, counts, row count and `missing_rate`.
8. **Baseline.** The reference profiles are stored in `drift_baselines` (`model_uri, training_run_id, training_data_sha256, profile_json`). If a baseline for that model URI already exists and differs in any value, the task fails ("explicit baseline review is required"); otherwise it is inserted once.
9. **Compare (`compare`).** Current values are binned with the reference edges. PSI is the sum over bins of `(q - p) * ln(q / p)`, where `p` and `q` are the reference and current shares, each smoothed by adding 0.5 to every bin count. The field status is `DRIFT` if PSI is at or above the threshold (0.2 by default), or if the missing rate changed by 0.05 or more. Otherwise it is `OK`.
10. **Self-test controls.** With `--self-test`, the reference compared with itself must give `OK` for every field, and the shifted group must give `DRIFT` for all three feature fields. If not, the task fails with "Drift detector controls failed". This proves the detector can detect both stability and drift on every run without writing fake alerts.
11. **Persist.** Rows are merged (insert if new) on `batch_id`, `model_uri` and `field` into:
    - `drift_metrics`: every field, with `psi`, `reference_missing_rate`, `current_missing_rate`, `current_rows`, `status`.
    - `drift_alerts`: only fields whose status is `DRIFT`.
    The code reads `drift_metrics` back and fails if the stored values differ, which catches a reused batch id with changed results.
12. **MLflow and receipt.** An MLflow run in `/Users/<your user>/blueprint-drift-monitor` logs metrics `max_psi`, `drift_alert_count` and `rows`, tags (`component`, `model_uri`, `batch_id`, `training_run_id`, `reference_data_sha256`), and the artifact `drift_receipt.json`. The same receipt is written to `/Workspace/Users/<your user>/.bundle/data-platform-blueprint-demo/dev/runtime/drift-receipts/<batch_id>.json` (path and filename are validated). The receipt holds the metrics, alert fields, the self-test results, the tables written, `source_tables_modified: false` and the threshold.
13. **Alerting.** If any field is in `DRIFT`, the results are saved first and then the task raises `DRIFT_ALERT: <fields>`. The job run fails visibly. There is no automatic email, retraining or model promotion.

**Outputs.** Tables `drift_baselines`, `drift_metrics`, `drift_alerts`; MLflow experiment `blueprint-drift-monitor`; receipt JSON files. The five source tables are read only.

**Where to see Pipeline 3 results in the workspace.**

- Jobs & Pipelines: job `data-platform-blueprint-demo-drift-monitor` (and the `drift_monitor` task inside `ml_pipeline`). A drift alert shows as a failed task with the `DRIFT_ALERT` message.
- Experiments: `blueprint-drift-monitor`, metrics `max_psi`, `drift_alert_count`, `rows`, artifact `drift_receipt.json`.
- Catalog Explorer: `drift_baselines`, `drift_metrics`, `drift_alerts`.
- Workspace files: `/Workspace/Users/<your user>/.bundle/data-platform-blueprint-demo/dev/runtime/drift-receipts/<run_id>.json`.

---

## The MLOps lifecycle across the three pipelines

1. **Data in.** `prepare_features` builds Bronze, Silver, labels, feature table and Gold, with quality checks. `ingest_changes` lands change files in a Volume and Auto Loader loads them incrementally into `raw_customer_changes`.
2. **Transform.** `cdc_scd2` quarantines invalid events, deduplicates to the latest event per customer, merges into `customers_current` and builds the `customers_scd2` history from the Change Data Feed.
3. **Train.** `train_and_register` trains a gradient boosting classifier on a stratified 80 percent split, evaluates on the 20 percent holdout (accuracy, F1, ROC AUC) and enforces an F1 gate of 0.5.
4. **Register.** The model is logged with its feature lookups and registered as `<catalog>.<schema>.churn_model` in Unity Catalog, with registry and MLflow readback checks and a receipt artifact.
5. **Score.** `batch_score` scores every current customer with a pinned model version and stores predictions with the model URI and run id.
6. **Monitor.** The drift monitor compares features, predictions and labels with the training reference using PSI (threshold 0.2) and missing-rate change (0.05), proves the detector works through in-memory controls, stores metrics and alerts, and fails the run on real drift.
7. **CI/CD.** `scripts/ci_cd.py` and the local git hook automate the deployment path (below).

### CI/CD with `scripts/ci_cd.py` and the local git hook

The hook is installed by `scripts/install_git_hook.py`. It writes a `post-commit` hook that runs `scripts/ci_cd.py`, and it refuses to overwrite an existing hook or a configured `core.hooksPath`. There is no GitHub Actions workflow. Delivery runs on a commit, never on a timer.

On each commit, `ci_cd.py` does the following:

1. **Local checks (`local_checks`).** Parses every Python file for syntax; fails if `.github/workflows` exists; validates the bundle (exactly the three jobs, no continuous or trigger settings, no classic clusters, only `platform_pipeline` may have a schedule and it must be `PAUSED`, one concurrent run, job timeout up to 900 s, task retries 0 and task timeouts up to 600 s, all source files present); then runs the unit tests in `tests/` (`test_drift.py` for the PSI logic and `test_platform.py` for the change batches, quarantine counts, bounded batch range, paused schedule, pinned model version and receipt validation). `python scripts/ci_cd.py --check` runs only this stage.
2. **Gates.** Deployment happens only on the branch named in `ci/config.json` (`deploy_branch`, `main`), only if `enabled` is true (it is `false` in the distributed config and is switched on in the untracked `.ci-local.json`), and only with a clean working tree. A lock prevents two local deployments at once. Receipts are kept outside the source tree, one per commit, and a commit that is already verified is not redeployed.
3. **Credentials and identity.** The token comes from the `DATABRICKS_TOKEN` environment variable if set, otherwise from the local credential broker key named in the config. The runner verifies the authenticated user and the original job, and refuses to deploy if the original pipeline has an active run. It saves the original job settings once so `scripts/rollback_pipeline.py` can restore them.
4. **Bundle deploy.** `databricks bundle validate --strict -t dev`, then `bundle deploy -t dev`, then `bundle summary`. It checks the original `ml_pipeline` job id is preserved, that the drift job has no schedule or trigger, and that the platform job schedule is paused.
5. **Smoke run.** It starts the `drift_monitor` job once (with an idempotency token), waits up to 960 seconds, requires every task to succeed, downloads the drift receipt from the workspace and verifies it (`verify_drift`: controls passed, source tables unmodified, matching batch id, no alert fields). It then starts the `platform_pipeline` job once and verifies its receipt (`verify_platform`: Auto Loader rows equal landed rows, quarantine present, CDC watermark equals the batch, SCD2 current rows equal current rows and some rows closed, predictions equal current rows with model version 1, five original table digests present).
6. **Result.** The commit receipt state becomes `verified` and `latest.json` is updated. Any failure is recorded in the receipt and the command exits with an error. A failed hook does not undo the commit; it blocks the deployment.

`scripts/preflight.py` is a read-only check (operator identity, catalog existence, strict bundle validation) that deploys nothing and starts no compute. `scripts/rollback_pipeline.py` restores the saved original job settings and disables automatic delivery.

## How to run it yourself

1. Set up your own Databricks workspace and a catalog and schema you can write to. Update the bundle variables (`workspace_host`, `catalog`, `schema`) in `infrastructure/databricks/asset-bundles/databricks.yml`, and the host, operator, operator id, original job id and receipt roots in `ci/config.json`, to match your workspace and user. The scripts compare these values with the live workspace and refuse to run if they differ.
2. Create a personal access token in your own workspace (Settings, Developer, Access tokens). Set it in your environment as `DATABRICKS_TOKEN`. The scripts support this directly (`authorized_environment` in `scripts/ci_cd.py` and `approved_token` in `scripts/preflight.py`). Do not write the token into any file in this repository.
3. Run the read-only checks: `python scripts/preflight.py --expected-operator <your user name>` and `python scripts/ci_cd.py --check`.
4. Deploy and run manually with the Databricks CLI from `infrastructure/databricks/asset-bundles`: `databricks bundle deploy -t dev`, then `databricks bundle run ml_pipeline -t dev`, then `databricks bundle run platform_pipeline -t dev` (it needs the model from the first run), and `databricks bundle run drift_monitor -t dev` for a standalone drift check.
5. Or use the automated path: create `.ci-local.json` with `{"enabled": true}` (and the CLI path if needed), run `python scripts/install_git_hook.py`, and commit on `main`.
