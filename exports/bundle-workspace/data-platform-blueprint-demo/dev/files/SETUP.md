# AWS Databricks blueprint setup

This package adapts the supplied MIT repository's Databricks training and bundle templates. Its bounded deployment completed successfully on October 13, 2026. [ARCHITECTURE.md](ARCHITECTURE.md) explains the repository design and the verified demo flow.

## Trial and hosting

Use a client-owned AWS Databricks workspace. The earlier Azure workspace is not the selected target. Verify the exact workspace owner, cloud, region, URL and authorized operator before uploads or runs.

The email-based [express trial](https://docs.databricks.com/aws/en/getting-started/express-setup) provides serverless compute and default storage without a payment method. These run in Databricks-managed AWS infrastructure, outside the client's own AWS account. Databricks trial credits cover this usage, not the client's AWS credits. Confirm this hosting boundary is acceptable before using it.

The [AWS Marketplace trial](https://docs.databricks.com/aws/en/getting-started/free-trial) can convert to paid billing after the trial or credits end. Run only within verified trial coverage. Do not link Marketplace billing, add payment details, upgrade the AWS account or create a classic cluster as a workaround. The account owner completes any signup, terms acceptance, identity verification and secure operator access themselves.

Before compute, read the trial's actual remaining credits and expiration in Manage trial, identify the existing billing method and Marketplace linkage, and confirm this run is fully covered by the allowance. The published trial lasts 14 days. Personal-email trials can limit jobs and notebooks to 50 DBUs per hour, provide CPU only, and restrict external network access. These published limits do not establish the allowance or billing state of this particular account.

No model endpoint, SQL warehouse, schedule or continuous workload is created by this package. Run one small job manually, with one concurrent run, no task retries, a five-minute limit per task and a fifteen-minute limit for the job. Verify that no run remains active afterward. Trial assets must be exported before expiration; they are not permanent storage.

The selected catalog enables predictive optimization by inheritance, which can run billed serverless maintenance automatically. Preparation disables it on a newly created demo schema and on these five demo tables. Existing unrelated schemas, tables and catalog settings are preserved. Verify the demo schema and table settings remain disabled after the run.

## Requirements

Serverless environment version 4 supplies Python 3.12, scikit-learn 1.6.1 and MLflow 2.22. The bundle adds Databricks Feature Engineering 0.18.1, whose package metadata was checked. Let the platform resolve that small additional dependency; do not clone the runtime or installed dependencies into the source tree.

Use an existing Unity Catalog catalog in which the operator can create the isolated `blueprint_demo` schema and its demo tables/model. Do not change account-wide permissions or use an existing production schema. If a table with the demo name already exists with different data, the job stops rather than overwriting it.

## Configure and deploy

Use Databricks bundle tooling with the client-owned authorized session. Authentication stays in the approved secret broker or authorized browser session. Do not write OAuth tokens, personal access tokens, passwords or access keys into configuration files. A workspace URL and catalog name are nonsecret configuration.

The configuration targets `https://dbc-939dcb75-eac7.cloud.databricks.com`, catalog `workspace` and schema `blueprint_demo`. The existing client-delegated API operator was verified as `lawsuser4@gmail.com`, and native execution established access to the demo tables, MLflow and model registry. The invited browser user remains `takundahove62@gmail.com`. Configuration defaults alone do not establish access or credit coverage. For reproduction in another approved workspace, override them only after verifying that workspace's identity and allowance.

```text
databricks bundle validate --strict -t dev
databricks bundle deploy -t dev
databricks bundle run ml_pipeline -t dev
```

For another approved workspace, first set the bundle's `workspace.host` to that exact origin. Authentication fields require a literal host and do not support bundle-variable interpolation. Then append `--var workspace_host=APPROVED_WORKSPACE_URL,catalog=APPROVED_CATALOG,schema=ISOLATED_SCHEMA` consistently to validate, deploy and run. The task host guard must match the authentication host. Do not execute against an unverified account. Bundle deployment uploads the reviewed source and creates the manual job; it does not prove a successful run. If a run result is uncertain, inspect that run ID before any retry. An existing different table is a reason to select a new isolated schema, not overwrite that table.

## Read-only preflight

Use the official [Databricks CLI](https://docs.databricks.com/aws/en/dev-tools/cli/install). The offline configuration check used CLI 1.20.0. No CLI authentication cache or credential file is included in this package.

Before the deployment commands, use `scripts/preflight.py` to check the authenticated operator, catalog readback and strict bundle validation. It does not deploy or run compute. It accepts an existing approved access token through a private process environment or an existing DPAPI broker key, captures provider output privately, and never writes the token to a file. It does not create a token, log in, add permissions or certify trial coverage. Obtain the authorized OAuth session through the existing secure sign-in path after membership is granted.

Run this from the package root, replacing the operator and broker key with approved nonsecret identifiers:

```text
python scripts/preflight.py --cli databricks --expected-operator APPROVED_OPERATOR_EMAIL --credential-key EXISTING_APPROVED_OAUTH_BROKER_KEY
```

On this deployment PC, the shared CLI executable is `C:/Users/arete/AppData/Local/Programs/DatabricksCLI/v1.20.0/databricks.exe`; pass that path with `--cli` if it is not on PATH. The broker key must contain an existing authorized workspace access token. Never use the email verification code as an API token or pass a credential value as a command argument. The helper deliberately reports that schema-creation permission and trial allowance remain unverified; confirm those before deploy or run.

## Access and allowance before another run

1. Use an approved operator or explicitly delegated API session for Databricks account `a484b8ca-0893-4fb9-a142-ab251c64e15c` and workspace `7474660191855131`. Running the job requires access to its isolated schema, tables, feature table and model in `workspace`. An AWS IAM user or workspace link alone does not grant Databricks membership. Access for the completed deployment was verified separately through the client-provided API session.
2. Make Manage Trial available for inspection, or provide a current owner view of the remaining allowance, exact expiry and billing linkage for that same account. This can be done without granting full account administration. Confirm that the bounded manual run is fully covered by the active trial.

No password, authorization code, personal access token, access key, new payment method or subscription approval is needed in chat. No further dataset or model objective is needed for this selected synthetic deployment demo. Agreed real-data testing and evaluation remain the next funded phase.

## Core journey and verification

The first task creates a 500-row synthetic customer-churn fixture, Bronze and Silver Delta tables, labels, a Unity Catalog feature table with `customer_id` as primary key, and a small Gold label-count aggregate. It checks every existing demo table's content before creating or writing resources. After preparation, it reads back exact fixture content and hashes, primary keys and quality results. This is educational data, not customer information.

The second task keeps the upstream GradientBoostingClassifier and FeatureLookup workflow. It splits 400 training rows from 100 held-out rows, trains only on the training split, records holdout metrics in MLflow, packages feature lookup metadata with FeatureEngineeringClient.log_model and registers the actual model in Unity Catalog when the gate passes. It then reads back the exact version linked to that run and the final MLflow run state.

Verify the actual successful run ID, row counts, primary key metadata, non-overlapping split, MLflow experiment and recorded metrics, saved model artifact, Unity Catalog model name/version and run provenance. The output must contain a real `deployment_receipt.json` artifact and a FINISHED MLflow run. A deployment response, created job or local syntax check is not enough.

This minimal check belongs to the funded deployment phase. Broader performance testing, evaluation on agreed data, acceptance comparison and additional capability work belong to the next funded phase. The synthetic holdout scores do not establish business performance.

The training task waits at most two minutes for registration within its five-minute task limit. Final readback requires a READY version linked to that run, matching stored metrics, a FINISHED run, `deployment_receipt.json` and `model/MLmodel`. A missing artifact causes failure rather than a completed-deployment claim.

## Walkthrough after access is granted

1. Sign in as the invited operator and verify the exact workspace URL and account. Check Frankfurt, serverless availability, trial allowance and expiry before starting the job. Do not start the existing SQL warehouse just to inspect the demo.
2. Complete read-only preflight and confirm permission for the isolated schema. Run strict bundle validation. A permission or billing blocker stops here; preserve existing data and account settings.
3. Deploy once. Open the returned job URL and confirm exactly `prepare_features` followed by `train_and_register`, serverless environment 4, and no schedule, endpoint, classic cluster or automatic retries.
4. Start one manual run and keep its job ID and run ID. Verify both tasks report success. Use `databricks jobs get-run RUN_ID --output json` to read actual task states; the deployment response alone does not establish completion.
5. Inspect preparation output for 500 rows, `customer_id` primary keys, five table hashes and `data_quality_verified: true`. The four row-level tables hold 500 rows each; Gold holds two label-count aggregates.
6. Open the MLflow run from job output. Inspect the 400 training rows, 100 held-out rows, stored metrics, packaged model files and `deployment_receipt.json`. In Unity Catalog, confirm the exact `workspace.blueprint_demo.churn_model` version is READY and linked to that run. The training task also performs these readbacks.
7. Retain source, nonsecret variables, job/run IDs and the actual receipt. Reproduce with the same source in another approved isolated schema. Repeating preparation in the original matching schema preserves its fixture; another training run creates a new model version and must be intentional.

`NOTICE.md` explains the changes from the supplied template. This package retains its feature lookup, GradientBoosting model, MLflow and Unity Catalog approach, supplies missing demo data and corrects training-data evaluation. It replaces classic clusters and daily execution with this bounded manual run. It does not claim deployment of the broad production platform, SCD2 workflow, continuous ingestion, online feature store or serving endpoint.

## Reproduction and stopping

Reproduce using the same reviewed code and bundle variables in an isolated schema. Reference the existing data location rather than copying operational data into each version. Retain the deployment parameters, source and successful run receipt for rollback.

Stop by leaving the manual job idle and verifying no run is active. There is no schedule or serving endpoint to keep running. Do not automatically delete schemas, tables, model versions, notebooks or files. Any retirement is an explicit operator action that must preserve needed data and exports.

After completion, run `databricks jobs list-runs --job-id JOB_ID --active-only --output json`. The active-run list must be empty. If this demo's own run remains active, cancel only that run with `databricks jobs cancel-run RUN_ID --timeout 60s`, then read it again to confirm its terminal state. A cancellation response alone does not prove compute stopped. Do not cancel other jobs or the owner's existing warehouse.

Finish the owner's reproduction before retiring resources. Preserve the synthetic fixture through its source generator rather than copying a dataset into another version. Do not use `bundle destroy` or delete the workspace as automatic cleanup. Remove owned demo files only through verified recoverable controls after explicit approval. Retain model versions and tables when deletion has no verified recovery path. Do not cancel subscriptions or alter billing as cleanup.

## Verified deployment

Local checks exercised creation and repeat preparation through an in-memory Spark adapter and rejected different content in all five tables before writes. The actual scikit-learn 1.6.1 model trained on 400 rows and evaluated 100 held-out rows, with synthetic accuracy 0.93, F1 0.588235 and ROC AUC 0.974359. Reloading the serialized model reproduced those metrics. These local smoke results do not establish cloud execution or business performance.

The configuration passed authenticated strict validation with Databricks CLI 1.20.0. The source sync roots include both task files, and the authentication host is a literal approved URL. Native serverless Spark uses create-only write mode `error`, and FeatureEngineeringClient.get_table is called with its required named argument. The source archive contains code, configuration, license and documentation only; its explicit allowlist excludes runtime caches, datasets and model binaries.

Completed October 13, 2026, in AWS workspace `7474660191855131`, Frankfurt (`eu-central-1`):

* Job `789648897090823`, successful run `984854810547896`, both tasks report SUCCESS.
* Preparation verified the 500-row synthetic fixture, five exact table-content hashes, feature primary key `customer_id` and data quality. Existing matching tables were preserved during the successful rerun.
* MLflow experiment `2817437476331785`, run `a7f9a3ce9ad043a492c1fccd801984ef` is FINISHED. It records 400 training rows and 100 separate held-out rows, accuracy 0.93, F1 0.5882352941176471 and ROC AUC 0.9743589743589743.
* Unity Catalog model `workspace.blueprint_demo.churn_model`, version `1`, is READY and linked to that exact MLflow run and workspace. Its feature dependency is `workspace.blueprint_demo.customer_features`.
* Native artifact listings confirm `deployment_receipt.json` and `model/MLmodel`. The job has no active run, and all five demo tables report predictive optimization DISABLE.

The first two runs exposed the write-mode and named-argument compatibility errors, which were corrected. They are terminal failures; the successful run above is the release receipt. There are no automatic retries, schedules, classic clusters, SQL warehouses or serving endpoints created by this package. Broader evaluation and real-data testing remain a separate funded phase.

The owner reported USD 400 trial credit and expiry October 26, 2026. The authorized bounded run proceeded on that quota assurance. Billing linkage was not independently established, and AWS credits were not treated as Databricks allowance. Recheck allowance and expiry before any additional compute.

Open the [deployed job](https://dbc-939dcb75-eac7.cloud.databricks.com/jobs/789648897090823?o=7474660191855131) in the client-owned workspace. Inspect the successful run listed above. Source files and this guide are delivered separately from the trained model, which remains in the existing Unity Catalog and MLflow storage.
