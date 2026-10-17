# Architecture and data flow

![Repository architecture and verified demo](diagrams/architecture.png)

Open [the vector diagram](diagrams/architecture.svg) for a sharper view. The editable source is [architecture.d2](diagrams/architecture.d2).

The upper section summarizes the supplied repository: raw sources enter Bronze, cleaning produces Silver, and Gold holds business aggregates. Gold feeds analytical consumption and feature preparation. The ML lifecycle trains, evaluates, registers and scores models, with optional drift feedback. These are repository templates, not a claim that the whole platform is deployed.

The middle section shows alternative AWS service and Databricks implementations. The selected demo uses Databricks on managed AWS infrastructure in Frankfurt. It did not provision separate Glue, SageMaker, S3, EventBridge, Step Functions or CloudWatch resources in the client's AWS account.

The green section is the flow verified in this deployment:

1. Generate 500 synthetic customer rows with seed 42.
2. `prepare_features` creates Bronze and Silver, derives labels and the `customer_features` table, and builds Gold label-count aggregates. It checks exact content and the feature primary key before training. Existing matching fixtures are preserved.
3. `train_and_register` joins labels with features through `FeatureLookup`. It trains GradientBoosting on 400 rows and evaluates a separate 100-row holdout.
4. MLflow retains metrics, the feature-aware model artifact and `deployment_receipt.json`.
5. Unity Catalog retains `workspace.blueprint_demo.churn_model`, version 1, in READY state, linked to that exact MLflow run.

The successful Databricks run is `984854810547896`. Both tasks report SUCCESS. The manual job has no active run or schedule. Inference services, continuous ingestion, drift monitoring and broader real-data evaluation are outside this bounded demo.

Source: the supplied [MIT repository](https://github.com/toretto17/data-platform-blueprint/tree/029a1c20fc666590a0fd93182bb71bf4a583b548), its pinned [README](https://github.com/toretto17/data-platform-blueprint/blob/029a1c20fc666590a0fd93182bb71bf4a583b548/README.md) and [architecture guide](https://github.com/toretto17/data-platform-blueprint/blob/029a1c20fc666590a0fd93182bb71bf4a583b548/docs/architecture/END_TO_END_FLOW.md). The green section reflects native job, table, MLflow and model readbacks, rather than repository documentation alone.
