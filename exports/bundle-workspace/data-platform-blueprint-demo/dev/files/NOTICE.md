# Source and adaptations

This package adapts [Data Platform Blueprint](https://github.com/toretto17/data-platform-blueprint/tree/029a1c20fc666590a0fd93182bb71bf4a583b548), commit `029a1c20fc666590a0fd93182bb71bf4a583b548`, under its MIT license. Preserve LICENSE with copies.

The adapted files are the Databricks training template and deployment bundle. The original training class, FeatureLookup, FeatureEngineeringClient packaging, MLflow tracking and Unity Catalog registration approach remain. Configuration now points at a small synthetic customer-churn fixture, with held-out evaluation and actual registration readback. The original evaluates on training data, which is corrected here.

The bundle uses manual serverless execution instead of the original classic EC2 cluster and daily schedule. Relative source paths resolve from the actual bundle location. The small fixture supplies the feature and label tables that the original template assumed already existed. It contains no real customer data.

This is a deployment demo, not an assertion that every capability advertised by the broad template has been deployed. No serving endpoint, online feature store, continuous stream, Azure migration, paid subscription or production rollout is included. Broader testing and evaluation remain the later agreed funded phase.
