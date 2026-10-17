CREATE TABLE workspace.blueprint_demo.churn_predictions (
  customer_id BIGINT,
  prediction DOUBLE,
  scoring_run_id STRING COLLATE UTF8_BINARY,
  model_uri STRING COLLATE UTF8_BINARY,
  scored_at TIMESTAMP)
USING delta
TBLPROPERTIES (
  'blueprint.component' = 'blueprint-platform-v1',
  'delta.enableDeletionVectors' = 'true',
  'delta.enableRowTracking' = 'true',
  'delta.feature.appendOnly' = 'supported',
  'delta.feature.deletionVectors' = 'supported',
  'delta.feature.domainMetadata' = 'supported',
  'delta.feature.invariants' = 'supported',
  'delta.feature.rowTracking' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7',
  'delta.parquet.compression.codec' = 'zstd',
  'delta.parquet.format.version' = '2.12.0',
  'delta.parquet.format.version.afe.internal' = '2.12.0')

