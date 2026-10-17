CREATE TABLE workspace.blueprint_demo.drift_baselines (
  model_uri STRING COLLATE UTF8_BINARY,
  training_run_id STRING COLLATE UTF8_BINARY,
  training_data_sha256 STRING COLLATE UTF8_BINARY,
  profile_json STRING COLLATE UTF8_BINARY)
USING delta
TBLPROPERTIES (
  'blueprint.component' = 'blueprint-drift-v1',
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

