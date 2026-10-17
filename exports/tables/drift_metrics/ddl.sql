CREATE TABLE workspace.blueprint_demo.drift_metrics (
  batch_id STRING COLLATE UTF8_BINARY,
  observed_at STRING COLLATE UTF8_BINARY,
  model_uri STRING COLLATE UTF8_BINARY,
  field STRING COLLATE UTF8_BINARY,
  psi DOUBLE,
  reference_missing_rate DOUBLE,
  current_missing_rate DOUBLE,
  current_rows BIGINT,
  status STRING COLLATE UTF8_BINARY)
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

