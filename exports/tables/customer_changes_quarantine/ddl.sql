CREATE TABLE workspace.blueprint_demo.customer_changes_quarantine (
  change_id STRING COLLATE UTF8_BINARY,
  customer_id BIGINT,
  tenure_months INT,
  usage_score DOUBLE,
  support_tickets INT,
  op STRING COLLATE UTF8_BINARY,
  event_ts STRING COLLATE UTF8_BINARY,
  batch_no INT,
  reason STRING COLLATE UTF8_BINARY)
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

