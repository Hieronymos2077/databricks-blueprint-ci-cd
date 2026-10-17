CREATE TABLE workspace.blueprint_demo.customer_features (
  customer_id BIGINT NOT NULL,
  tenure_months INT,
  usage_score DOUBLE,
  support_tickets INT,
  CONSTRAINT `customer_features_pk` PRIMARY KEY (`customer_id`))
USING delta
COMMENT 'Small synthetic customer-churn fixture for a deployment smoke run'
TBLPROPERTIES (
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.appendOnly' = 'supported',
  'delta.feature.deletionVectors' = 'supported',
  'delta.feature.invariants' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7',
  'delta.parquet.compression.codec' = 'zstd',
  'delta.parquet.format.version' = '2.12.0',
  'delta.parquet.format.version.afe.internal' = '2.12.0')

