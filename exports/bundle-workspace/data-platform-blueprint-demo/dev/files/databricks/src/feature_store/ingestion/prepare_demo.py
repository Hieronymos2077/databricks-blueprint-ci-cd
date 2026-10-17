"""Small synthetic fixture for the MIT data-platform-blueprint deployment demo.

No real customer data, downloads, cloud credentials or scheduled execution.
"""
import argparse
from collections import Counter
import hashlib
import json
import random
import re


def namespace(catalog, schema):
    for value in (catalog, schema):
        if not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]{0,62}', value):
            raise ValueError('Use a simple catalog/schema identifier')
    return catalog + '.' + schema


def sample_records():
    rng = random.Random(42)
    rows = []
    for identifier in range(500):
        months = rng.randint(1, 60)
        usage = round(rng.uniform(0, 100), 3)
        tickets = rng.randint(0, 8)
        score = -2.0 + 0.5 * tickets - 0.035 * months - 0.025 * usage + rng.uniform(-0.75, 0.75)
        rows.append((identifier, months, usage, tickets, int(score > 0)))
    return rows


def assert_table_matches(spark, table, columns, expected):
    selected = spark.table(table).select(*columns).limit(len(expected) + 1)
    if selected.count() != len(expected):
        raise ValueError('Existing demo table has a different row count: ' + table)
    actual = [tuple(row) for row in selected.orderBy(*columns).collect()]
    expected = sorted(expected)
    actual_digest = hashlib.sha256(json.dumps(actual).encode()).hexdigest()
    expected_digest = hashlib.sha256(json.dumps(expected).encode()).hexdigest()
    if actual_digest != expected_digest:
        raise ValueError('Existing demo table differs from the fixture; no overwrite performed: ' + table)
    return actual_digest


def prepare(spark, catalog, schema):
    from databricks.feature_engineering import FeatureEngineeringClient
    from pyspark.sql import functions as F
    name = namespace(catalog, schema)
    rows = sample_records()
    bronze = name + '.bronze_customers'
    silver = name + '.silver_customers'
    labels = name + '.labels'
    feature_table = name + '.customer_features'
    gold = name + '.gold_label_counts'
    columns = ['customer_id', 'tenure_months', 'usage_score', 'support_tickets', 'churn']
    tables = [
        (bronze, columns, rows),
        (silver, columns, rows),
        (labels, ['customer_id', 'churn'], [(row[0], row[4]) for row in rows]),
        (feature_table, columns[:4], [row[:4] for row in rows]),
        (gold, ['churn', 'count'], sorted(Counter(row[4] for row in rows).items())),
    ]
    # Check every existing table before creating or writing any demo resource.
    schema_exists = spark.catalog.databaseExists(name)
    for table, selected_columns, expected_rows in tables:
        if schema_exists and spark.catalog.tableExists(table):
            assert_table_matches(spark, table, selected_columns, expected_rows)
    fe = FeatureEngineeringClient()
    if schema_exists and spark.catalog.tableExists(feature_table) and list(fe.get_table(name=feature_table).primary_keys) != ['customer_id']:
        raise ValueError('Existing feature table has different primary keys')
    spark.sql(f'CREATE SCHEMA IF NOT EXISTS {name}')
    if not schema_exists:
        spark.sql(f'ALTER SCHEMA {name} DISABLE PREDICTIVE OPTIMIZATION')
    raw = spark.createDataFrame(rows, 'customer_id LONG, tenure_months INT, usage_score DOUBLE, support_tickets INT, churn INT')
    # These names are isolated to the agreed demo schema. Repeat runs preserve the fixture.
    if not spark.catalog.tableExists(bronze): raw.write.format('delta').mode('error').saveAsTable(bronze)
    actual = spark.table(bronze)
    count = actual.count()
    if count != 500 or actual.select('customer_id').distinct().count() != 500:
        raise ValueError('Existing demo Bronze table differs from the expected small fixture')
    valid = actual.filter((F.col('tenure_months') > 0) & (F.col('usage_score') >= 0) &
                         (F.col('support_tickets') >= 0) & F.col('churn').isin(0, 1))
    if valid.count() != count: raise ValueError('Demo input failed its data-quality check')
    if not spark.catalog.tableExists(silver): valid.write.format('delta').mode('error').saveAsTable(silver)
    if not spark.catalog.tableExists(labels):
        valid.select('customer_id', 'churn').write.format('delta').mode('error').saveAsTable(labels)
    features = valid.drop('churn')
    if not spark.catalog.tableExists(feature_table):
        fe.create_table(name=feature_table, primary_keys=['customer_id'], df=features,
                        description='Small synthetic customer-churn fixture for a deployment smoke run')
    stored = fe.get_table(name=feature_table)
    if list(stored.primary_keys) != ['customer_id']:
        raise ValueError('Existing feature table has different primary keys')
    if spark.table(feature_table).count() != count or spark.table(labels).count() != count:
        raise ValueError('Feature or label table readback count differs from Bronze')
    if not spark.catalog.tableExists(gold):
        valid.groupBy('churn').count().write.format('delta').mode('error').saveAsTable(gold)
    for table, _, _ in tables:
        spark.sql(f'ALTER TABLE {table} DISABLE PREDICTIVE OPTIMIZATION')
    hashes = {table: assert_table_matches(spark, table, selected_columns, expected_rows)
              for table, selected_columns, expected_rows in tables}
    result = {'rows':count, 'feature_table':feature_table, 'label_table':labels, 'gold_table':gold,
              'primary_keys':list(stored.primary_keys), 'table_sha256':hashes, 'data_quality_verified':True}
    print(json.dumps(result, sort_keys=True))
    return result


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--catalog', required=True)
    parser.add_argument('--schema', default='blueprint_demo')
    parser.add_argument('--expected-host', required=True)
    args = parser.parse_args()
    from urllib.parse import urlparse
    from databricks.sdk import WorkspaceClient
    expected = args.expected_host.rstrip('/').lower()
    parsed = urlparse(expected)
    if parsed.scheme != 'https' or not (parsed.hostname or '').endswith('.cloud.databricks.com') or parsed.path or parsed.query:
        raise ValueError('A verified AWS Databricks workspace URL is required')
    if WorkspaceClient().config.host.rstrip('/').lower() != expected:
        raise ValueError('Runtime workspace differs from the reviewed AWS target')
    from pyspark.sql import SparkSession
    prepare(SparkSession.builder.getOrCreate(), args.catalog, args.schema)
