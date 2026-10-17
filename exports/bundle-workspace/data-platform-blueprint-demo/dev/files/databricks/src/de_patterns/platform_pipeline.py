"""Bounded Databricks data platform path for the synthetic churn demo.

Steps (one job, run in order):
  ingest   Writes one deterministic synthetic change batch (inserts, updates, deletes,
           an out of order duplicate and one invalid row) as JSON files into a Unity
           Catalog Volume, then Auto Loader ingests new files with availableNow.
  cdc      Data quality split into valid rows and a quarantine table, then a CDC MERGE
           into customers_current (Change Data Feed enabled), then an SCD Type 2 history
           built from that table's Change Data Feed with a persisted version watermark.
  score    Batch scoring of the current customer state with the registered UC model
           version, written to churn_predictions, plus a verified run receipt.

The five original fixture tables are read only. Every table written here carries the
blueprint.component property and is refused if another owner created it. There are no
streams left running, no endpoints and no recurring compute started by this file.
"""
import argparse
from datetime import datetime, timedelta, timezone
import hashlib
import importlib.util
import json
from pathlib import Path
import random
import re

COMPONENT = 'blueprint-platform-v1'
FEATURES = ('tenure_months', 'usage_score', 'support_tickets')
MAX_BATCHES = 30
CHANGE_SCHEMA = ('change_id STRING, customer_id LONG, tenure_months INT, usage_score DOUBLE, '
                 'support_tickets INT, op STRING, event_ts STRING, batch_no INT')
RAW_COLUMNS = CHANGE_SCHEMA + ', source_file STRING, ingested_at TIMESTAMP'
ORIGINAL_TABLES = ('bronze_customers', 'silver_customers', 'labels', 'customer_features', 'gold_label_counts')


def qualified(catalog, schema):
    if not all(re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]{0,62}', part) for part in (catalog, schema)):
        raise ValueError('Use simple catalog and schema identifiers')
    return catalog + '.' + schema


def load_fixture(source):
    spec = importlib.util.spec_from_file_location('blueprint_platform_fixture', Path(source))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def change_batch(batch_no, base_rows):
    """Deterministic change events for one batch. Pure Python, unit tested locally."""
    if not 1 <= batch_no <= MAX_BATCHES:
        raise ValueError('Batch number is outside the bounded demo range')
    rng = random.Random(1000 + batch_no)
    start = datetime(2026, 10, 1, tzinfo=timezone.utc) + timedelta(days=batch_no)
    existing = [row[0] for row in base_rows]
    events = []

    def add(customer_id, months, usage, tickets, op, offset):
        stamp = (start + timedelta(seconds=offset)).strftime('%Y-%m-%dT%H:%M:%SZ')
        change_id = 'b%03d-%04d' % (batch_no, len(events))
        events.append((change_id, customer_id, months, usage, tickets, op, stamp, batch_no))

    by_id = {row[0]: row for row in base_rows}
    updated = rng.sample(existing, 20)
    for index, customer_id in enumerate(updated):
        _, months, usage, tickets, _ = by_id[customer_id]
        add(customer_id, months + batch_no, round(max(0.0, usage + rng.uniform(-15, 15)), 3),
            max(0, tickets + rng.randint(-2, 3)), 'U', 60 + index)
    # An older event for an already updated key arrives later in the file; ordering must win.
    late = updated[0]
    add(late, 1, 0.0, 0, 'U', 1)
    for k in range(5):
        add(100000 * batch_no + k, rng.randint(1, 12), round(rng.uniform(0, 100), 3), rng.randint(0, 8), 'I', 200 + k)
    deleted = rng.sample([value for value in existing if value not in updated], 2)
    for k, customer_id in enumerate(deleted):
        add(customer_id, None, None, None, 'D', 300 + k)
    # One invalid record proves the quarantine path; it must never reach customers_current.
    add(rng.choice(existing), -1, 10.0, 1, 'U', 400)
    return events


def expected_effects(events):
    """Latest valid event per key, the same rule the Spark MERGE applies."""
    valid = [e for e in events if valid_event(e)]
    latest = {}
    for event in sorted(valid, key=lambda e: (e[6], e[0])):
        latest[event[1]] = event
    return {'valid': len(valid), 'quarantined': len(events) - len(valid), 'keys': len(latest),
            'deletes': sum(1 for e in latest.values() if e[5] == 'D')}


def valid_event(event):
    _, customer_id, months, usage, tickets, op, _, _ = event
    if customer_id is None or op not in ('I', 'U', 'D'):
        return False
    if op == 'D':
        return True
    return months is not None and months > 0 and usage is not None and usage >= 0 and tickets is not None and tickets >= 0


def owned_table(spark, name, columns, cdf=False):
    properties = f"'blueprint.component' = '{COMPONENT}'"
    if cdf:
        properties += ", 'delta.enableChangeDataFeed' = 'true'"
    spark.sql(f'CREATE TABLE IF NOT EXISTS {name} ({columns}) USING DELTA TBLPROPERTIES ({properties})')
    actual = {row[0]: row[1] for row in spark.sql(f'SHOW TBLPROPERTIES {name}').collect()}
    if actual.get('blueprint.component') != COMPONENT:
        raise ValueError('Existing table has different ownership: ' + name)
    if cdf and actual.get('delta.enableChangeDataFeed') != 'true':
        raise ValueError('Change Data Feed is not enabled on ' + name)
    spark.sql(f'ALTER TABLE {name} DISABLE PREDICTIVE OPTIMIZATION')


def watermark(spark, ns, name):
    rows = spark.table(ns + '.platform_watermarks').where(f"name = '{name}'").collect()
    return int(rows[0].value) if rows else None


def set_watermark(spark, ns, name, value):
    stamp = datetime.now(timezone.utc).isoformat()
    spark.sql(f"MERGE INTO {ns}.platform_watermarks t USING (SELECT '{name}' AS name, {int(value)} AS value, "
              f"'{stamp}' AS updated_at) s ON t.name = s.name "
              f"WHEN MATCHED THEN UPDATE SET t.value = s.value, t.updated_at = s.updated_at "
              f"WHEN NOT MATCHED THEN INSERT (name, value, updated_at) VALUES (s.name, s.value, s.updated_at)")


def control_tables(spark, ns):
    owned_table(spark, ns + '.platform_watermarks', 'name STRING, value LONG, updated_at STRING')
    owned_table(spark, ns + '.platform_batches', 'run_id STRING, batch_no INT, landed_rows INT, created_at STRING')


def ingest(spark, ns, catalog, schema, run_id, fixture):
    control_tables(spark, ns)
    spark.sql(f'CREATE VOLUME IF NOT EXISTS {ns}.landing')
    base = f'/Volumes/{catalog}/{schema}/landing'
    known = spark.table(ns + '.platform_batches').where(f"run_id = '{run_id}'").collect()
    if known:
        batch_no = int(known[0].batch_no)
    else:
        highest = spark.table(ns + '.platform_batches').agg({'batch_no': 'max'}).collect()[0][0]
        batch_no = int(highest or 0) + 1
    events = change_batch(batch_no, fixture.sample_records())
    target = f'{base}/customer_changes/batch_{batch_no:04d}'
    frame = spark.createDataFrame(events, CHANGE_SCHEMA)
    try:
        landed = spark.read.schema(CHANGE_SCHEMA).json(target)
        landed_ids = sorted(row.change_id for row in landed.select('change_id').collect())
        if landed_ids != sorted(e[0] for e in events):
            raise ValueError('An existing landing batch differs from its deterministic content')
    except Exception as error:
        if 'PATH_NOT_FOUND' not in str(error) and 'does not exist' not in str(error):
            raise
        frame.coalesce(1).write.mode('error').json(target)
    if not known:
        stamp = datetime.now(timezone.utc).isoformat()
        spark.createDataFrame([(run_id, batch_no, len(events), stamp)],
                              'run_id STRING, batch_no INT, landed_rows INT, created_at STRING'
                              ).createOrReplaceTempView('blueprint_new_batch')
        spark.sql(f'MERGE INTO {ns}.platform_batches t USING blueprint_new_batch s ON t.batch_no = s.batch_no '
                  'WHEN NOT MATCHED THEN INSERT *')
        owner = spark.table(ns + '.platform_batches').where(f'batch_no = {batch_no}').collect()
        if len(owner) != 1 or owner[0].run_id != run_id:
            raise ValueError('Another run already owns this batch number')
    raw = ns + '.raw_customer_changes'
    owned_table(spark, raw, RAW_COLUMNS)
    from pyspark.sql import functions as F
    # Only data part files; writer marker files such as _SUCCESS are never ingested.
    stream = (spark.readStream.format('cloudFiles').option('cloudFiles.format', 'json')
              .option('pathGlobFilter', '*.json').schema(CHANGE_SCHEMA).load(base + '/customer_changes')
              .withColumn('source_file', F.col('_metadata.file_path'))
              .withColumn('ingested_at', F.current_timestamp()))
    query = (stream.writeStream.option('checkpointLocation', base + '/_checkpoints/raw_customer_changes')
             .trigger(availableNow=True).toTable(raw))
    query.awaitTermination()
    ingested = spark.table(raw).where(f'batch_no = {batch_no}')
    if ingested.count() != len(events) or ingested.select('change_id').distinct().count() != len(events):
        raise RuntimeError('Auto Loader did not ingest the landing batch exactly once')
    result = {'step': 'ingest', 'batch_no': batch_no, 'landed_rows': len(events), 'landing_path': target,
              'auto_loader_rows': len(events), 'raw_table': raw}
    print(json.dumps(result, sort_keys=True))
    return result


def apply_cdc(spark, ns):
    control_tables(spark, ns)
    current = ns + '.customers_current'
    owned_table(spark, current, 'customer_id LONG, tenure_months INT, usage_score DOUBLE, support_tickets INT, '
                'last_event_ts TIMESTAMP, last_change_id STRING', cdf=True)
    owned_table(spark, ns + '.customer_changes_quarantine', CHANGE_SCHEMA + ', reason STRING')
    if watermark(spark, ns, 'seed_done') is None:
        if spark.table(current).limit(1).count():
            raise ValueError('customers_current has rows but no seed watermark; refusing to guess its state')
        spark.sql(f"INSERT INTO {current} SELECT customer_id, tenure_months, usage_score, support_tickets, "
                  f"TIMESTAMP'1970-01-01 00:00:00', 'seed' FROM {ns}.silver_customers")
        set_watermark(spark, ns, 'seed_done', 1)
    applied = watermark(spark, ns, 'cdc_applied_batch') or 0
    pending = spark.table(ns + '.raw_customer_changes').where(f'batch_no > {applied}')
    batches = sorted(row.batch_no for row in pending.select('batch_no').distinct().collect())
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window
    for batch_no in batches:
        rows = pending.where(f'batch_no = {batch_no}').select(*CHANGE_SCHEMA_FIELDS)
        rule = (F.col('customer_id').isNotNull() & F.col('op').isin('I', 'U', 'D') &
                ((F.col('op') == 'D') | ((F.col('tenure_months') > 0) & (F.col('usage_score') >= 0) &
                                          (F.col('support_tickets') >= 0))))
        invalid = rows.where(~F.coalesce(rule, F.lit(False))).withColumn('reason', F.lit('failed range or operation check'))
        invalid.createOrReplaceTempView('blueprint_invalid_changes')
        spark.sql(f'MERGE INTO {ns}.customer_changes_quarantine t USING blueprint_invalid_changes s '
                  'ON t.change_id = s.change_id WHEN NOT MATCHED THEN INSERT *')
        order = Window.partitionBy('customer_id').orderBy(F.col('event_ts').desc(), F.col('change_id').desc())
        latest = (rows.where(F.coalesce(rule, F.lit(False)))
                  .withColumn('event_time', F.to_timestamp('event_ts'))
                  .withColumn('rank', F.row_number().over(order)).where('rank = 1').drop('rank'))
        latest.createOrReplaceTempView('blueprint_latest_changes')
        # Explicit columns: the upstream template's UPDATE SET * carried CDF metadata columns.
        spark.sql(f"""MERGE INTO {current} t USING blueprint_latest_changes s ON t.customer_id = s.customer_id
            WHEN MATCHED AND s.op = 'D' AND s.event_time > t.last_event_ts THEN DELETE
            WHEN MATCHED AND s.op <> 'D' AND s.event_time > t.last_event_ts THEN UPDATE SET
              t.tenure_months = s.tenure_months, t.usage_score = s.usage_score,
              t.support_tickets = s.support_tickets, t.last_event_ts = s.event_time, t.last_change_id = s.change_id
            WHEN NOT MATCHED AND s.op <> 'D' THEN INSERT
              (customer_id, tenure_months, usage_score, support_tickets, last_event_ts, last_change_id)
              VALUES (s.customer_id, s.tenure_months, s.usage_score, s.support_tickets, s.event_time, s.change_id)""")
        set_watermark(spark, ns, 'cdc_applied_batch', batch_no)
    if spark.table(current).count() != spark.table(current).select('customer_id').distinct().count():
        raise RuntimeError('customers_current has duplicate keys')
    if spark.table(current).where('tenure_months <= 0 OR usage_score < 0 OR support_tickets < 0').count():
        raise RuntimeError('An invalid change reached customers_current')
    history = apply_scd2(spark, ns, current)
    result = {'step': 'cdc', 'applied_batches': batches, 'cdc_applied_batch': watermark(spark, ns, 'cdc_applied_batch'),
              'current_rows': spark.table(current).count(),
              'quarantined_rows': spark.table(ns + '.customer_changes_quarantine').count(), **history}
    print(json.dumps(result, sort_keys=True, default=str))
    return result


CHANGE_SCHEMA_FIELDS = [part.strip().split(' ')[0] for part in CHANGE_SCHEMA.split(',')]


def apply_scd2(spark, ns, current):
    history = ns + '.customers_scd2'
    owned_table(spark, history, 'customer_id LONG, tenure_months INT, usage_score DOUBLE, support_tickets INT, '
                'valid_from TIMESTAMP, valid_to TIMESTAMP, is_current BOOLEAN, source_version LONG')
    done = watermark(spark, ns, 'scd2_source_version')
    latest_version = int(spark.sql(f'DESCRIBE HISTORY {current} LIMIT 1').collect()[0].version)
    start = 0 if done is None else done + 1
    versions = []
    if start <= latest_version:
        changes = spark.sql(f"SELECT * FROM table_changes('{current}', {start}, {latest_version}) "
                            "WHERE _change_type IN ('insert', 'update_postimage', 'delete')")
        changes.createOrReplaceTempView('blueprint_cdf_changes')
        versions = sorted(row[0] for row in changes.select('_commit_version').distinct().collect())
        for version in versions:
            spark.sql(f"""MERGE INTO {history} t USING (SELECT customer_id, _commit_timestamp AS ts,
                    _commit_version AS ver FROM blueprint_cdf_changes WHERE _commit_version = {version}) s
                ON t.customer_id = s.customer_id AND t.is_current AND t.source_version < s.ver
                WHEN MATCHED THEN UPDATE SET t.is_current = false, t.valid_to = s.ts""")
            spark.sql(f"""MERGE INTO {history} t USING (SELECT * FROM blueprint_cdf_changes
                    WHERE _commit_version = {version} AND _change_type <> 'delete') s
                ON t.customer_id = s.customer_id AND t.source_version = s._commit_version
                WHEN NOT MATCHED THEN INSERT (customer_id, tenure_months, usage_score, support_tickets,
                  valid_from, valid_to, is_current, source_version)
                VALUES (s.customer_id, s.tenure_months, s.usage_score, s.support_tickets,
                  s._commit_timestamp, NULL, true, s._commit_version)""")
            set_watermark(spark, ns, 'scd2_source_version', version)
        set_watermark(spark, ns, 'scd2_source_version', latest_version)
    open_rows = spark.table(history).where('is_current')
    if open_rows.count() != open_rows.select('customer_id').distinct().count():
        raise RuntimeError('SCD2 has more than one current row for a key')
    mismatch = spark.sql(f"""SELECT count(*) FROM {current} c FULL OUTER JOIN
            (SELECT * FROM {history} WHERE is_current) h ON c.customer_id = h.customer_id
            WHERE c.customer_id IS NULL OR h.customer_id IS NULL OR c.tenure_months <> h.tenure_months
               OR c.usage_score <> h.usage_score OR c.support_tickets <> h.support_tickets""").collect()[0][0]
    if mismatch:
        raise RuntimeError('SCD2 current rows differ from customers_current')
    return {'scd2_versions_applied': versions, 'scd2_rows': spark.table(history).count(),
            'scd2_current_rows': open_rows.count(), 'scd2_closed_rows': spark.table(history).where('NOT is_current').count()}


def score(spark, ns, run_id, model_version, fixture, operator, receipt_path):
    from databricks.feature_engineering import FeatureEngineeringClient
    from mlflow.tracking import MlflowClient
    import mlflow
    mlflow.set_registry_uri('databricks-uc')
    if not re.fullmatch(r'[0-9]{1,6}', model_version):
        raise ValueError('Use an explicit numeric model version; Unity Catalog has no latest alias')
    version = MlflowClient().get_model_version(ns + '.churn_model', model_version)
    if str(version.status) != 'READY':
        raise ValueError('The selected model version is not READY')
    model_uri = f'models:/{ns}.churn_model/{model_version}'
    frame = spark.table(ns + '.customers_current').select('customer_id', *FEATURES)
    expected = frame.count()
    # Explicit feature columns override the training lookups, so changed and new customers are scored on current values.
    scored = FeatureEngineeringClient().score_batch(model_uri=model_uri, df=frame, result_type='double')
    from pyspark.sql import functions as F
    output = (scored.select('customer_id', 'prediction')
              .withColumn('scoring_run_id', F.lit(run_id)).withColumn('model_uri', F.lit(model_uri))
              .withColumn('scored_at', F.current_timestamp()))
    table = ns + '.churn_predictions'
    owned_table(spark, table, 'customer_id LONG, prediction DOUBLE, scoring_run_id STRING, model_uri STRING, scored_at TIMESTAMP')
    output.createOrReplaceTempView('blueprint_scores')
    spark.sql(f'MERGE INTO {table} t USING blueprint_scores s ON t.scoring_run_id = s.scoring_run_id '
              'AND t.customer_id = s.customer_id WHEN NOT MATCHED THEN INSERT *')
    saved = spark.table(table).where(f"scoring_run_id = '{run_id}'")
    if saved.count() != expected or saved.where('prediction IS NULL OR prediction NOT IN (0.0, 1.0)').count():
        raise RuntimeError('Batch scoring readback differs from the current customer count or has invalid predictions')
    # The five original fixture tables must still match the delivered fixture exactly.
    original_hashes = original_table_hashes(spark, ns, fixture)
    batch = spark.table(ns + '.platform_batches').where(f"run_id = '{run_id}'").collect()
    if len(batch) != 1:
        raise RuntimeError('This run has no landing batch record')
    batch_no = int(batch[0].batch_no)
    events = change_batch(batch_no, fixture.sample_records())
    effects = expected_effects(events)
    quarantined = spark.table(ns + '.customer_changes_quarantine').where(f'batch_no = {batch_no}').count()
    if quarantined != effects['quarantined']:
        raise RuntimeError('Quarantine count differs from the expected invalid rows')
    receipt = {'run_id': run_id, 'component': COMPONENT, 'batch_no': batch_no, 'landed_rows': len(events),
        'auto_loader_rows': spark.table(ns + '.raw_customer_changes').where(f'batch_no = {batch_no}').count(),
        'valid_changes': effects['valid'], 'quarantined_rows': quarantined, 'changed_keys': effects['keys'],
        'cdc_applied_batch': watermark(spark, ns, 'cdc_applied_batch'),
        'scd2_source_version': watermark(spark, ns, 'scd2_source_version'),
        'current_rows': expected, 'scd2_rows': spark.table(ns + '.customers_scd2').count(),
        'scd2_current_rows': spark.table(ns + '.customers_scd2').where('is_current').count(),
        'scd2_closed_rows': spark.table(ns + '.customers_scd2').where('NOT is_current').count(),
        'predictions': saved.count(), 'predicted_churn': saved.where('prediction = 1.0').count(),
        'model_uri': model_uri, 'training_run_id': version.run_id,
        'original_table_sha256': original_hashes, 'source_tables_modified': False,
        'tables': [ns + '.' + name for name in ('raw_customer_changes', 'customer_changes_quarantine', 'customers_current',
                                                'customers_scd2', 'churn_predictions', 'platform_batches', 'platform_watermarks')]}
    if receipt['auto_loader_rows'] != len(events) or receipt['scd2_current_rows'] != expected:
        raise RuntimeError('Receipt consistency check failed')
    path = Path(receipt_path)
    prefix = '/Workspace/Users/' + operator + '/.bundle/data-platform-blueprint-demo/dev/runtime/platform-receipts/'
    if not str(path).startswith(prefix) or path.name != run_id + '.json':
        raise ValueError('The receipt path is outside the approved runtime directory')
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(receipt, sort_keys=True, default=str) + '\n', encoding='utf-8')
    print(json.dumps(receipt, sort_keys=True, default=str))
    return receipt


def original_table_hashes(spark, ns, fixture):
    rows = fixture.sample_records()
    from collections import Counter
    expected = {
        'bronze_customers': (['customer_id', *FEATURES, 'churn'], rows),
        'silver_customers': (['customer_id', *FEATURES, 'churn'], rows),
        'labels': (['customer_id', 'churn'], [(row[0], row[4]) for row in rows]),
        'customer_features': (['customer_id', *FEATURES], [row[:4] for row in rows]),
        'gold_label_counts': (['churn', 'count'], sorted(Counter(row[4] for row in rows).items())),
    }
    return {name: fixture.assert_table_matches(spark, ns + '.' + name, columns, values)
            for name, (columns, values) in expected.items()}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--step', required=True, choices=('ingest', 'cdc', 'score'))
    parser.add_argument('--catalog', required=True)
    parser.add_argument('--schema', default='blueprint_demo')
    parser.add_argument('--expected-host', required=True)
    parser.add_argument('--expected-operator', required=True)
    parser.add_argument('--run-id', required=True)
    parser.add_argument('--fixture-source', required=True)
    parser.add_argument('--model-version', default='1')
    parser.add_argument('--receipt-path', default='')
    args = parser.parse_args()
    if not re.fullmatch(r'[0-9]{1,30}', args.run_id):
        raise ValueError('Use the numeric job run identifier')
    from databricks.sdk import WorkspaceClient
    client = WorkspaceClient()
    if client.config.host.rstrip('/').lower() != args.expected_host.rstrip('/').lower():
        raise ValueError('Runtime workspace differs from the approved workspace')
    if client.current_user.me().user_name.casefold() != args.expected_operator.casefold():
        raise ValueError('Runtime operator differs from the approved operator')
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
    ns = qualified(args.catalog, args.schema)
    fixture = load_fixture(args.fixture_source)
    if args.step == 'ingest':
        ingest(spark, ns, args.catalog, args.schema, args.run_id, fixture)
    elif args.step == 'cdc':
        apply_cdc(spark, ns)
    else:
        score(spark, ns, args.run_id, args.model_version, fixture, args.expected_operator, args.receipt_path)


if __name__ == '__main__':
    main()
