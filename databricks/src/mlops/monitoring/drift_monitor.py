"""Bounded feature and prediction drift checks for the synthetic churn workflow.

Reference profiles are tied to the registered model's original training data.
Shifted self-test rows exist only in memory. The five source tables are read-only.
"""
import argparse
from bisect import bisect_right
from datetime import datetime, timezone
import hashlib
import importlib.util
import json
import math
from pathlib import Path
import re

FEATURES = ('tenure_months', 'usage_score', 'support_tickets')
COMPONENT = 'blueprint-drift-v1'


def finite_values(values):
    return [float(value) for value in values if value is not None and math.isfinite(float(value))]


def profile(values, edges=None):
    clean = sorted(finite_values(values))
    if not values or (not clean and edges is None):
        raise ValueError('A reference or current field has no finite values')
    if edges is None:
        # Outer bins remain open so values beyond the training range are counted.
        edges = sorted({clean[min(len(clean)-1, int(len(clean)*q/10))] for q in range(1, 10)})
    counts = [0] * (len(edges) + 1)
    for value in clean:
        counts[bisect_right(edges, value)] += 1
    return {'edges': edges, 'counts': counts, 'rows': len(values),
            'missing_rate': 1-len(clean)/len(values)}


def compare(reference, values, threshold=.2):
    if not 0 < threshold < 10:
        raise ValueError('A positive bounded PSI threshold is required')
    current = profile(values, reference['edges'])
    bins = len(reference['counts'])
    expected_total, actual_total = sum(reference['counts']), sum(current['counts'])
    psi = 0.0
    for expected, actual in zip(reference['counts'], current['counts']):
        p = (expected + .5)/(expected_total + .5*bins)
        q = (actual + .5)/(actual_total + .5*bins)
        psi += (q-p)*math.log(q/p)
    missing_change = abs(reference['missing_rate']-current['missing_rate'])
    return {'psi': psi, 'reference_missing_rate': reference['missing_rate'],
            'current_missing_rate': current['missing_rate'], 'current_rows': current['rows'],
            'status': 'DRIFT' if psi >= threshold or missing_change >= .05 else 'OK'}


def qualified(catalog, schema):
    if not all(re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]{0,62}', part) for part in (catalog, schema)):
        raise ValueError('Use simple catalog and schema identifiers')
    return catalog+'.'+schema


def reference_rows(source=None):
    if source is None:
        source = Path(__file__).resolve().parents[2]/'feature_store/ingestion/prepare_demo.py'
    source = Path(source)
    spec = importlib.util.spec_from_file_location('blueprint_reference_fixture', source)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.sample_records()


def training_digest(rows):
    import pandas as pd
    frame = pd.DataFrame(rows, columns=['customer_id', *FEATURES, 'churn'])
    columns = [*FEATURES, 'churn']
    frame = frame[columns].sort_values(columns, kind='stable').reset_index(drop=True)
    return hashlib.sha256(frame.to_json(orient='split', double_precision=8).encode()).hexdigest()


def owned_table(spark, name, columns):
    spark.sql(f"CREATE TABLE IF NOT EXISTS {name} ({columns}) USING DELTA "
              f"TBLPROPERTIES ('blueprint.component' = '{COMPONENT}')")
    properties = {row[0]: row[1] for row in spark.sql(f'SHOW TBLPROPERTIES {name}').collect()}
    if properties.get('blueprint.component') != COMPONENT:
        raise ValueError('Existing monitoring table has different ownership: '+name)
    spark.sql(f'ALTER TABLE {name} DISABLE PREDICTIVE OPTIMIZATION')


def merge_rows(spark, name, rows, columns, keys):
    if not rows:
        return
    view = 'blueprint_drift_'+hashlib.sha256(name.encode()).hexdigest()[:12]
    spark.createDataFrame(rows, columns).createOrReplaceTempView(view)
    condition = ' AND '.join(f't.{key}=s.{key}' for key in keys)
    spark.sql(f'MERGE INTO {name} t USING {view} s ON {condition} WHEN NOT MATCHED THEN INSERT *')


def run(args):
    from databricks.sdk import WorkspaceClient
    from databricks.feature_engineering import FeatureEngineeringClient
    from pyspark.sql import SparkSession
    import mlflow
    from mlflow.tracking import MlflowClient

    w = WorkspaceClient()
    if w.config.host.rstrip('/').lower() != args.expected_host.rstrip('/').lower():
        raise ValueError('Runtime workspace differs from the approved workspace')
    identity = w.current_user.me()
    if identity.user_name.casefold() != args.expected_operator.casefold():
        raise ValueError('Runtime operator differs from the approved operator')
    if not re.fullmatch(r'[A-Za-z0-9_.:@-]{1,200}', args.batch_id):
        raise ValueError('Use a short nonsecret batch identifier')
    ns = qualified(args.catalog, args.schema)
    spark = SparkSession.builder.getOrCreate()
    mlflow.set_registry_uri('databricks-uc')
    client = MlflowClient()
    model_name = ns+'.churn_model'
    if args.model_version == 'latest':
        ready = [v for v in client.search_model_versions("name = '"+model_name+"'") if str(v.status) == 'READY']
        if not ready:
            raise ValueError('No READY model is available')
        version = max(ready, key=lambda value: int(value.version))
    else:
        version = client.get_model_version(model_name, args.model_version)
    if str(version.status) != 'READY':
        raise ValueError('The selected registered model is not READY')
    model_uri = 'models:/'+model_name+'/'+str(version.version)
    training = client.get_run(version.run_id)
    reference = reference_rows(args.reference_source)
    digest = training_digest(reference)
    if training.info.status != 'FINISHED' or training.data.tags.get('data_sha256') != digest:
        raise ValueError('The reference fixture does not match this model training run')

    fields = ['customer_id', *FEATURES, 'churn']
    current_frame = spark.table(ns+'.customer_features').join(spark.table(ns+'.labels'), 'customer_id', 'left')
    current = [tuple(row) for row in current_frame.select(*fields).limit(args.max_rows+1).collect()]
    if not current or len(current) > args.max_rows or len({row[0] for row in current}) != len(current):
        raise ValueError('The current batch is empty, oversized or has duplicate keys')
    if any(row[0] is None for row in current):
        raise ValueError('A current primary key is missing')
    combined = [(*row, 'reference') for row in reference] + [(*row, 'current') for row in current]
    if args.self_test:
        combined += [(row[0], row[1]+1000, row[2]+1000.0, row[3]+1000, row[4], 'shifted') for row in reference]
    valid = [row for row in combined if all(row[index] is not None and math.isfinite(float(row[index]))
                                          for index in (1, 2, 3))]
    frame = spark.createDataFrame(valid,
        'customer_id LONG, tenure_months INT, usage_score DOUBLE, support_tickets INT, churn INT, case_name STRING')
    # Explicit feature columns override lookups, preserving an independent training reference.
    scored = FeatureEngineeringClient().score_batch(model_uri=model_uri, df=frame, result_type='double')
    scored_rows = scored.select(*fields, 'case_name', 'prediction').collect()
    predictions = {(row.case_name, row.customer_id): row.prediction for row in scored_rows}
    if len(predictions) != len(valid):
        raise ValueError('Batch scoring duplicated or lost valid input rows')
    # Invalid feature rows remain in quality profiles, with a missing prediction.
    cases = {name: [dict(zip([*fields, 'case_name'], row),
                        prediction=predictions.get((row[-1], row[0])))
                   for row in combined if row[-1] == name]
             for name in ('reference', 'current', 'shifted')}
    if len(cases['reference']) != 500 or len(cases['current']) != len(current):
        raise ValueError('Batch scoring changed the expected row counts')
    monitored = [*FEATURES, 'prediction', 'churn']
    reference_profiles = {field: profile([row[field] for row in cases['reference']],
        edges=[.5] if field in ('prediction', 'churn') else None) for field in monitored}
    reference_json = json.dumps(reference_profiles, sort_keys=True)
    baseline_columns = 'model_uri STRING, training_run_id STRING, training_data_sha256 STRING, profile_json STRING'
    owned_table(spark, ns+'.drift_baselines', baseline_columns)
    old = spark.table(ns+'.drift_baselines').where(f"model_uri = '{model_uri}'").limit(2).collect()
    if old and (len(old) != 1 or old[0].training_run_id != version.run_id or
                old[0].training_data_sha256 != digest or old[0].profile_json != reference_json):
        raise ValueError('Stored training reference differs; explicit baseline review is required')
    merge_rows(spark, ns+'.drift_baselines', [(model_uri, version.run_id, digest, reference_json)],
               baseline_columns, ['model_uri'])
    metrics = {field: compare(reference_profiles[field], [row[field] for row in cases['current']], args.psi_threshold)
               for field in monitored}
    controls = {}
    if args.self_test:
        negative = {field: compare(reference_profiles[field], [row[field] for row in cases['reference']], args.psi_threshold)
                    for field in monitored}
        positive = {field: compare(reference_profiles[field], [row[field] for row in cases['shifted']], args.psi_threshold)
                    for field in monitored}
        if any(result['status'] != 'OK' for result in negative.values()) or not all(
                positive[field]['status'] == 'DRIFT' for field in FEATURES):
            raise RuntimeError('Drift detector controls failed')
        controls = {'unchanged_reference': negative, 'shifted_in_memory': positive,
                    'source_tables_modified': False, 'controls_passed': True}
    now = datetime.now(timezone.utc).isoformat()
    metric_columns = ('batch_id STRING, observed_at STRING, model_uri STRING, field STRING, psi DOUBLE, '
                      'reference_missing_rate DOUBLE, current_missing_rate DOUBLE, current_rows LONG, status STRING')
    rows = [(args.batch_id, now, model_uri, field, result['psi'], result['reference_missing_rate'],
             result['current_missing_rate'], result['current_rows'], result['status']) for field, result in metrics.items()]
    for suffix in ('drift_metrics', 'drift_alerts'):
        owned_table(spark, ns+'.'+suffix, metric_columns)
        selected = rows if suffix == 'drift_metrics' else [row for row in rows if row[-1] == 'DRIFT']
        merge_rows(spark, ns+'.'+suffix, selected, metric_columns, ['batch_id', 'model_uri', 'field'])
    saved = spark.table(ns+'.drift_metrics').where(f"batch_id = '{args.batch_id}' AND model_uri = '{model_uri}'").collect()
    if len(saved) != len(monitored) or {row.field for row in saved} != set(monitored):
        raise RuntimeError('Persisted drift metrics failed readback')
    for row in saved:
        expected = metrics[row.field]
        if row.status != expected['status'] or row.current_rows != expected['current_rows'] or not math.isclose(
                row.psi, expected['psi'], abs_tol=1e-10):
            raise RuntimeError('A batch identifier was reused for changed monitoring results')
    alerts = [field for field, result in metrics.items() if result['status'] == 'DRIFT']
    receipt = {'batch_id': args.batch_id, 'model_uri': model_uri, 'training_run_id': version.run_id,
        'reference_data_sha256': digest, 'reference_rows': 500, 'current_rows': len(current),
        'metrics': metrics, 'alert_fields': alerts, 'self_test': controls,
        'tables': [ns+'.drift_baselines', ns+'.drift_metrics', ns+'.drift_alerts'],
        'source_tables_modified': False, 'threshold_psi': args.psi_threshold}
    mlflow.set_experiment(args.experiment)
    with mlflow.start_run() as monitoring_run:
        receipt['monitoring_mlflow_run_id'] = monitoring_run.info.run_id
        mlflow.set_tags({'component': COMPONENT, 'model_uri': model_uri, 'batch_id': args.batch_id,
                         'training_run_id': version.run_id, 'reference_data_sha256': digest})
        mlflow.log_metrics({'max_psi': max(result['psi'] for result in metrics.values()),
                            'drift_alert_count': len(alerts), 'rows': len(current)})
        mlflow.log_dict(receipt, 'drift_receipt.json')
    receipt_path = Path(args.receipt_path)
    expected_prefix = '/Workspace/Users/'+args.expected_operator+'/.bundle/data-platform-blueprint-demo/dev/runtime/drift-receipts/'
    if not str(receipt_path).startswith(expected_prefix) or receipt_path.name != args.batch_id+'.json':
        raise ValueError('The receipt path is outside the approved runtime directory')
    receipt_path.parent.mkdir(parents=True, exist_ok=True)
    receipt_path.write_text(json.dumps(receipt, sort_keys=True)+'\n', encoding='utf-8')
    print(json.dumps(receipt, sort_keys=True))
    if alerts:
        raise RuntimeError('DRIFT_ALERT: '+', '.join(alerts)+'. Inspect drift_metrics and drift_alerts; no automatic retraining occurred.')
    return receipt


if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('--catalog', required=True)
    p.add_argument('--schema', default='blueprint_demo')
    p.add_argument('--expected-host', required=True)
    p.add_argument('--expected-operator', required=True)
    p.add_argument('--experiment', required=True)
    p.add_argument('--model-version', default='latest')
    p.add_argument('--reference-source', required=True)
    p.add_argument('--receipt-path', required=True)
    p.add_argument('--batch-id', required=True)
    p.add_argument('--max-rows', type=int, default=10000)
    p.add_argument('--psi-threshold', type=float, default=.2)
    p.add_argument('--self-test', action='store_true')
    arguments = p.parse_args()
    if not 100 <= arguments.max_rows <= 10000:
        p.error('max-rows must be between 100 and 10000')
    run(arguments)
