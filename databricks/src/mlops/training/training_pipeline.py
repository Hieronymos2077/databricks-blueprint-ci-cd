"""Adapted MIT data-platform-blueprint training template for an AWS serverless trial.

Upstream commit: 029a1c20fc666590a0fd93182bb71bf4a583b548.
Keeps FeatureLookup, feature-store model packaging, MLflow and Unity Catalog.
Corrects the upstream evaluation on training data. No endpoint or schedule.
See LICENSE for the upstream copyright and permission notice.
"""
import argparse
import hashlib
import json
import logging
import math
from typing import Optional

logger = logging.getLogger('training_databricks')


class ModelTrainerDatabricks:
    def __init__(self, cfg: Optional[dict] = None):
        cfg = cfg or {}
        from pyspark.sql import SparkSession
        import mlflow
        self.spark = SparkSession.builder.getOrCreate()
        self.mlflow = mlflow
        self.EXPERIMENT_NAME = cfg['experiment_name']
        self.MODEL_NAME = cfg['model_name']
        self.FEATURE_TABLE = cfg['feature_table']
        self.LABEL_TABLE = cfg['label_table']
        self.PRIMARY_KEYS = ['customer_id']
        self.LABEL = 'churn'
        self.FEATURE_NAMES = ['tenure_months', 'usage_score', 'support_tickets']
        self._fe = None
        mlflow.set_registry_uri('databricks-uc')
        mlflow.set_experiment(self.EXPERIMENT_NAME)

    @property
    def fe(self):
        if self._fe is None:
            from databricks.feature_engineering import FeatureEngineeringClient
            self._fe = FeatureEngineeringClient()
        return self._fe

    def _get_training_data(self):
        from databricks.feature_engineering import FeatureLookup
        labels = self.spark.table(self.LABEL_TABLE)
        if labels.count() != 500: raise ValueError('This smoke run requires the bounded 500-row fixture')
        training_set = self.fe.create_training_set(df=labels,
            feature_lookups=[FeatureLookup(table_name=self.FEATURE_TABLE, feature_names=self.FEATURE_NAMES,
                                          lookup_key=self.PRIMARY_KEYS)],
            label=self.LABEL, exclude_columns=self.PRIMARY_KEYS)
        frame = training_set.load_df().select(*self.FEATURE_NAMES, self.LABEL).toPandas()
        split, digest = self._split_frame(frame)
        return training_set, split, digest

    def _split_frame(self, frame):
        from sklearn.model_selection import train_test_split
        if len(frame) != 500 or frame.isna().any().any(): raise ValueError('Feature join failed readback')
        frame = frame[self.FEATURE_NAMES + [self.LABEL]].sort_values(
            self.FEATURE_NAMES + [self.LABEL], kind='stable').reset_index(drop=True)
        X, y = frame[self.FEATURE_NAMES], frame[self.LABEL]
        if set(y.unique()) != {0, 1}: raise ValueError('Both classes are required')
        split = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)
        if set(split[0].index) & set(split[1].index): raise ValueError('Training and holdout overlap')
        digest = hashlib.sha256(frame.to_json(orient='split', double_precision=8).encode()).hexdigest()
        return split, digest

    def _train_model(self, X, y):
        from sklearn.ensemble import GradientBoostingClassifier
        model = GradientBoostingClassifier(n_estimators=100, max_depth=3, learning_rate=0.1, random_state=42)
        model.fit(X, y)
        return model

    def _evaluate(self, model, X, y):
        from sklearn.metrics import accuracy_score, f1_score, roc_auc_score
        predictions = model.predict(X)
        positive_index = list(model.classes_).index(1)
        probabilities = model.predict_proba(X)[:, positive_index]
        return {'accuracy':float(accuracy_score(y, predictions)),
                'f1':float(f1_score(y, predictions, zero_division=0)),
                'roc_auc':float(roc_auc_score(y, probabilities))}

    def _log_and_register(self, model, training_set, metrics, run_id):
        from mlflow.tracking import MlflowClient
        self.fe.log_model(model=model, artifact_path='model', flavor=self.mlflow.sklearn,
            training_set=training_set, registered_model_name=self.MODEL_NAME, infer_input_example=True,
            await_registration_for=120)
        client = MlflowClient()
        versions = [v for v in client.search_model_versions("name = '" + self.MODEL_NAME + "'") if v.run_id == run_id]
        if len(versions) != 1: raise RuntimeError('Exact model-version registration readback is not verified')
        version = client.get_model_version(self.MODEL_NAME, versions[0].version)
        if str(version.status) != 'READY': raise RuntimeError('Registered model is not ready')
        if version.run_id != run_id: raise RuntimeError('Registered model provenance differs from this run')
        return str(version.version)

    def _verify_final_run(self, client, receipt):
        saved = client.get_run(receipt['run_id'])
        if saved.info.status != 'FINISHED' or saved.data.tags.get('registration_verified') != 'true':
            raise RuntimeError('MLflow final run readback did not confirm completion')
        for key, value in receipt['metrics'].items():
            if not math.isclose(saved.data.metrics.get(key, float('nan')), value, rel_tol=1e-10):
                raise RuntimeError('MLflow metric readback differs: ' + key)
        artifacts = {artifact.path for artifact in client.list_artifacts(receipt['run_id'])}
        model_artifacts = {artifact.path for artifact in client.list_artifacts(receipt['run_id'], 'model')}
        if 'deployment_receipt.json' not in artifacts or 'model/MLmodel' not in model_artifacts:
            raise RuntimeError('The receipt or packaged model artifact is missing')

    def run(self):
        import mlflow.sklearn
        training_set, split, digest = self._get_training_data()
        X_train, X_test, y_train, y_test = split
        with self.mlflow.start_run() as run:
            self.mlflow.log_params({'fixture':'synthetic_churn_500','train_rows':len(X_train),
                'holdout_rows':len(X_test),'random_seed':42})
            self.mlflow.set_tags({'upstream_commit':'029a1c20fc666590a0fd93182bb71bf4a583b548',
                                 'data_sha256':digest,'purpose':'deployment_smoke'})
            model = self._train_model(X_train, y_train)
            metrics = self._evaluate(model, X_test, y_test)
            self.mlflow.log_metrics(metrics)
            receipt = {'run_id':run.info.run_id,'metrics':metrics,'train_rows':len(X_train),
                       'holdout_rows':len(X_test),'model_name':self.MODEL_NAME,'data_sha256':digest}
            if metrics['f1'] < .5:
                self.mlflow.log_dict(dict(receipt, registered=False), 'deployment_receipt.json')
                raise RuntimeError('Holdout gate failed; model was not registered')
            version = self._log_and_register(model, training_set, metrics, run.info.run_id)
            receipt.update(model_version=version, registered=True)
            self.mlflow.log_dict(receipt, 'deployment_receipt.json')
            self.mlflow.set_tag('registration_verified', 'true')
        from mlflow.tracking import MlflowClient
        self._verify_final_run(MlflowClient(), receipt)
        print(json.dumps(receipt, sort_keys=True))
        return receipt


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--catalog', required=True)
    parser.add_argument('--schema', default='blueprint_demo')
    parser.add_argument('--experiment', required=True)
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
    import re
    if not all(re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]{0,62}', value) for value in (args.catalog, args.schema)):
        raise ValueError('Use simple catalog and schema identifiers')
    name = args.catalog + '.' + args.schema
    ModelTrainerDatabricks({'experiment_name':args.experiment, 'model_name':name+'.churn_model',
                           'feature_table':name+'.customer_features','label_table':name+'.labels'}).run()
