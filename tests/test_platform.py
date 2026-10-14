import importlib.util
import json
from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]


def load(name, relative):
    spec = importlib.util.spec_from_file_location(name, ROOT/relative)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


platform = load('platform_pipeline', 'databricks/src/de_patterns/platform_pipeline.py')
fixture = load('prepare_demo', 'databricks/src/feature_store/ingestion/prepare_demo.py')
sys.path.insert(0, str(ROOT/'scripts'))
ci_cd = load('ci_cd', 'scripts/ci_cd.py')


class ChangeBatchTests(unittest.TestCase):
    def setUp(self):
        self.base = fixture.sample_records()

    def test_batches_are_deterministic_and_unique(self):
        first = platform.change_batch(1, self.base)
        self.assertEqual(first, platform.change_batch(1, self.base))
        self.assertNotEqual(first, platform.change_batch(2, self.base))
        self.assertEqual(len({event[0] for event in first}), len(first))

    def test_batch_contains_every_change_type_and_one_invalid_row(self):
        events = platform.change_batch(3, self.base)
        self.assertEqual({event[5] for event in events}, {'I', 'U', 'D'})
        self.assertEqual(sum(1 for event in events if not platform.valid_event(event)), 1)

    def test_late_event_loses_to_newer_update(self):
        events = platform.change_batch(4, self.base)
        late = events[20]
        newer = [event for event in events if event[1] == late[1] and event is not late and platform.valid_event(event)]
        self.assertTrue(newer)
        self.assertLess(late[6], newer[0][6])

    def test_expected_effects_counts(self):
        events = platform.change_batch(5, self.base)
        effects = platform.expected_effects(events)
        self.assertEqual(effects['quarantined'], 1)
        self.assertEqual(effects['valid'], len(events)-1)
        self.assertEqual(effects['deletes'], 2)
        # 20 updates (one key also has a late event, and the invalid row is excluded), 5 inserts, 2 deletes.
        self.assertEqual(effects['keys'], 27)

    def test_batch_range_is_bounded(self):
        for value in (0, platform.MAX_BATCHES+1):
            with self.assertRaises(ValueError):
                platform.change_batch(value, self.base)

    def test_identifiers_are_validated(self):
        with self.assertRaises(ValueError):
            platform.qualified('workspace', 'demo; DROP TABLE x')


class BundleTests(unittest.TestCase):
    def setUp(self):
        self.jobs = json.loads((ROOT/'infrastructure/databricks/asset-bundles/databricks.yml').read_text(encoding='utf-8'))['resources']['jobs']

    def test_platform_schedule_is_paused(self):
        schedule = self.jobs['platform_pipeline']['schedule']
        self.assertEqual(schedule['pause_status'], 'PAUSED')
        self.assertTrue(ci_cd.paused_schedule(schedule))
        self.assertFalse(ci_cd.paused_schedule(dict(schedule, pause_status='UNPAUSED')))
        self.assertFalse(any('schedule' in job for name, job in self.jobs.items() if name != 'platform_pipeline'))

    def test_platform_scores_a_pinned_model_version(self):
        score = self.jobs['platform_pipeline']['tasks'][2]['spark_python_task']['parameters']
        self.assertEqual(score[score.index('--model-version')+1], '1')


class ReceiptTests(unittest.TestCase):
    def receipt(self, **changes):
        value = {'run_id': '7', 'source_tables_modified': False, 'landed_rows': 29, 'auto_loader_rows': 29,
                 'quarantined_rows': 1, 'batch_no': 1, 'cdc_applied_batch': 1, 'current_rows': 503,
                 'scd2_current_rows': 503, 'scd2_closed_rows': 22, 'predictions': 503,
                 'model_uri': 'models:/workspace.blueprint_demo.churn_model/1',
                 'original_table_sha256': {str(index): 'x' for index in range(5)}}
        value.update(changes)
        return value

    def test_valid_receipt_passes(self):
        ci_cd.verify_platform(self.receipt(), 7)

    def test_inconsistent_receipts_fail(self):
        for changes in ({'run_id': '8'}, {'auto_loader_rows': 58}, {'quarantined_rows': 0},
                        {'cdc_applied_batch': 0}, {'scd2_current_rows': 502}, {'predictions': 1},
                        {'model_uri': 'models:/x/2'}, {'source_tables_modified': True}):
            with self.assertRaises(ValueError):
                ci_cd.verify_platform(self.receipt(**changes), 7)


if __name__ == '__main__':
    unittest.main()
