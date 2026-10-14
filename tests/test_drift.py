import importlib.util
import math
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('drift', ROOT/'databricks/src/mlops/monitoring/drift_monitor.py')
drift = importlib.util.module_from_spec(spec)
spec.loader.exec_module(drift)


class DriftTests(unittest.TestCase):
    def test_unchanged_batch_is_not_drift(self):
        values = list(range(500))
        result = drift.compare(drift.profile(values), values)
        self.assertEqual(result['status'], 'OK')
        self.assertAlmostEqual(result['psi'], 0)

    def test_out_of_range_shift_is_detected(self):
        reference = drift.profile(list(range(500)))
        result = drift.compare(reference, list(range(1000, 1500)))
        self.assertEqual(result['status'], 'DRIFT')
        self.assertGreater(result['psi'], .2)
        self.assertTrue(math.isfinite(result['psi']))

    def test_prediction_distribution_change_is_detected(self):
        reference = drift.profile([0]*450+[1]*50, [.5])
        self.assertEqual(drift.compare(reference, [1]*500)['status'], 'DRIFT')

    def test_missing_values_are_detected(self):
        reference = drift.profile(list(range(500)))
        self.assertEqual(drift.compare(reference, [None]*100+list(range(400)))['status'], 'DRIFT')

    def test_empty_or_nonfinite_batch_fails_closed(self):
        for values in ([], [None], [float('nan')]):
            with self.assertRaises(ValueError):
                drift.profile(values)

    def test_all_missing_current_values_raise_a_drift_alert(self):
        reference = drift.profile(list(range(500)))
        self.assertEqual(drift.compare(reference, [None]*500)['status'], 'DRIFT')

    def test_fixture_and_identifiers(self):
        self.assertEqual(len(drift.reference_rows()), 500)
        with self.assertRaises(ValueError):
            drift.qualified('workspace', 'x; drop table')


if __name__ == '__main__':
    unittest.main()
