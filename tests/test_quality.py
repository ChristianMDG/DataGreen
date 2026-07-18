import unittest
import pandas as pd
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from scripts.quality_checks import temporal_check, drift_check, alerts

class TestQuality(unittest.TestCase):
    def setUp(self):
        self.df = pd.DataFrame({
            'city': ['Paris', 'Paris', 'London', 'London'],
            'date': pd.date_range('2024-01-01', periods=4),
            'hour': [0, 12, 6, 18],
            'aqi': [1, 2, 3, 4],
            'pm2_5': [10, 20, 30, 40],
            'pm10': [15, 25, 35, 45]
        })

    def test_temporal(self):
        r = temporal_check(self.df)
        self.assertIn('date_min', r)
        self.assertIn('date_max', r)
        self.assertIn('hours', r)

    def test_drift(self):
        r = drift_check(self.df)
        self.assertIn('drift', r)

    def test_alerts(self):
        r = alerts(self.df)
        self.assertIsInstance(r, list)
        self.assertGreater(len(r), 0)

    def test_empty(self):
        empty = pd.DataFrame()
        self.assertEqual(temporal_check(empty), {})
        self.assertEqual(drift_check(empty), {'drift': False})

if __name__ == "__main__":
    unittest.main()
