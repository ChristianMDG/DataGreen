import unittest
import pandas as pd
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from scripts.transform import transform_basic, transform_complete, transform_final

class TestTransform(unittest.TestCase):
    def setUp(self):
        self.sample = [
            {
                'dt': 1704067200,
                'main': {'aqi': 1},
                'components': {'pm2_5': 10.5, 'pm10': 15.2, 'co': 0.5, 'no': 0.1, 'no2': 0.3, 'o3': 0.2, 'so2': 0.1},
                'city': 'Paris'
            },
            {
                'dt': 1704153600,
                'main': {'aqi': 2},
                'components': {'pm2_5': 20.3, 'pm10': 25.1, 'co': 0.8, 'no': 0.2, 'no2': 0.5, 'o3': 0.3, 'so2': 0.2},
                'city': 'London'
            }
        ]

    def test_basic_columns(self):
        df = transform_basic(self.sample)
        cols = ['city', 'date', 'hour', 'aqi', 'pm2_5', 'pm10', 'co', 'no', 'no2', 'o3', 'so2']
        for c in cols:
            self.assertIn(c, df.columns)

    def test_basic_length(self):
        df = transform_basic(self.sample)
        self.assertEqual(len(df), 2)

    def test_basic_date(self):
        df = transform_basic(self.sample)
        self.assertEqual(str(df['date'].iloc[0]), '2024-01-01')

    def test_complete_columns(self):
        df = transform_complete(self.sample)
        cols = ['city', 'date', 'hour', 'day', 'month', 'year', 'season', 'weekend',
                'aqi', 'pm2_5', 'pm10', 'co', 'no', 'no2', 'o3', 'so2', 'pm_ratio', 'aqi_cat', 'quality']
        for c in cols:
            self.assertIn(c, df.columns)

    def test_complete_length(self):
        df = transform_complete(self.sample)
        self.assertEqual(len(df), 2)

    def test_complete_season(self):
        df = transform_complete(self.sample)
        self.assertEqual(df['season'].iloc[0], 'Winter')

    def test_complete_weekend(self):
        df = transform_complete(self.sample)
        self.assertEqual(df['weekend'].iloc[0], False)

    def test_complete_aqi_cat(self):
        df = transform_complete(self.sample)
        self.assertEqual(df['aqi_cat'].iloc[0], 'Good')
        self.assertEqual(df['aqi_cat'].iloc[1], 'Fair')

    def test_complete_pm_ratio(self):
        df = transform_complete(self.sample)
        self.assertEqual(round(df['pm_ratio'].iloc[0], 2), 0.69)

    def test_final_columns(self):
        df = transform_final(self.sample)
        self.assertIn('p_date', df.columns)

    def test_final_length(self):
        df = transform_final(self.sample)
        self.assertEqual(len(df), 2)

    def test_empty(self):
        empty = []
        self.assertTrue(transform_basic(empty).empty)
        self.assertTrue(transform_complete(empty).empty)
        self.assertTrue(transform_final(empty).empty)

    def test_missing_data(self):
        data = [{'dt': 1704067200, 'main': {'aqi': 1}, 'components': {}, 'city': 'Paris'}]
        df = transform_basic(data)
        self.assertEqual(len(df), 1)

if __name__ == "__main__":
    unittest.main()
