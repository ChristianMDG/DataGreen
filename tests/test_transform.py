import unittest
import pandas as pd
import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from scripts.transform import basic, complete

class TestTransform(unittest.TestCase):
    def test_basic(self):
        d = [{'dt': 1704067200, 'main': {'aqi': 1}, 'components': {'pm2_5': 10.5, 'pm10': 15.2}, 'city': 'Paris'}]
        df = basic(pd.DataFrame(d))
        self.assertEqual(len(df), 1)
        self.assertIn('date', df.columns)

    def test_complete(self):
        d = [{'dt': 1704067200, 'main': {'aqi': 1}, 'components': {'pm2_5': 10.5, 'pm10': 15.2}, 'city': 'Paris'}]
        df = complete(pd.DataFrame(d))
        self.assertEqual(len(df), 1)
        self.assertIn('season', df.columns)
        self.assertIn('aqi_cat', df.columns)

    def test_empty(self):
        self.assertTrue(basic(pd.DataFrame()).empty)
        self.assertTrue(complete(pd.DataFrame()).empty)

if __name__ == "__main__":
    unittest.main()
