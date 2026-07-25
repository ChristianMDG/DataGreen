import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

sys.path.insert(
    0,
    os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
)

from scripts import transform


class TestTransform(unittest.TestCase):

    def test_clean_dataframe_removes_invalid_rows(self):
        """Checks that rows without a city or timestamp are removed."""

        dataframe = pd.DataFrame([
            {
                "city": "Paris",
                "timestamp": "2026-01-01T10:00:00",
                "aqi": 2
            },
            {
                "city": None,
                "timestamp": "2026-01-01T11:00:00",
                "aqi": 3
            },
            {
                "city": "Rome",
                "timestamp": None,
                "aqi": 4
            }
        ])

        result = transform.clean_dataframe(dataframe)

        self.assertEqual(len(result), 1)
        self.assertEqual(result.iloc[0]["city"], "Paris")
        self.assertEqual(
            result.iloc[0]["timestamp"],
            "2026-01-01T10:00:00"
        )

    def test_clean_dataframe_removes_duplicates_and_keeps_last(self):
        """
        Checks that duplicate city and timestamp rows are removed
        and that the last row is kept.
        """

        dataframe = pd.DataFrame([
            {
                "city": "Paris",
                "timestamp": "2026-01-01T10:00:00",
                "aqi": 2
            },
            {
                "city": "Paris",
                "timestamp": "2026-01-01T10:00:00",
                "aqi": 4
            }
        ])

        result = transform.clean_dataframe(dataframe)

        self.assertEqual(len(result), 1)
        self.assertEqual(result.iloc[0]["aqi"], 4)

    def test_clean_dataframe_sorts_rows_and_resets_index(self):
        """Checks that rows are sorted and the index is reset."""

        dataframe = pd.DataFrame([
            {
                "city": "Paris",
                "timestamp": "2026-01-02T10:00:00",
                "aqi": 2
            },
            {
                "city": "Berlin",
                "timestamp": "2026-01-02T08:00:00",
                "aqi": 1
            },
            {
                "city": "Paris",
                "timestamp": "2026-01-01T09:00:00",
                "aqi": 3
            }
        ])

        result = transform.clean_dataframe(dataframe)

        self.assertEqual(
            result["city"].tolist(),
            ["Berlin", "Paris", "Paris"]
        )

        self.assertEqual(
            result["timestamp"].tolist(),
            [
                "2026-01-02T08:00:00",
                "2026-01-01T09:00:00",
                "2026-01-02T10:00:00"
            ]
        )

        self.assertEqual(result.index.tolist(), [0, 1, 2])

    def test_clean_dataframe_returns_empty_dataframe(self):
        """Checks that an empty result remains a DataFrame."""

        dataframe = pd.DataFrame([
            {
                "city": None,
                "timestamp": None,
                "aqi": 2
            }
        ])

        result = transform.clean_dataframe(dataframe)

        self.assertTrue(result.empty)
        self.assertIsInstance(result, pd.DataFrame)

    def test_transform_returns_none_when_no_json_file_exists(self):
        """Checks that the transformation stops when no JSON file exists."""

        with tempfile.TemporaryDirectory() as temporary_folder:
            raw_folder = os.path.join(temporary_folder, "raw")
            clean_folder = os.path.join(temporary_folder, "clean")

            os.makedirs(raw_folder)

            with patch.object(
                transform,
                "RAW_FOLDER",
                raw_folder
            ):
                with patch.object(
                    transform,
                    "CLEAN_FOLDER",
                    clean_folder
                ):
                    result = transform.transform_air_quality_data()

            self.assertIsNone(result)
            self.assertFalse(os.path.exists(clean_folder))

    def test_transform_creates_csv_from_valid_json(self):
        """Checks that valid raw JSON data is transformed into a CSV file."""

        raw_data = {
            "city": "Paris",
            "data": {
                "list": [
                    {
                        "dt": 1767225600,
                        "main": {
                            "aqi": 2
                        },
                        "components": {
                            "co": 100.0,
                            "no": 1.0,
                            "no2": 15.0,
                            "o3": 40.0,
                            "pm2_5": 12.0,
                            "pm10": 20.0,
                            "so2": 5.0,
                            "nh3": 2.0
                        }
                    }
                ]
            }
        }

        with tempfile.TemporaryDirectory() as temporary_folder:
            raw_folder = os.path.join(temporary_folder, "raw")
            clean_folder = os.path.join(temporary_folder, "clean")

            os.makedirs(raw_folder)

            json_path = os.path.join(
                raw_folder,
                "backfill_Paris.json"
            )

            with open(json_path, "w", encoding="utf-8") as json_file:
                json.dump(raw_data, json_file)

            with patch.object(
                transform,
                "RAW_FOLDER",
                raw_folder
            ):
                with patch.object(
                    transform,
                    "CLEAN_FOLDER",
                    clean_folder
                ):
                    output_file = (
                        transform.transform_air_quality_data()
                    )

            self.assertIsNotNone(output_file)
            self.assertTrue(os.path.exists(output_file))
            self.assertEqual(
                os.path.basename(output_file),
                "air_quality.csv"
            )

            result = pd.read_csv(output_file)

            self.assertEqual(len(result), 1)
            self.assertEqual(result.iloc[0]["city"], "Paris")
            self.assertEqual(result.iloc[0]["country"], "FR")
            self.assertAlmostEqual(
                result.iloc[0]["latitude"],
                48.8566
            )
            self.assertAlmostEqual(
                result.iloc[0]["longitude"],
                2.3522
            )
            self.assertEqual(result.iloc[0]["aqi"], 2)
            self.assertEqual(result.iloc[0]["pm2_5"], 12.0)
            self.assertEqual(result.iloc[0]["pm10"], 20.0)

    def test_transform_supports_alternative_json_format(self):
        """
        Checks that the transformation supports a city stored inside
        the data object and measurements stored in a top-level list.
        """

        raw_data = {
            "data": {
                "city": "London"
            },
            "list": [
                {
                    "dt": 1767225600,
                    "main": {
                        "aqi": 3
                    },
                    "components": {
                        "co": 110.0,
                        "no": 2.0,
                        "no2": 16.0,
                        "o3": 42.0,
                        "pm2_5": 14.0,
                        "pm10": 22.0,
                        "so2": 6.0,
                        "nh3": 3.0
                    }
                }
            ]
        }

        with tempfile.TemporaryDirectory() as temporary_folder:
            raw_folder = os.path.join(temporary_folder, "raw")
            clean_folder = os.path.join(temporary_folder, "clean")

            os.makedirs(raw_folder)

            json_path = os.path.join(
                raw_folder,
                "london.json"
            )

            with open(json_path, "w", encoding="utf-8") as json_file:
                json.dump(raw_data, json_file)

            with patch.object(
                transform,
                "RAW_FOLDER",
                raw_folder
            ):
                with patch.object(
                    transform,
                    "CLEAN_FOLDER",
                    clean_folder
                ):
                    output_file = (
                        transform.transform_air_quality_data()
                    )

            self.assertIsNotNone(output_file)

            result = pd.read_csv(output_file)

            self.assertEqual(len(result), 1)
            self.assertEqual(result.iloc[0]["city"], "London")
            self.assertEqual(result.iloc[0]["country"], "UK")
            self.assertEqual(result.iloc[0]["aqi"], 3)

    def test_transform_rebuilds_single_file_across_runs(self):
        """
        Checks that clean/ always contains exactly ONE CSV file, rebuilt
        from raw/ on every run (no accumulation of timestamped files).
        """

        raw_data = {
            "city": "Paris",
            "data": {
                "list": [
                    {
                        "dt": 1767225600,
                        "main": {"aqi": 2},
                        "components": {"pm2_5": 12.0, "pm10": 20.0}
                    }
                ]
            }
        }

        with tempfile.TemporaryDirectory() as temporary_folder:
            raw_folder = os.path.join(temporary_folder, "raw")
            clean_folder = os.path.join(temporary_folder, "clean")

            os.makedirs(raw_folder)

            json_path = os.path.join(raw_folder, "paris_run1.json")
            with open(json_path, "w", encoding="utf-8") as json_file:
                json.dump(raw_data, json_file)

            with patch.object(transform, "RAW_FOLDER", raw_folder):
                with patch.object(transform, "CLEAN_FOLDER", clean_folder):
                    first_run = transform.transform_air_quality_data()

                    # Un deuxième fichier apparaît dans raw/ (nouvelle heure)
                    json_path_2 = os.path.join(raw_folder, "paris_run2.json")
                    with open(json_path_2, "w", encoding="utf-8") as json_file:
                        json.dump(raw_data, json_file)

                    second_run = transform.transform_air_quality_data()

            self.assertEqual(first_run, second_run)

            clean_files = [
                f for f in os.listdir(clean_folder) if f.endswith(".csv")
            ]
            self.assertEqual(clean_files, ["air_quality.csv"])

    def test_transform_ignores_invalid_json_file(self):
        """Checks that an invalid JSON file is ignored."""

        with tempfile.TemporaryDirectory() as temporary_folder:
            raw_folder = os.path.join(temporary_folder, "raw")
            clean_folder = os.path.join(temporary_folder, "clean")

            os.makedirs(raw_folder)

            invalid_json_path = os.path.join(
                raw_folder,
                "invalid.json"
            )

            Path(invalid_json_path).write_text(
                "{ invalid json content",
                encoding="utf-8"
            )

            with patch.object(
                transform,
                "RAW_FOLDER",
                raw_folder
            ):
                with patch.object(
                    transform,
                    "CLEAN_FOLDER",
                    clean_folder
                ):
                    result = transform.transform_air_quality_data()

            self.assertIsNone(result)
            self.assertFalse(os.path.exists(clean_folder))


if __name__ == "__main__":
    unittest.main()