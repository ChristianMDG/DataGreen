import os
import sys
import tempfile
import unittest

import pandas as pd

sys.path.insert(
    0,
    os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
)

from scripts import validate_clean


class TestValidateClean(unittest.TestCase):

    @staticmethod
    def create_valid_dataframe():
        """Creates a valid DataFrame used by several tests."""

        return pd.DataFrame([
            {
                "city": "Paris",
                "timestamp": "2026-01-01T10:00:00",
                "date": "2026-01-01",
                "aqi": 2,
                "pm2_5": 12.5,
                "pm10": 20.5
            },
            {
                "city": "London",
                "timestamp": "2026-01-02T11:00:00",
                "date": "2026-01-02",
                "aqi": 3,
                "pm2_5": 18.0,
                "pm10": 25.0
            },
            {
                "city": "Berlin",
                "timestamp": "2026-01-03T12:00:00",
                "date": "2026-01-03",
                "aqi": 1,
                "pm2_5": 8.0,
                "pm10": 14.0
            },
            {
                "city": "Madrid",
                "timestamp": "2026-01-04T13:00:00",
                "date": "2026-01-04",
                "aqi": 4,
                "pm2_5": 28.0,
                "pm10": 38.0
            },
            {
                "city": "Rome",
                "timestamp": "2026-01-05T14:00:00",
                "date": "2026-01-05",
                "aqi": 5,
                "pm2_5": 35.0,
                "pm10": 48.0
            }
        ])

    def test_returns_false_when_no_csv_file_exists(self):
        """Checks that validation fails when no CSV file exists."""

        with tempfile.TemporaryDirectory() as temporary_folder:
            result = validate_clean.validate_clean_file(
                temporary_folder
            )

        self.assertFalse(result)

    def test_returns_true_for_valid_csv_file(self):
        """Checks that a valid CSV file passes validation."""

        with tempfile.TemporaryDirectory() as temporary_folder:
            dataframe = self.create_valid_dataframe()

            csv_path = os.path.join(
                temporary_folder,
                "air_quality_20260105.csv"
            )

            dataframe.to_csv(csv_path, index=False)

            result = validate_clean.validate_clean_file(
                temporary_folder
            )

        self.assertTrue(result)

    def test_returns_false_when_required_column_is_missing(self):
        """Checks that validation fails when a required column is missing."""

        with tempfile.TemporaryDirectory() as temporary_folder:
            dataframe = self.create_valid_dataframe()

            dataframe = dataframe.drop(columns=["pm10"])

            csv_path = os.path.join(
                temporary_folder,
                "air_quality_20260105.csv"
            )

            dataframe.to_csv(csv_path, index=False)

            result = validate_clean.validate_clean_file(
                temporary_folder
            )

        self.assertFalse(result)

    def test_returns_false_when_city_column_is_missing(self):
        """Checks that validation fails when the city column is missing."""

        with tempfile.TemporaryDirectory() as temporary_folder:
            dataframe = self.create_valid_dataframe()
            dataframe = dataframe.drop(columns=["city"])

            csv_path = os.path.join(
                temporary_folder,
                "air_quality_20260105.csv"
            )

            dataframe.to_csv(csv_path, index=False)

            result = validate_clean.validate_clean_file(
                temporary_folder
            )

        self.assertFalse(result)

    def test_missing_values_only_generate_warning(self):
        """
        Checks that missing values generate a warning
        without making validation fail.
        """

        with tempfile.TemporaryDirectory() as temporary_folder:
            dataframe = self.create_valid_dataframe()

            dataframe.loc[0, "pm2_5"] = None

            csv_path = os.path.join(
                temporary_folder,
                "air_quality_20260105.csv"
            )

            dataframe.to_csv(csv_path, index=False)

            result = validate_clean.validate_clean_file(
                temporary_folder
            )

        self.assertTrue(result)

    def test_duplicates_only_generate_warning(self):
        """
        Checks that duplicate rows generate a warning
        without making validation fail.
        """

        with tempfile.TemporaryDirectory() as temporary_folder:
            dataframe = self.create_valid_dataframe()

            duplicate = dataframe.iloc[[0]]
            dataframe = pd.concat(
                [dataframe, duplicate],
                ignore_index=True
            )

            csv_path = os.path.join(
                temporary_folder,
                "air_quality_20260105.csv"
            )

            dataframe.to_csv(csv_path, index=False)

            result = validate_clean.validate_clean_file(
                temporary_folder
            )

        self.assertTrue(result)

    def test_invalid_aqi_only_generates_warning(self):
        """
        Checks that an AQI outside the 1-5 range generates a warning
        without making validation fail.
        """

        with tempfile.TemporaryDirectory() as temporary_folder:
            dataframe = self.create_valid_dataframe()

            dataframe.loc[0, "aqi"] = 8

            csv_path = os.path.join(
                temporary_folder,
                "air_quality_20260105.csv"
            )

            dataframe.to_csv(csv_path, index=False)

            result = validate_clean.validate_clean_file(
                temporary_folder
            )

        self.assertTrue(result)

    def test_missing_cities_only_generate_warning(self):
        """
        Checks that missing expected cities generate a warning
        without making validation fail.
        """

        with tempfile.TemporaryDirectory() as temporary_folder:
            dataframe = self.create_valid_dataframe()

            dataframe = dataframe[
                dataframe["city"] == "Paris"
            ]

            csv_path = os.path.join(
                temporary_folder,
                "air_quality_20260105.csv"
            )

            dataframe.to_csv(csv_path, index=False)

            result = validate_clean.validate_clean_file(
                temporary_folder
            )

        self.assertTrue(result)

    def test_latest_csv_file_is_validated(self):
        """Checks that the most recent CSV filename is selected."""

        with tempfile.TemporaryDirectory() as temporary_folder:
            invalid_dataframe = self.create_valid_dataframe().drop(
                columns=["pm10"]
            )

            valid_dataframe = self.create_valid_dataframe()

            old_file = os.path.join(
                temporary_folder,
                "air_quality_20260101.csv"
            )

            latest_file = os.path.join(
                temporary_folder,
                "air_quality_20260105.csv"
            )

            invalid_dataframe.to_csv(old_file, index=False)
            valid_dataframe.to_csv(latest_file, index=False)

            result = validate_clean.validate_clean_file(
                temporary_folder
            )

        self.assertTrue(result)

    def test_invalid_date_returns_false(self):
        """Checks that an invalid date value makes validation fail."""

        with tempfile.TemporaryDirectory() as temporary_folder:
            dataframe = self.create_valid_dataframe()

            dataframe.loc[0, "date"] = "invalid-date"

            csv_path = os.path.join(
                temporary_folder,
                "air_quality_20260105.csv"
            )

            dataframe.to_csv(csv_path, index=False)

            result = validate_clean.validate_clean_file(
                temporary_folder
            )

        self.assertFalse(result)


if __name__ == "__main__":
    unittest.main()