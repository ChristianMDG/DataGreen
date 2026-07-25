import importlib
import json
import os
import sys
import tempfile
import unittest
from datetime import datetime, timedelta
from unittest.mock import Mock, patch

sys.path.insert(
    0,
    os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
)

with patch("airflow.models.Variable.get", return_value=None):
    extract = importlib.import_module("scripts.extract")


class TestExtract(unittest.TestCase):

    def test_cities_contains_five_expected_cities(self):
        """Checks that the five expected cities are configured."""

        expected_cities = [
            "Paris",
            "London",
            "Berlin",
            "Madrid",
            "Rome"
        ]

        self.assertEqual(len(extract.CITIES), 5)
        self.assertEqual(
            sorted(extract.CITIES.keys()),
            sorted(expected_cities)
        )

    def test_split_date_range(self):
        """Checks that a date range is split into 30-day intervals."""

        start_date = datetime(2026, 1, 1)
        end_date = datetime(2026, 3, 2)

        result = extract.split_date_range(
            start_date,
            end_date,
            days=30
        )

        self.assertEqual(len(result), 2)

        self.assertEqual(
            result[0],
            (
                start_date,
                start_date + timedelta(days=30)
            )
        )

        self.assertEqual(
            result[1],
            (
                start_date + timedelta(days=30),
                end_date
            )
        )

    def test_split_date_range_for_short_period(self):
        """Checks a period shorter than 30 days."""

        start_date = datetime(2026, 1, 1)
        end_date = datetime(2026, 1, 10)

        result = extract.split_date_range(
            start_date,
            end_date,
            days=30
        )

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0], (start_date, end_date))

    def test_fetch_historical_data_without_api_key(self):
        """
        Without an API key, the function should return simulated data
        without calling OpenWeather.
        """

        with patch.object(extract, "API_KEY", None):
            with patch.object(
                extract.requests,
                "get"
            ) as mock_get:

                result = extract.fetch_historical_data(
                    city="Paris",
                    lat=48.8566,
                    lon=2.3522,
                    start_date=datetime(2026, 1, 1),
                    end_date=datetime(2026, 1, 2)
                )

        self.assertEqual(result["city"], "Paris")
        self.assertEqual(result["data"], {"list": []})
        self.assertIn("timestamp", result)

        mock_get.assert_not_called()

    def test_fetch_historical_data_success(self):
        """Checks a successful extraction using a mocked API response."""

        fake_api_data = {
            "list": [
                {
                    "dt": 1767225600,
                    "main": {
                        "aqi": 2
                    },
                    "components": {
                        "co": 100.0,
                        "no2": 15.0,
                        "pm2_5": 12.0,
                        "pm10": 20.0
                    }
                }
            ]
        }

        fake_response = Mock()
        fake_response.raise_for_status.return_value = None
        fake_response.json.return_value = fake_api_data

        with patch.object(extract, "API_KEY", "fake-api-key"):
            with patch.object(
                extract.requests,
                "get",
                return_value=fake_response
            ) as mock_get:

                result = extract.fetch_historical_data(
                    city="Paris",
                    lat=48.8566,
                    lon=2.3522,
                    start_date=datetime(2026, 1, 1),
                    end_date=datetime(2026, 1, 2)
                )

        self.assertEqual(result["city"], "Paris")
        self.assertEqual(result["data"], fake_api_data)
        self.assertIn("timestamp", result)

        mock_get.assert_called_once()

        called_url = mock_get.call_args.args[0]
        called_parameters = mock_get.call_args.kwargs["params"]
        called_timeout = mock_get.call_args.kwargs["timeout"]

        self.assertEqual(
            called_url,
            "http://api.openweathermap.org/data/2.5/"
            "air_pollution/history"
        )

        self.assertEqual(called_parameters["lat"], 48.8566)
        self.assertEqual(called_parameters["lon"], 2.3522)
        self.assertEqual(
            called_parameters["appid"],
            "fake-api-key"
        )
        self.assertEqual(called_timeout, 60)

    def test_fetch_historical_data_retries_after_error(self):
        """
        Checks that the function retries after a network error
        and returns None after the final failure.
        """

        network_error = extract.requests.exceptions.Timeout(
            "Connection timed out"
        )

        with patch.object(extract, "API_KEY", "fake-api-key"):
            with patch.object(
                extract.requests,
                "get",
                side_effect=network_error
            ) as mock_get:
                with patch.object(
                    extract.time,
                    "sleep"
                ) as mock_sleep:

                    result = extract.fetch_historical_data(
                        city="Paris",
                        lat=48.8566,
                        lon=2.3522,
                        start_date=datetime(2026, 1, 1),
                        end_date=datetime(2026, 1, 2),
                        retries=3
                    )

        self.assertIsNone(result)

        self.assertEqual(mock_get.call_count, 3)

        self.assertEqual(mock_sleep.call_count, 2)

        mock_sleep.assert_any_call(20)
        mock_sleep.assert_any_call(40)

    def test_save_backfill_data_creates_json_file(self):
        """Checks that a JSON file is created."""

        data = {
            "city": "Paris",
            "data": {
                "list": []
            },
            "timestamp": "2026-01-01T10:00:00"
        }

        with tempfile.TemporaryDirectory() as temporary_folder:
            with patch.object(
                extract,
                "RAW_FOLDER",
                temporary_folder
            ):
                filename = extract.save_backfill_data(
                    "Paris",
                    data
                )

            self.assertIsNotNone(filename)
            self.assertTrue(os.path.exists(filename))
            self.assertTrue(filename.endswith(".json"))
            self.assertIn("backfill_Paris_", filename)

            with open(
                filename,
                "r",
                encoding="utf-8"
            ) as json_file:
                saved_data = json.load(json_file)

            self.assertEqual(saved_data, data)

    def test_save_backfill_data_with_none(self):
        """No file should be created when no data is provided."""

        with tempfile.TemporaryDirectory() as temporary_folder:
            with patch.object(
                extract,
                "RAW_FOLDER",
                temporary_folder
            ):
                result = extract.save_backfill_data(
                    "Paris",
                    None
                )

            self.assertIsNone(result)
            self.assertEqual(
                os.listdir(temporary_folder),
                []
            )

    def test_main_stops_when_api_key_is_missing(self):
        """
        main() should stop immediately when the API key is missing
        and should not create any threads.
        """

        with patch.object(extract, "API_KEY", None):
            with patch.object(
                extract,
                "ThreadPoolExecutor"
            ) as mock_executor:

                result = extract.main()

        self.assertIsNone(result)
        mock_executor.assert_not_called()


if __name__ == "__main__":
    unittest.main()