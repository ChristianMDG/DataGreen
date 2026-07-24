import os
import sys
import tempfile
import unittest
from datetime import date
from unittest.mock import Mock, patch

import pandas as pd

sys.path.insert(
    0,
    os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
)

from scripts import load_warehouse


class FakeResult:
    """Simulates a result returned by SQLAlchemy."""

    def __init__(self, rows=None, rowcount=1):
        self.rows = rows or []
        self.rowcount = rowcount

    def __iter__(self):
        return iter(self.rows)

    def fetchone(self):
        return self.rows[0]


class FakeTransaction:
    """Simulates a database transaction."""

    def __init__(self):
        self.committed = False
        self.rolled_back = False

    def commit(self):
        self.committed = True

    def rollback(self):
        self.rolled_back = True


class FakeConnection:
    """Simulates a SQLAlchemy database connection."""

    def __init__(
        self,
        city_rows=None,
        time_rows=None,
        total=1,
        raise_on_fact_insert=False
    ):
        self.city_rows = (
            city_rows
            if city_rows is not None
            else [(1, "Paris")]
        )

        self.time_rows = (
            time_rows
            if time_rows is not None
            else [(10, date(2026, 1, 1), 0)]
        )

        self.total = total
        self.raise_on_fact_insert = raise_on_fact_insert
        self.transaction = FakeTransaction()
        self.executed_queries = []

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception, traceback):
        return False

    def begin(self):
        return self.transaction

    def execute(self, query, parameters=None):
        query_text = str(query)
        self.executed_queries.append(query_text)

        if "SELECT city_id, city_name FROM dim_city" in query_text:
            return FakeResult(self.city_rows)

        if "SELECT time_id, date, hour FROM dim_time" in query_text:
            return FakeResult(self.time_rows)

        if "INSERT INTO fact_air_quality" in query_text:
            if self.raise_on_fact_insert:
                raise RuntimeError("Simulated insertion error")

            return FakeResult(rowcount=1)

        if "SELECT COUNT(*) FROM fact_air_quality" in query_text:
            return FakeResult([(self.total,)])

        if "SELECT c.city_name, COUNT(*)" in query_text:
            return FakeResult([("Paris", self.total)])

        return FakeResult(rowcount=1)


class FakeEngine:
    """Simulates a SQLAlchemy engine."""

    def __init__(self, connection):
        self.connection = connection

    def connect(self):
        return self.connection


class TestLoadWarehouse(unittest.TestCase):

    @staticmethod
    def create_valid_dataframe():
        """Creates a valid DataFrame for warehouse loading tests."""

        return pd.DataFrame([
            {
                "city": "Paris",
                "country": "FR",
                "latitude": 48.8566,
                "longitude": 2.3522,
                "timestamp": "2026-01-01T00:00:00",
                "date": "2026-01-01",
                "hour": 0,
                "day_of_week": "Thursday",
                "month": 1,
                "year": 2026,
                "is_weekend": False,
                "aqi": 2,
                "co": 100.0,
                "no": 1.0,
                "no2": 15.0,
                "o3": 40.0,
                "pm2_5": 12.0,
                "pm10": 20.0,
                "so2": 5.0,
                "nh3": 2.0
            }
        ])

    def test_get_engine_uses_expected_database_url(self):
        """Checks that get_engine() builds the expected database URL."""

        fake_engine = Mock()

        with patch.object(
            load_warehouse,
            "create_engine",
            return_value=fake_engine
        ) as mock_create_engine:

            result = load_warehouse.get_engine()

        expected_url = (
            "postgresql://warehouse:warehouse"
            "@postgres_warehouse:5432/air_quality_db"
        )

        mock_create_engine.assert_called_once_with(expected_url)
        self.assertIs(result, fake_engine)

    def test_load_returns_none_when_no_csv_exists(self):
        """Checks that loading stops when no clean CSV file exists."""

        with tempfile.TemporaryDirectory() as temporary_folder:
            with patch.object(
                load_warehouse,
                "get_engine"
            ) as mock_get_engine:

                result = load_warehouse.load_to_warehouse(
                    temporary_folder
                )

        self.assertIsNone(result)
        mock_get_engine.assert_not_called()

    def test_load_uses_latest_csv_file(self):
        """Checks that the latest CSV filename is selected."""

        with tempfile.TemporaryDirectory() as temporary_folder:
            dataframe = self.create_valid_dataframe()

            old_file = os.path.join(
                temporary_folder,
                "air_quality_20260101.csv"
            )

            latest_file = os.path.join(
                temporary_folder,
                "air_quality_20260102.csv"
            )

            dataframe.to_csv(old_file, index=False)
            dataframe.to_csv(latest_file, index=False)

            connection = FakeConnection()
            engine = FakeEngine(connection)

            real_read_csv = pd.read_csv

            with patch.object(
                load_warehouse,
                "get_engine",
                return_value=engine
            ):
                with patch.object(
                    load_warehouse.pd,
                    "read_csv",
                    wraps=real_read_csv
                ) as mock_read_csv:

                    load_warehouse.load_to_warehouse(
                        temporary_folder
                    )

        mock_read_csv.assert_called_once_with(latest_file)

    def test_load_inserts_one_record_successfully(self):
        """Checks that one valid record is loaded successfully."""

        with tempfile.TemporaryDirectory() as temporary_folder:
            dataframe = self.create_valid_dataframe()

            csv_file = os.path.join(
                temporary_folder,
                "air_quality_20260101.csv"
            )

            dataframe.to_csv(csv_file, index=False)

            connection = FakeConnection(total=1)
            engine = FakeEngine(connection)

            with patch.object(
                load_warehouse,
                "get_engine",
                return_value=engine
            ):
                result = load_warehouse.load_to_warehouse(
                    temporary_folder
                )

        self.assertEqual(
            result,
            {
                "status": "success",
                "inserted": 1,
                "total": 1
            }
        )

        self.assertTrue(connection.transaction.committed)
        self.assertFalse(connection.transaction.rolled_back)

        executed_sql = "\n".join(
            connection.executed_queries
        )

        self.assertIn(
            "INSERT INTO dim_city",
            executed_sql
        )

        self.assertIn(
            "INSERT INTO dim_time",
            executed_sql
        )

        self.assertIn(
            "INSERT INTO fact_air_quality",
            executed_sql
        )

    def test_load_skips_record_when_city_mapping_is_missing(self):
        """
        Checks that a record is skipped when its city
        is absent from the city mapping.
        """

        with tempfile.TemporaryDirectory() as temporary_folder:
            dataframe = self.create_valid_dataframe()

            csv_file = os.path.join(
                temporary_folder,
                "air_quality_20260101.csv"
            )

            dataframe.to_csv(csv_file, index=False)

            connection = FakeConnection(
                city_rows=[],
                total=0
            )

            engine = FakeEngine(connection)

            with patch.object(
                load_warehouse,
                "get_engine",
                return_value=engine
            ):
                result = load_warehouse.load_to_warehouse(
                    temporary_folder
                )

        self.assertEqual(result["status"], "success")
        self.assertEqual(result["inserted"], 0)
        self.assertEqual(result["total"], 0)
        self.assertFalse(connection.transaction.committed)

    def test_load_skips_record_when_time_mapping_is_missing(self):
        """
        Checks that a record is skipped when its date and hour
        are absent from the time mapping.
        """

        with tempfile.TemporaryDirectory() as temporary_folder:
            dataframe = self.create_valid_dataframe()

            csv_file = os.path.join(
                temporary_folder,
                "air_quality_20260101.csv"
            )

            dataframe.to_csv(csv_file, index=False)

            connection = FakeConnection(
                time_rows=[],
                total=0
            )

            engine = FakeEngine(connection)

            with patch.object(
                load_warehouse,
                "get_engine",
                return_value=engine
            ):
                result = load_warehouse.load_to_warehouse(
                    temporary_folder
                )

        self.assertEqual(result["status"], "success")
        self.assertEqual(result["inserted"], 0)
        self.assertEqual(result["total"], 0)
        self.assertFalse(connection.transaction.committed)

    def test_transaction_is_rolled_back_after_insertion_error(self):
        """
        Checks that the transaction is rolled back
        when fact insertion fails.
        """

        with tempfile.TemporaryDirectory() as temporary_folder:
            dataframe = self.create_valid_dataframe()

            csv_file = os.path.join(
                temporary_folder,
                "air_quality_20260101.csv"
            )

            dataframe.to_csv(csv_file, index=False)

            connection = FakeConnection(
                raise_on_fact_insert=True
            )

            engine = FakeEngine(connection)

            with patch.object(
                load_warehouse,
                "get_engine",
                return_value=engine
            ):
                with self.assertRaises(RuntimeError):
                    load_warehouse.load_to_warehouse(
                        temporary_folder
                    )

        self.assertFalse(connection.transaction.committed)
        self.assertTrue(connection.transaction.rolled_back)


if __name__ == "__main__":
    unittest.main()