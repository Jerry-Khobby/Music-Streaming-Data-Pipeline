"""
Tests for glue_jobs/validation_job.py

Covers:
  - checkMissingColumns  – all columns present, subset missing, all missing
  - checkNonEmpty        – non-empty DataFrame, empty DataFrame
  - validateTable        – orchestration: valid table, missing cols, empty data
  - REQUIRED_COLUMNS     – constant integrity
"""

import sys
import pytest
from unittest.mock import MagicMock, patch

# Patch SparkContext so the module-level bootstrap does not spin up a cluster.
with patch("pyspark.context.SparkContext"):
    sys.argv = ["validation_job", "--JOB_NAME", "test", "--glue_database", "db"]
    import importlib
    import glue_jobs.validation_job as validation_job


from glue_jobs.validation_job import (
    REQUIRED_COLUMNS,
    checkMissingColumns,
    checkNonEmpty,
    validateTable,
)


# ---------------------------------------------------------------------------
# REQUIRED_COLUMNS constant
# ---------------------------------------------------------------------------

class TestRequiredColumnsConstant:
    def test_streams_has_required_keys(self):
        assert "user_id"    in REQUIRED_COLUMNS["streams"]
        assert "track_id"   in REQUIRED_COLUMNS["streams"]
        assert "listen_time" in REQUIRED_COLUMNS["streams"]

    def test_songs_has_required_keys(self):
        assert "track_id"    in REQUIRED_COLUMNS["songs"]
        assert "track_name"  in REQUIRED_COLUMNS["songs"]
        assert "track_genre" in REQUIRED_COLUMNS["songs"]
        assert "duration_ms" in REQUIRED_COLUMNS["songs"]

    def test_users_has_required_keys(self):
        assert "user_id"      in REQUIRED_COLUMNS["users"]
        assert "user_name"    in REQUIRED_COLUMNS["users"]
        assert "user_country" in REQUIRED_COLUMNS["users"]

    def test_exactly_three_tables_defined(self):
        assert set(REQUIRED_COLUMNS.keys()) == {"streams", "songs", "users"}


# ---------------------------------------------------------------------------
# checkMissingColumns
# ---------------------------------------------------------------------------

class TestCheckMissingColumns:
    def test_no_error_when_all_columns_present(self, spark):
        df = spark.createDataFrame(
            [("u1", "t1", "2024-01-01")],
            ["user_id", "track_id", "listen_time"],
        )
        checkMissingColumns(df, "streams")  # must not raise

    def test_raises_when_one_column_missing(self, spark):
        df = spark.createDataFrame(
            [("u1", "t1")],
            ["user_id", "track_id"],  # listen_time absent
        )
        with pytest.raises(ValueError, match="listen_time"):
            checkMissingColumns(df, "streams")

    def test_raises_when_all_columns_missing(self, spark):
        df = spark.createDataFrame([(1,)], ["irrelevant"])
        with pytest.raises(ValueError):
            checkMissingColumns(df, "streams")

    def test_error_message_contains_table_name(self, spark):
        df = spark.createDataFrame([(1,)], ["irrelevant"])
        with pytest.raises(ValueError, match="streams"):
            checkMissingColumns(df, "streams")

    def test_extra_columns_are_allowed(self, spark):
        df = spark.createDataFrame(
            [("u1", "t1", "2024-01-01", "extra")],
            ["user_id", "track_id", "listen_time", "extra_col"],
        )
        checkMissingColumns(df, "streams")  # must not raise

    def test_songs_all_columns_present(self, spark):
        df = spark.createDataFrame(
            [("t1", "Song A", "Pop", 200000)],
            ["track_id", "track_name", "track_genre", "duration_ms"],
        )
        checkMissingColumns(df, "songs")

    def test_songs_missing_duration(self, spark):
        df = spark.createDataFrame(
            [("t1", "Song A", "Pop")],
            ["track_id", "track_name", "track_genre"],
        )
        with pytest.raises(ValueError, match="duration_ms"):
            checkMissingColumns(df, "songs")

    def test_users_all_columns_present(self, spark):
        df = spark.createDataFrame(
            [("u1", "Alice", "Ghana")],
            ["user_id", "user_name", "user_country"],
        )
        checkMissingColumns(df, "users")


# ---------------------------------------------------------------------------
# checkNonEmpty
# ---------------------------------------------------------------------------

class TestCheckNonEmpty:
    def test_no_error_for_non_empty_dataframe(self, spark):
        df = spark.createDataFrame([("u1",)], ["user_id"])
        checkNonEmpty(df, "streams")  # must not raise

    def test_raises_for_empty_dataframe(self, spark):
        df = spark.createDataFrame([], spark.createDataFrame([("x",)], ["c"]).schema)
        with pytest.raises(ValueError, match="empty"):
            checkNonEmpty(df, "streams")

    def test_error_message_contains_table_name(self, spark):
        df = spark.createDataFrame([], spark.createDataFrame([("x",)], ["c"]).schema)
        with pytest.raises(ValueError, match="users"):
            checkNonEmpty(df, "users")

    def test_multiple_rows_pass(self, spark):
        data = [(str(i),) for i in range(100)]
        df = spark.createDataFrame(data, ["user_id"])
        checkNonEmpty(df, "streams")  # must not raise


# ---------------------------------------------------------------------------
# validateTable (orchestration)
# ---------------------------------------------------------------------------

class TestValidateTable:
    def test_valid_table_does_not_raise(self, spark):
        df = spark.createDataFrame(
            [("u1", "t1", "2024-01-01")],
            ["user_id", "track_id", "listen_time"],
        )
        mock_glue = MagicMock()
        mock_glue.create_dynamic_frame.from_catalog.return_value.toDF.return_value = df
        validateTable(mock_glue, "test_db", "streams")

    def test_raises_on_empty_table(self, spark):
        empty_df = spark.createDataFrame(
            [],
            spark.createDataFrame([("u1", "t1", "2024-01-01")], ["user_id", "track_id", "listen_time"]).schema,
        )
        mock_glue = MagicMock()
        mock_glue.create_dynamic_frame.from_catalog.return_value.toDF.return_value = empty_df
        with pytest.raises(ValueError):
            validateTable(mock_glue, "test_db", "streams")

    def test_raises_on_missing_columns(self, spark):
        df = spark.createDataFrame([("u1",)], ["user_id"])
        mock_glue = MagicMock()
        mock_glue.create_dynamic_frame.from_catalog.return_value.toDF.return_value = df
        with pytest.raises(ValueError):
            validateTable(mock_glue, "test_db", "streams")
