"""
Tests for glue_jobs/dynamodb_loader.py

Covers:
  - toDecimal             – None input, integer, float, string
  - buildGenreKpisItem    – all fields, type coercions
  - buildTopSongsItem     – all fields, type coercions
  - buildTopGenresItem    – all fields, type coercions
  - writePartitionToDynamo – boto3 batch_writer interaction
  - loadToDynamo          – foreachPartition called, item builder invoked
  - Constants             – table name constants
"""

import sys
import pytest
from decimal import Decimal
from unittest.mock import MagicMock, patch, call

with patch("pyspark.context.SparkContext"):
    sys.argv = [
        "dynamodb_loader",
        "--JOB_NAME", "test",
        "--curated_bucket", "bucket",
        "--aws_region", "us-east-1",
    ]
    from glue_jobs.dynamodb_loader import (
        GENRE_KPIS_TABLE,
        TOP_SONGS_TABLE,
        TOP_GENRES_TABLE,
        toDecimal,
        buildGenreKpisItem,
        buildTopSongsItem,
        buildTopGenresItem,
        writePartitionToDynamo,
        loadToDynamo,
    )


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

class TestConstants:
    def test_genre_kpis_table_name(self):
        assert GENRE_KPIS_TABLE == "genre_kpis"

    def test_top_songs_table_name(self):
        assert TOP_SONGS_TABLE == "top_songs"

    def test_top_genres_table_name(self):
        assert TOP_GENRES_TABLE == "top_genres"


# ---------------------------------------------------------------------------
# toDecimal
# ---------------------------------------------------------------------------

class TestToDecimal:
    def test_none_returns_none(self):
        assert toDecimal(None) is None

    def test_integer_converts_to_decimal(self):
        result = toDecimal(100)
        assert isinstance(result, Decimal)
        assert result == Decimal("100")

    def test_float_converts_to_decimal(self):
        result = toDecimal(3.14)
        assert isinstance(result, Decimal)

    def test_string_number_converts_to_decimal(self):
        result = toDecimal("200000")
        assert result == Decimal("200000")

    def test_zero_converts_to_decimal(self):
        result = toDecimal(0)
        assert result == Decimal("0")

    def test_large_number_converts(self):
        result = toDecimal(9_999_999_999)
        assert result == Decimal("9999999999")

    def test_negative_number_converts(self):
        result = toDecimal(-50)
        assert result == Decimal("-50")


# ---------------------------------------------------------------------------
# buildGenreKpisItem
# ---------------------------------------------------------------------------

class TestBuildGenreKpisItem:
    def _make_row(self, **overrides):
        base = {
            "genre_date":                  "Pop#2024-01-01",
            "stream_date":                 "2024-01-01",
            "track_genre":                 "Pop",
            "listen_count":                100,
            "unique_listeners":            50,
            "total_listen_time_ms":        200000,
            "avg_listen_time_ms_per_user": 4000.0,
        }
        base.update(overrides)
        return base

    def test_genre_date_preserved(self):
        item = buildGenreKpisItem(self._make_row())
        assert item["genre_date"] == "Pop#2024-01-01"

    def test_stream_date_is_string(self):
        item = buildGenreKpisItem(self._make_row())
        assert isinstance(item["stream_date"], str)

    def test_listen_count_is_int(self):
        item = buildGenreKpisItem(self._make_row(listen_count=100))
        assert isinstance(item["listen_count"], int)
        assert item["listen_count"] == 100

    def test_unique_listeners_is_int(self):
        item = buildGenreKpisItem(self._make_row(unique_listeners=50))
        assert isinstance(item["unique_listeners"], int)

    def test_total_listen_time_is_decimal(self):
        item = buildGenreKpisItem(self._make_row(total_listen_time_ms=200000))
        assert isinstance(item["total_listen_time_ms"], Decimal)

    def test_avg_listen_time_is_decimal(self):
        item = buildGenreKpisItem(self._make_row(avg_listen_time_ms_per_user=4000.0))
        assert isinstance(item["avg_listen_time_ms_per_user"], Decimal)

    def test_null_total_listen_time_becomes_none(self):
        item = buildGenreKpisItem(self._make_row(total_listen_time_ms=None))
        assert item["total_listen_time_ms"] is None

    def test_null_avg_listen_time_becomes_none(self):
        item = buildGenreKpisItem(self._make_row(avg_listen_time_ms_per_user=None))
        assert item["avg_listen_time_ms_per_user"] is None

    def test_all_expected_keys_present(self):
        item = buildGenreKpisItem(self._make_row())
        expected = {
            "genre_date", "stream_date", "track_genre",
            "listen_count", "unique_listeners",
            "total_listen_time_ms", "avg_listen_time_ms_per_user",
        }
        assert expected == set(item.keys())


# ---------------------------------------------------------------------------
# buildTopSongsItem
# ---------------------------------------------------------------------------

class TestBuildTopSongsItem:
    def _make_row(self, **overrides):
        base = {
            "genre_date":  "Pop#2024-01-01",
            "rank":        1,
            "stream_date": "2024-01-01",
            "track_genre": "Pop",
            "track_id":    "t1",
            "track_name":  "Song A",
            "play_count":  42,
        }
        base.update(overrides)
        return base

    def test_genre_date_preserved(self):
        item = buildTopSongsItem(self._make_row())
        assert item["genre_date"] == "Pop#2024-01-01"

    def test_rank_is_int(self):
        item = buildTopSongsItem(self._make_row(rank=1))
        assert isinstance(item["rank"], int)

    def test_stream_date_is_string(self):
        item = buildTopSongsItem(self._make_row())
        assert isinstance(item["stream_date"], str)

    def test_play_count_is_int(self):
        item = buildTopSongsItem(self._make_row(play_count=42))
        assert isinstance(item["play_count"], int)
        assert item["play_count"] == 42

    def test_track_id_preserved(self):
        item = buildTopSongsItem(self._make_row(track_id="t99"))
        assert item["track_id"] == "t99"

    def test_track_name_preserved(self):
        item = buildTopSongsItem(self._make_row(track_name="My Song"))
        assert item["track_name"] == "My Song"

    def test_all_expected_keys_present(self):
        item = buildTopSongsItem(self._make_row())
        expected = {"genre_date", "rank", "stream_date", "track_genre", "track_id", "track_name", "play_count"}
        assert expected == set(item.keys())


# ---------------------------------------------------------------------------
# buildTopGenresItem
# ---------------------------------------------------------------------------

class TestBuildTopGenresItem:
    def _make_row(self, **overrides):
        base = {
            "date":         "2024-01-01",
            "rank":         1,
            "track_genre":  "Pop",
            "listen_count": 200,
        }
        base.update(overrides)
        return base

    def test_date_is_string(self):
        item = buildTopGenresItem(self._make_row())
        assert isinstance(item["date"], str)

    def test_rank_is_int(self):
        item = buildTopGenresItem(self._make_row(rank=2))
        assert isinstance(item["rank"], int)
        assert item["rank"] == 2

    def test_listen_count_is_int(self):
        item = buildTopGenresItem(self._make_row(listen_count=200))
        assert isinstance(item["listen_count"], int)
        assert item["listen_count"] == 200

    def test_track_genre_preserved(self):
        item = buildTopGenresItem(self._make_row(track_genre="Jazz"))
        assert item["track_genre"] == "Jazz"

    def test_all_expected_keys_present(self):
        item = buildTopGenresItem(self._make_row())
        expected = {"date", "rank", "track_genre", "listen_count"}
        assert expected == set(item.keys())


# ---------------------------------------------------------------------------
# writePartitionToDynamo
# ---------------------------------------------------------------------------

class TestWritePartitionToDynamo:
    def test_calls_put_item_for_each_row(self):
        rows = [
            {"genre_date": "Pop#2024-01-01", "listen_count": 10},
            {"genre_date": "Rock#2024-01-01", "listen_count": 5},
        ]
        mock_table = MagicMock()
        mock_batch = MagicMock()
        mock_table.batch_writer.return_value.__enter__ = MagicMock(return_value=mock_batch)
        mock_table.batch_writer.return_value.__exit__ = MagicMock(return_value=False)

        mock_dynamodb = MagicMock()
        mock_dynamodb.Table.return_value = mock_table

        with patch("boto3.resource", return_value=mock_dynamodb):
            writePartitionToDynamo(rows, "genre_kpis", "us-east-1")

        assert mock_batch.put_item.call_count == 2

    def test_creates_dynamodb_resource_with_correct_region(self):
        with patch("boto3.resource") as mock_resource:
            mock_table = MagicMock()
            mock_resource.return_value.Table.return_value = mock_table
            mock_table.batch_writer.return_value.__enter__ = MagicMock(return_value=MagicMock())
            mock_table.batch_writer.return_value.__exit__ = MagicMock(return_value=False)

            writePartitionToDynamo([], "genre_kpis", "eu-west-1")

        mock_resource.assert_called_once_with("dynamodb", region_name="eu-west-1")

    def test_uses_correct_table_name(self):
        with patch("boto3.resource") as mock_resource:
            mock_table = MagicMock()
            mock_resource.return_value.Table.return_value = mock_table
            mock_table.batch_writer.return_value.__enter__ = MagicMock(return_value=MagicMock())
            mock_table.batch_writer.return_value.__exit__ = MagicMock(return_value=False)

            writePartitionToDynamo([], "top_songs", "us-east-1")

        mock_resource.return_value.Table.assert_called_once_with("top_songs")

    def test_empty_rows_does_not_call_put_item(self):
        mock_table = MagicMock()
        mock_batch = MagicMock()
        mock_table.batch_writer.return_value.__enter__ = MagicMock(return_value=mock_batch)
        mock_table.batch_writer.return_value.__exit__ = MagicMock(return_value=False)

        with patch("boto3.resource") as mock_resource:
            mock_resource.return_value.Table.return_value = mock_table
            writePartitionToDynamo([], "genre_kpis", "us-east-1")

        mock_batch.put_item.assert_not_called()


# ---------------------------------------------------------------------------
# loadToDynamo
# ---------------------------------------------------------------------------

class TestLoadToDynamo:
    def test_calls_foreach_partition(self, spark):
        df = spark.createDataFrame(
            [("Pop#2024-01-01", "2024-01-01", "Pop", 10, 5, 200000, 40000.0)],
            ["genre_date", "stream_date", "track_genre", "listen_count",
             "unique_listeners", "total_listen_time_ms", "avg_listen_time_ms_per_user"],
        )
        with patch.object(df, "foreachPartition") as mock_fp:
            loadToDynamo(df, "genre_kpis", "us-east-1", buildGenreKpisItem)
            mock_fp.assert_called_once()

    def test_item_builder_is_called_per_row(self, spark):
        rows = [
            ("Pop#2024-01-01", "2024-01-01", "Pop", 10, 5, 200000, 40000.0),
            ("Rock#2024-01-01", "2024-01-01", "Rock", 8, 4, 180000, 45000.0),
        ]
        df = spark.createDataFrame(
            rows,
            ["genre_date", "stream_date", "track_genre", "listen_count",
             "unique_listeners", "total_listen_time_ms", "avg_listen_time_ms_per_user"],
        )
        captured_items = []

        def capture_builder(row):
            item = buildGenreKpisItem(row)
            captured_items.append(item)
            return item

        with patch("glue_jobs.dynamodb_loader.writePartitionToDynamo") as mock_write:
            loadToDynamo(df, "genre_kpis", "us-east-1", capture_builder)
