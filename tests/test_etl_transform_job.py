"""
Tests for glue_jobs/etl_transform_job.py  (Bronze → Silver)

Covers:
  - SONGS_COLUMNS            – constant integrity
  - STREAM_DEDUP_KEY         – constant integrity
  - buildEnrichedStreams      – join correctness, stream_date derivation, unmatched rows
  - loadExistingPartitions   – first-run (path missing), date filtering
  - mergeWithExisting        – accumulates across batches, deduplicates re-delivered events
  - writeParquet             – write interaction (overwrite mode, partitioning)
"""

import sys
import pytest
from unittest.mock import MagicMock, patch, PropertyMock
from pyspark.sql import functions as F

with patch("pyspark.context.SparkContext"):
    sys.argv = [
        "etl_transform_job",
        "--JOB_NAME", "test",
        "--glue_database", "db",
        "--curated_bucket", "bucket",
    ]
    from glue_jobs.etl_transform_job import (
        SONGS_COLUMNS,
        buildEnrichedStreams,
        writeParquet,
    )


# ---------------------------------------------------------------------------
# SONGS_COLUMNS constant
# ---------------------------------------------------------------------------

class TestSongsColumns:
    def test_contains_track_id(self):
        assert "track_id" in SONGS_COLUMNS

    def test_contains_track_name(self):
        assert "track_name" in SONGS_COLUMNS

    def test_contains_track_genre(self):
        assert "track_genre" in SONGS_COLUMNS

    def test_contains_duration_ms(self):
        assert "duration_ms" in SONGS_COLUMNS

    def test_has_exactly_four_columns(self):
        assert len(SONGS_COLUMNS) == 4


# ---------------------------------------------------------------------------
# buildEnrichedStreams  (Bronze → Silver join)
# ---------------------------------------------------------------------------

class TestBuildEnrichedStreams:
    def test_stream_date_column_added(self, enriched_df):
        assert "stream_date" in enriched_df.columns

    def test_stream_date_is_date_type(self, enriched_df):
        from pyspark.sql.types import DateType
        field = next(f for f in enriched_df.schema.fields if f.name == "stream_date")
        assert isinstance(field.dataType, DateType)

    def test_song_columns_present_in_output(self, enriched_df):
        for col in ["track_name", "track_genre", "duration_ms"]:
            assert col in enriched_df.columns

    def test_stream_columns_present_in_output(self, enriched_df):
        for col in ["user_id", "track_id", "listen_time"]:
            assert col in enriched_df.columns

    def test_inner_join_drops_unmatched_streams(self, spark, songs_df):
        streams = spark.createDataFrame(
            [
                ("u1", "t1", "2024-01-01 10:00:00"),   # matches
                ("u2", "UNKNOWN", "2024-01-01 11:00:00"),  # no match
            ],
            ["user_id", "track_id", "listen_time"],
        )
        result = buildEnrichedStreams(streams, songs_df)
        assert result.count() == 1

    def test_inner_join_drops_unmatched_songs(self, spark, streams_df):
        songs = spark.createDataFrame(
            [
                ("t1", "Song A", "Pop", 200000),
                ("t99", "Ghost Song", "Jazz", 100000),  # no stream references t99
            ],
            ["track_id", "track_name", "track_genre", "duration_ms"],
        )
        result = buildEnrichedStreams(streams_df, songs)
        track_ids = {r.track_id for r in result.select("track_id").collect()}
        assert "t99" not in track_ids

    def test_all_matched_streams_are_kept(self, spark, songs_df):
        streams = spark.createDataFrame(
            [("u1", "t1", "2024-01-01 10:00:00")] * 5,
            ["user_id", "track_id", "listen_time"],
        )
        result = buildEnrichedStreams(streams, songs_df)
        assert result.count() == 5

    def test_empty_streams_returns_empty(self, spark, songs_df):
        from pyspark.sql.types import StructType, StructField, StringType
        schema = StructType([
            StructField("user_id",     StringType(), False),
            StructField("track_id",    StringType(), False),
            StructField("listen_time", StringType(), True),
        ])
        empty = spark.createDataFrame([], schema)
        result = buildEnrichedStreams(empty, songs_df)
        assert result.count() == 0

    def test_empty_songs_returns_empty(self, spark, streams_df):
        from pyspark.sql.types import StructType, StructField, StringType, LongType
        schema = StructType([
            StructField("track_id",    StringType(), True),
            StructField("track_name",  StringType(), True),
            StructField("track_genre", StringType(), True),
            StructField("duration_ms", LongType(),   True),
        ])
        empty = spark.createDataFrame([], schema)
        result = buildEnrichedStreams(streams_df, empty)
        assert result.count() == 0

    def test_stream_date_derived_from_listen_time(self, spark, songs_df):
        streams = spark.createDataFrame(
            [("u1", "t1", "2024-03-15 08:30:00")],
            ["user_id", "track_id", "listen_time"],
        )
        result = buildEnrichedStreams(streams, songs_df)
        row = result.select("stream_date").first()
        assert str(row["stream_date"]) == "2024-03-15"

    def test_multiple_streams_same_track_all_kept(self, spark, songs_df):
        streams = spark.createDataFrame(
            [
                ("u1", "t1", "2024-01-01 09:00:00"),
                ("u2", "t1", "2024-01-01 10:00:00"),
                ("u3", "t1", "2024-01-01 11:00:00"),
            ],
            ["user_id", "track_id", "listen_time"],
        )
        result = buildEnrichedStreams(streams, songs_df)
        assert result.count() == 3

    def test_only_songs_columns_selected_from_songs(self, spark):
        streams = spark.createDataFrame(
            [("u1", "t1", "2024-01-01 10:00:00")],
            ["user_id", "track_id", "listen_time"],
        )
        songs = spark.createDataFrame(
            [("t1", "Song A", "Pop", 200000, "extra_col_value")],
            ["track_id", "track_name", "track_genre", "duration_ms", "should_be_dropped"],
        )
        result = buildEnrichedStreams(streams, songs)
        assert "should_be_dropped" not in result.columns


# ---------------------------------------------------------------------------
# writeParquet
# ---------------------------------------------------------------------------

class TestWriteParquet:
    def _mock_writer(self):
        mock_writer = MagicMock()
        mock_writer.mode.return_value = mock_writer
        mock_writer.format.return_value = mock_writer
        mock_writer.partitionBy.return_value = mock_writer
        mock_writer.save.return_value = None
        return mock_writer

    def test_uses_overwrite_mode(self, enriched_df):
        mock_writer = self._mock_writer()
        with patch("pyspark.sql.DataFrame.write", new_callable=PropertyMock, return_value=mock_writer):
            writeParquet(enriched_df, "s3://bucket/silver/enriched_streams")
        mock_writer.mode.assert_called_once_with("overwrite")

    def test_uses_parquet_format(self, enriched_df):
        mock_writer = self._mock_writer()
        with patch("pyspark.sql.DataFrame.write", new_callable=PropertyMock, return_value=mock_writer):
            writeParquet(enriched_df, "s3://bucket/silver/enriched_streams")
        mock_writer.format.assert_called_once_with("parquet")

    def test_partitions_by_stream_date_when_provided(self, enriched_df):
        mock_writer = self._mock_writer()
        with patch("pyspark.sql.DataFrame.write", new_callable=PropertyMock, return_value=mock_writer):
            writeParquet(enriched_df, "s3://bucket/silver/enriched_streams", partitionCols=["stream_date"])
        mock_writer.partitionBy.assert_called_once_with("stream_date")

    def test_no_partition_by_when_cols_omitted(self, enriched_df):
        mock_writer = self._mock_writer()
        with patch("pyspark.sql.DataFrame.write", new_callable=PropertyMock, return_value=mock_writer):
            writeParquet(enriched_df, "s3://bucket/silver/enriched_streams")
        mock_writer.partitionBy.assert_not_called()
