"""
Tests for glue_jobs/kpi_aggregation_job.py

Covers:
  - computeListenCount       – grouping, count values
  - computeUniqueListeners   – distinct user counting
  - computeListeningTime     – total and average duration
  - assembleGenreKpis        – join correctness, genre_date key
  - computeTopSongsPerGenre  – ranking, top-N cutoff, genre_date key
  - computeTopGenresPerDay   – ranking, top-N cutoff, column rename
  - writeParquet             – write interaction
  - Constants                – TOP_SONGS_RANK, TOP_GENRES_RANK
"""

import sys
import pytest
from unittest.mock import MagicMock, patch
from pyspark.sql import functions as F

with patch("pyspark.context.SparkContext"):
    sys.argv = ["kpi_aggregation_job", "--JOB_NAME", "test", "--curated_bucket", "bucket"]
    from glue_jobs.kpi_aggregation_job import (
        TOP_SONGS_RANK,
        TOP_GENRES_RANK,
        computeListenCount,
        computeUniqueListeners,
        computeListeningTime,
        assembleGenreKpis,
        computeTopSongsPerGenre,
        computeTopGenresPerDay,
        writeParquet,
    )


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

class TestConstants:
    def test_top_songs_rank_is_three(self):
        assert TOP_SONGS_RANK == 3

    def test_top_genres_rank_is_five(self):
        assert TOP_GENRES_RANK == 5


# ---------------------------------------------------------------------------
# Shared fixture helpers
# ---------------------------------------------------------------------------

def make_enriched(spark, rows=None):
    """Return a minimal enriched_streams DataFrame."""
    if rows is None:
        rows = [
            ("u1", "t1", "Song A", "Pop",  200000, "2024-01-01"),
            ("u2", "t1", "Song A", "Pop",  200000, "2024-01-01"),
            ("u1", "t2", "Song B", "Rock", 180000, "2024-01-01"),
            ("u3", "t2", "Song B", "Rock", 180000, "2024-01-01"),
            ("u2", "t3", "Song C", "Pop",  220000, "2024-01-02"),
            ("u4", "t1", "Song A", "Pop",  200000, "2024-01-02"),
        ]
    return spark.createDataFrame(
        rows,
        ["user_id", "track_id", "track_name", "track_genre", "duration_ms", "stream_date"],
    )


# ---------------------------------------------------------------------------
# computeListenCount
# ---------------------------------------------------------------------------

class TestComputeListenCount:
    def test_returns_listen_count_column(self, spark):
        df = make_enriched(spark)
        result = computeListenCount(df)
        assert "listen_count" in result.columns

    def test_groups_by_date_and_genre(self, spark):
        df = make_enriched(spark)
        result = computeListenCount(df)
        assert "stream_date" in result.columns
        assert "track_genre" in result.columns

    def test_count_equals_total_rows_for_group(self, spark):
        rows = [("u1", "t1", "Song A", "Pop", 200000, "2024-01-01")] * 5
        df = make_enriched(spark, rows)
        result = computeListenCount(df)
        count = result.filter(F.col("track_genre") == "Pop").first()["listen_count"]
        assert count == 5

    def test_separate_rows_for_each_genre(self, spark):
        df = make_enriched(spark)
        result = computeListenCount(df).filter(F.col("stream_date") == "2024-01-01")
        genres = {r.track_genre for r in result.collect()}
        assert "Pop" in genres
        assert "Rock" in genres

    def test_no_null_counts(self, spark):
        df = make_enriched(spark)
        result = computeListenCount(df)
        assert result.filter(F.col("listen_count").isNull()).count() == 0


# ---------------------------------------------------------------------------
# computeUniqueListeners
# ---------------------------------------------------------------------------

class TestComputeUniqueListeners:
    def test_returns_unique_listeners_column(self, spark):
        df = make_enriched(spark)
        result = computeUniqueListeners(df)
        assert "unique_listeners" in result.columns

    def test_deduplicates_same_user(self, spark):
        rows = [
            ("u1", "t1", "Song A", "Pop", 200000, "2024-01-01"),
            ("u1", "t1", "Song A", "Pop", 200000, "2024-01-01"),
            ("u2", "t1", "Song A", "Pop", 200000, "2024-01-01"),
        ]
        df = make_enriched(spark, rows)
        result = computeUniqueListeners(df)
        count = result.filter(F.col("track_genre") == "Pop").first()["unique_listeners"]
        assert count == 2

    def test_single_user_returns_one(self, spark):
        rows = [("u1", "t1", "Song A", "Pop", 200000, "2024-01-01")] * 3
        df = make_enriched(spark, rows)
        result = computeUniqueListeners(df)
        assert result.first()["unique_listeners"] == 1

    def test_no_null_unique_listeners(self, spark):
        df = make_enriched(spark)
        result = computeUniqueListeners(df)
        assert result.filter(F.col("unique_listeners").isNull()).count() == 0


# ---------------------------------------------------------------------------
# computeListeningTime
# ---------------------------------------------------------------------------

class TestComputeListeningTime:
    def test_has_total_and_avg_columns(self, spark):
        df = make_enriched(spark)
        result = computeListeningTime(df)
        assert "total_listen_time_ms" in result.columns
        assert "avg_listen_time_ms_per_user" in result.columns

    def test_total_is_sum_of_durations(self, spark):
        rows = [
            ("u1", "t1", "Song A", "Pop", 100000, "2024-01-01"),
            ("u2", "t1", "Song A", "Pop", 200000, "2024-01-01"),
        ]
        df = make_enriched(spark, rows)
        result = computeListeningTime(df)
        row = result.filter(F.col("track_genre") == "Pop").first()
        assert row["total_listen_time_ms"] == 300000

    def test_avg_divides_total_by_distinct_users(self, spark):
        rows = [
            ("u1", "t1", "Song A", "Pop", 100000, "2024-01-01"),
            ("u2", "t1", "Song A", "Pop", 100000, "2024-01-01"),
        ]
        df = make_enriched(spark, rows)
        result = computeListeningTime(df)
        row = result.filter(F.col("track_genre") == "Pop").first()
        assert row["avg_listen_time_ms_per_user"] == 100000.0

    def test_no_null_totals(self, spark):
        df = make_enriched(spark)
        result = computeListeningTime(df)
        assert result.filter(F.col("total_listen_time_ms").isNull()).count() == 0


# ---------------------------------------------------------------------------
# assembleGenreKpis
# ---------------------------------------------------------------------------

class TestAssembleGenreKpis:
    def test_has_all_expected_columns(self, spark):
        df = make_enriched(spark)
        result = assembleGenreKpis(df)
        expected = {
            "stream_date", "track_genre", "listen_count",
            "unique_listeners", "total_listen_time_ms",
            "avg_listen_time_ms_per_user", "genre_date",
        }
        assert expected.issubset(set(result.columns))

    def test_genre_date_uses_hash_separator(self, spark):
        df = make_enriched(spark)
        result = assembleGenreKpis(df)
        sample = result.select("genre_date").first()["genre_date"]
        assert "#" in sample

    def test_no_null_genre_dates(self, spark):
        df = make_enriched(spark)
        result = assembleGenreKpis(df)
        assert result.filter(F.col("genre_date").isNull()).count() == 0

    def test_listen_count_and_unique_listeners_aligned(self, spark):
        rows = [
            ("u1", "t1", "Song A", "Pop", 200000, "2024-01-01"),
            ("u2", "t1", "Song A", "Pop", 200000, "2024-01-01"),
        ]
        df = make_enriched(spark, rows)
        result = assembleGenreKpis(df)
        row = result.first()
        assert row["listen_count"] == 2
        assert row["unique_listeners"] == 2

    def test_same_user_counted_once_in_unique_listeners(self, spark):
        rows = [
            ("u1", "t1", "Song A", "Pop", 200000, "2024-01-01"),
            ("u1", "t1", "Song A", "Pop", 200000, "2024-01-01"),
        ]
        df = make_enriched(spark, rows)
        result = assembleGenreKpis(df)
        row = result.first()
        assert row["unique_listeners"] == 1
        assert row["listen_count"] == 2

    def test_multiple_genres_produce_multiple_rows(self, spark):
        df = make_enriched(spark)
        result = assembleGenreKpis(df).filter(F.col("stream_date") == "2024-01-01")
        assert result.count() >= 2


# ---------------------------------------------------------------------------
# computeTopSongsPerGenre
# ---------------------------------------------------------------------------

class TestComputeTopSongsPerGenre:
    def test_rank_column_present(self, spark):
        df = make_enriched(spark)
        result = computeTopSongsPerGenre(df)
        assert "rank" in result.columns

    def test_max_rank_within_top_songs_rank(self, spark):
        data = [(f"u{i}", f"t{i}", f"Song {i}", "Pop", 200000, "2024-01-01") for i in range(10)]
        df = make_enriched(spark, data)
        result = computeTopSongsPerGenre(df)
        max_rank = result.agg(F.max("rank")).first()[0]
        assert max_rank <= TOP_SONGS_RANK

    def test_highest_play_count_gets_rank_one(self, spark):
        rows = [
            ("u1", "t1", "Song A", "Pop", 200000, "2024-01-01"),
            ("u2", "t1", "Song A", "Pop", 200000, "2024-01-01"),
            ("u3", "t1", "Song A", "Pop", 200000, "2024-01-01"),
            ("u4", "t2", "Song B", "Pop", 180000, "2024-01-01"),
        ]
        df = make_enriched(spark, rows)
        result = computeTopSongsPerGenre(df)
        rank1 = result.filter(F.col("rank") == 1).first()
        assert rank1["track_id"] == "t1"

    def test_genre_date_present_and_no_nulls(self, spark):
        df = make_enriched(spark)
        result = computeTopSongsPerGenre(df)
        assert "genre_date" in result.columns
        assert result.filter(F.col("genre_date").isNull()).count() == 0

    def test_play_count_column_present(self, spark):
        df = make_enriched(spark)
        result = computeTopSongsPerGenre(df)
        assert "play_count" in result.columns


# ---------------------------------------------------------------------------
# computeTopGenresPerDay
# ---------------------------------------------------------------------------

class TestComputeTopGenresPerDay:
    def test_stream_date_renamed_to_date(self, spark):
        kpis = assembleGenreKpis(make_enriched(spark))
        result = computeTopGenresPerDay(kpis)
        assert "date" in result.columns
        assert "stream_date" not in result.columns

    def test_result_columns_exact(self, spark):
        kpis = assembleGenreKpis(make_enriched(spark))
        result = computeTopGenresPerDay(kpis)
        assert set(result.columns) == {"date", "track_genre", "listen_count", "rank"}

    def test_max_rank_within_top_genres_rank(self, spark):
        data = [("u1", "t1", "Song", f"Genre{i}", 200000, "2024-01-01") for i in range(10)]
        df = make_enriched(spark, data)
        kpis = assembleGenreKpis(df)
        result = computeTopGenresPerDay(kpis)
        max_rank = result.agg(F.max("rank")).first()[0]
        assert max_rank <= TOP_GENRES_RANK

    def test_rank_one_has_highest_listen_count(self, spark):
        from pyspark.sql.types import StructType, StructField, StringType, LongType
        kpis = spark.createDataFrame(
            [
                ("2024-01-01", "Pop",  200, "Pop#2024-01-01"),
                ("2024-01-01", "Rock", 50,  "Rock#2024-01-01"),
            ],
            ["stream_date", "track_genre", "listen_count", "genre_date"],
        )
        result = computeTopGenresPerDay(kpis)
        rank1 = result.filter(F.col("rank") == 1).first()
        assert rank1["track_genre"] == "Pop"

    def test_independent_ranking_per_day(self, spark):
        kpis = spark.createDataFrame(
            [
                ("2024-01-01", "Pop",  100, "Pop#2024-01-01"),
                ("2024-01-01", "Rock", 50,  "Rock#2024-01-01"),
                ("2024-01-02", "Jazz", 80,  "Jazz#2024-01-02"),
                ("2024-01-02", "Pop",  40,  "Pop#2024-01-02"),
            ],
            ["stream_date", "track_genre", "listen_count", "genre_date"],
        )
        result = computeTopGenresPerDay(kpis)
        rank1_genres = {r.track_genre for r in result.filter(F.col("rank") == 1).collect()}
        assert "Pop" in rank1_genres   # rank 1 on 2024-01-01
        assert "Jazz" in rank1_genres  # rank 1 on 2024-01-02


# ---------------------------------------------------------------------------
# writeParquet
# ---------------------------------------------------------------------------

class TestWriteParquet:
    def test_writes_with_overwrite_mode(self, enriched_df):
        mock_writer = MagicMock()
        mock_writer.mode.return_value = mock_writer
        mock_writer.format.return_value = mock_writer
        mock_writer.partitionBy.return_value = mock_writer

        with patch.object(enriched_df, "write", mock_writer):
            writeParquet(enriched_df, "s3://bucket/path", partitionCols=["stream_date"])

        mock_writer.mode.assert_called_once_with("overwrite")

    def test_writes_parquet_format(self, enriched_df):
        mock_writer = MagicMock()
        mock_writer.mode.return_value = mock_writer
        mock_writer.format.return_value = mock_writer
        mock_writer.partitionBy.return_value = mock_writer

        with patch.object(enriched_df, "write", mock_writer):
            writeParquet(enriched_df, "s3://bucket/path", partitionCols=["stream_date"])

        mock_writer.format.assert_called_once_with("parquet")

    def test_no_partition_by_when_cols_omitted(self, enriched_df):
        mock_writer = MagicMock()
        mock_writer.mode.return_value = mock_writer
        mock_writer.format.return_value = mock_writer
        mock_writer.save.return_value = None

        with patch.object(enriched_df, "write", mock_writer):
            writeParquet(enriched_df, "s3://bucket/path")

        mock_writer.partitionBy.assert_not_called()
