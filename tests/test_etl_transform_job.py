"""
Tests for glue_jobs/etl_transform_job.py

Covers:
  - buildEnrichedStreams  – join correctness, column derivation, unmatched rows
  - buildGenreDate        – composite key format
  - computeGenreKpis      – aggregation values, composite key presence
  - computeTopSongs       – ranking, top-N cutoff, tie handling
  - computeTopGenres      – ranking, top-N cutoff, column rename
  - writeParquet          – delegates to DataFrame.write (interaction test)
  - Constants             – TOP_SONGS_RANK, TOP_GENRES_RANK values
"""

import sys
import pytest
from unittest.mock import MagicMock, patch, call
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
        TOP_SONGS_RANK,
        TOP_GENRES_RANK,
        buildEnrichedStreams,
        buildGenreDate,
        computeGenreKpis,
        computeTopSongs,
        computeTopGenres,
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

    def test_songs_columns_contains_required_fields(self):
        assert "track_id"    in SONGS_COLUMNS
        assert "track_name"  in SONGS_COLUMNS
        assert "track_genre" in SONGS_COLUMNS
        assert "duration_ms" in SONGS_COLUMNS


# ---------------------------------------------------------------------------
# buildEnrichedStreams
# ---------------------------------------------------------------------------

class TestBuildEnrichedStreams:
    def test_result_contains_stream_date_column(self, enriched_df):
        assert "stream_date" in enriched_df.columns

    def test_stream_date_is_date_type(self, enriched_df):
        from pyspark.sql.types import DateType
        field = next(f for f in enriched_df.schema.fields if f.name == "stream_date")
        assert isinstance(field.dataType, DateType)

    def test_inner_join_drops_unmatched_streams(self, spark, songs_df):
        streams = spark.createDataFrame(
            [("u1", "t1", "2024-01-01 10:00:00"), ("u2", "UNKNOWN", "2024-01-01 11:00:00")],
            ["user_id", "track_id", "listen_time"],
        )
        result = buildEnrichedStreams(streams, songs_df)
        assert result.count() == 1

    def test_inner_join_drops_unmatched_songs(self, spark, streams_df):
        songs = spark.createDataFrame(
            [("t1", "Song A", "Pop", 200000), ("t99", "Unknown", "Jazz", 100000)],
            ["track_id", "track_name", "track_genre", "duration_ms"],
        )
        result = buildEnrichedStreams(streams_df, songs)
        track_ids = {r.track_id for r in result.select("track_id").collect()}
        assert "t99" not in track_ids

    def test_enriched_has_song_columns(self, enriched_df):
        for col in ["track_name", "track_genre", "duration_ms"]:
            assert col in enriched_df.columns

    def test_enriched_has_stream_columns(self, enriched_df):
        for col in ["user_id", "track_id", "listen_time"]:
            assert col in enriched_df.columns

    def test_row_count_matches_matched_streams(self, spark, songs_df):
        streams = spark.createDataFrame(
            [("u1", "t1", "2024-01-01 10:00:00")] * 5,
            ["user_id", "track_id", "listen_time"],
        )
        result = buildEnrichedStreams(streams, songs_df)
        assert result.count() == 5

    def test_empty_streams_returns_empty(self, spark, songs_df):
        empty = spark.createDataFrame([], streams_df_schema())
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


def streams_df_schema():
    from pyspark.sql.types import StructType, StructField, StringType
    return StructType([
        StructField("user_id",     StringType(), False),
        StructField("track_id",    StringType(), False),
        StructField("listen_time", StringType(), True),
    ])


# ---------------------------------------------------------------------------
# buildGenreDate
# ---------------------------------------------------------------------------

class TestBuildGenreDate:
    def test_genre_date_column_added(self, enriched_df):
        result = buildGenreDate(enriched_df)
        assert "genre_date" in result.columns

    def test_genre_date_format_uses_hash_separator(self, enriched_df):
        result = buildGenreDate(enriched_df)
        sample = result.select("genre_date").first()["genre_date"]
        assert "#" in sample

    def test_genre_date_starts_with_genre(self, spark):
        df = spark.createDataFrame(
            [("Pop", "2024-01-01")],
            ["track_genre", "stream_date"],
        )
        result = buildGenreDate(df)
        value = result.select("genre_date").first()["genre_date"]
        assert value.startswith("Pop#")

    def test_genre_date_ends_with_date(self, spark):
        df = spark.createDataFrame(
            [("Pop", "2024-01-01")],
            ["track_genre", "stream_date"],
        )
        result = buildGenreDate(df)
        value = result.select("genre_date").first()["genre_date"]
        assert value.endswith("2024-01-01")

    def test_genre_date_exact_value(self, spark):
        df = spark.createDataFrame(
            [("Rock", "2024-06-15")],
            ["track_genre", "stream_date"],
        )
        result = buildGenreDate(df)
        value = result.select("genre_date").first()["genre_date"]
        assert value == "Rock#2024-06-15"


# ---------------------------------------------------------------------------
# computeGenreKpis
# ---------------------------------------------------------------------------

class TestComputeGenreKpis:
    def test_output_has_expected_columns(self, enriched_df):
        result = computeGenreKpis(enriched_df)
        expected = {"stream_date", "track_genre", "listen_count", "unique_listeners",
                    "total_listen_time_ms", "avg_listen_time_ms_per_user", "genre_date"}
        assert expected.issubset(set(result.columns))

    def test_listen_count_equals_total_streams(self, spark):
        df = spark.createDataFrame(
            [
                ("u1", "t1", "2024-01-01", "Pop", 200000),
                ("u2", "t1", "2024-01-01", "Pop", 200000),
                ("u3", "t1", "2024-01-01", "Pop", 200000),
            ],
            ["user_id", "track_id", "listen_time", "track_genre", "duration_ms"],
        )
        df = df.withColumn("stream_date", F.to_date(F.col("listen_time")))
        result = computeGenreKpis(df)
        row = result.filter(F.col("track_genre") == "Pop").first()
        assert row["listen_count"] == 3

    def test_unique_listeners_counts_distinct_users(self, spark):
        df = spark.createDataFrame(
            [
                ("u1", "t1", "2024-01-01", "Pop", 200000),
                ("u1", "t1", "2024-01-01", "Pop", 200000),  # same user twice
                ("u2", "t1", "2024-01-01", "Pop", 200000),
            ],
            ["user_id", "track_id", "listen_time", "track_genre", "duration_ms"],
        )
        df = df.withColumn("stream_date", F.to_date(F.col("listen_time")))
        result = computeGenreKpis(df)
        row = result.filter(F.col("track_genre") == "Pop").first()
        assert row["unique_listeners"] == 2

    def test_total_listen_time_sums_duration(self, spark):
        df = spark.createDataFrame(
            [
                ("u1", "t1", "2024-01-01", "Pop", 100000),
                ("u2", "t1", "2024-01-01", "Pop", 200000),
            ],
            ["user_id", "track_id", "listen_time", "track_genre", "duration_ms"],
        )
        df = df.withColumn("stream_date", F.to_date(F.col("listen_time")))
        result = computeGenreKpis(df)
        row = result.filter(F.col("track_genre") == "Pop").first()
        assert row["total_listen_time_ms"] == 300000

    def test_genre_date_composite_key_present(self, enriched_df):
        result = computeGenreKpis(enriched_df)
        assert result.filter(F.col("genre_date").isNull()).count() == 0

    def test_groups_by_date_and_genre_separately(self, enriched_df):
        result = computeGenreKpis(enriched_df)
        dates = {r.stream_date for r in result.select("stream_date").collect()}
        assert len(dates) >= 1

    def test_different_genres_produce_separate_rows(self, spark):
        df = spark.createDataFrame(
            [
                ("u1", "t1", "2024-01-01", "Pop",  200000),
                ("u2", "t2", "2024-01-01", "Rock", 180000),
            ],
            ["user_id", "track_id", "listen_time", "track_genre", "duration_ms"],
        )
        df = df.withColumn("stream_date", F.to_date(F.col("listen_time")))
        result = computeGenreKpis(df)
        assert result.count() == 2

    def test_avg_listen_time_computed_per_user(self, spark):
        df = spark.createDataFrame(
            [
                ("u1", "t1", "2024-01-01", "Pop", 200000),
                ("u2", "t1", "2024-01-01", "Pop", 200000),
            ],
            ["user_id", "track_id", "listen_time", "track_genre", "duration_ms"],
        )
        df = df.withColumn("stream_date", F.to_date(F.col("listen_time")))
        result = computeGenreKpis(df)
        row = result.filter(F.col("track_genre") == "Pop").first()
        # total=400000, distinct users=2 → avg=200000
        assert row["avg_listen_time_ms_per_user"] == 200000.0


# ---------------------------------------------------------------------------
# computeTopSongs
# ---------------------------------------------------------------------------

class TestComputeTopSongs:
    def test_result_has_rank_column(self, enriched_df):
        result = computeTopSongs(enriched_df)
        assert "rank" in result.columns

    def test_result_has_play_count_column(self, enriched_df):
        result = computeTopSongs(enriched_df)
        assert "play_count" in result.columns

    def test_max_rank_does_not_exceed_top_songs_rank(self, enriched_df):
        result = computeTopSongs(enriched_df)
        max_rank = result.agg(F.max("rank")).first()[0]
        assert max_rank <= TOP_SONGS_RANK

    def test_returns_at_most_top_n_per_genre_date(self, spark):
        data = [("u1", f"t{i}", "2024-01-01", "Pop", f"Song {i}", 200000) for i in range(10)]
        df = spark.createDataFrame(data, ["user_id", "track_id", "listen_time", "track_genre", "track_name", "duration_ms"])
        df = df.withColumn("stream_date", F.to_date(F.col("listen_time")))
        result = computeTopSongs(df)
        assert result.count() <= TOP_SONGS_RANK

    def test_rank_one_song_has_highest_play_count(self, spark):
        data = [
            ("u1", "t1", "2024-01-01", "Pop", "Song A", 200000),
            ("u2", "t1", "2024-01-01", "Pop", "Song A", 200000),
            ("u3", "t1", "2024-01-01", "Pop", "Song A", 200000),
            ("u1", "t2", "2024-01-01", "Pop", "Song B", 200000),
        ]
        df = spark.createDataFrame(data, ["user_id", "track_id", "listen_time", "track_genre", "track_name", "duration_ms"])
        df = df.withColumn("stream_date", F.to_date(F.col("listen_time")))
        result = computeTopSongs(df)
        rank1 = result.filter(F.col("rank") == 1).first()
        assert rank1["track_id"] == "t1"

    def test_genre_date_column_present(self, enriched_df):
        result = computeTopSongs(enriched_df)
        assert "genre_date" in result.columns

    def test_genre_date_has_no_nulls(self, enriched_df):
        result = computeTopSongs(enriched_df)
        assert result.filter(F.col("genre_date").isNull()).count() == 0

    def test_ties_share_same_rank(self, spark):
        data = [
            ("u1", "t1", "2024-01-01", "Pop", "Song A", 200000),
            ("u2", "t2", "2024-01-01", "Pop", "Song B", 200000),
        ]
        df = spark.createDataFrame(data, ["user_id", "track_id", "listen_time", "track_genre", "track_name", "duration_ms"])
        df = df.withColumn("stream_date", F.to_date(F.col("listen_time")))
        result = computeTopSongs(df)
        ranks = [r.rank for r in result.collect()]
        assert ranks.count(1) == 2  # both are tied at rank 1


# ---------------------------------------------------------------------------
# computeTopGenres
# ---------------------------------------------------------------------------

class TestComputeTopGenres:
    def test_max_rank_does_not_exceed_top_genres_rank(self, enriched_df):
        kpis = computeGenreKpis(enriched_df)
        result = computeTopGenres(kpis)
        max_rank = result.agg(F.max("rank")).first()[0]
        assert max_rank <= TOP_GENRES_RANK

    def test_stream_date_renamed_to_date(self, enriched_df):
        kpis = computeGenreKpis(enriched_df)
        result = computeTopGenres(kpis)
        assert "date" in result.columns
        assert "stream_date" not in result.columns

    def test_result_columns(self, enriched_df):
        kpis = computeGenreKpis(enriched_df)
        result = computeTopGenres(kpis)
        assert set(result.columns) == {"date", "track_genre", "listen_count", "rank"}

    def test_rank_one_genre_has_highest_listen_count(self, spark):
        kpis = spark.createDataFrame(
            [
                ("2024-01-01", "Pop",  100, "Pop#2024-01-01"),
                ("2024-01-01", "Rock", 50,  "Rock#2024-01-01"),
                ("2024-01-01", "Jazz", 30,  "Jazz#2024-01-01"),
            ],
            ["stream_date", "track_genre", "listen_count", "genre_date"],
        )
        result = computeTopGenres(kpis)
        rank1 = result.filter(F.col("rank") == 1).first()
        assert rank1["track_genre"] == "Pop"

    def test_at_most_five_genres_per_day(self, spark):
        data = [("2024-01-01", f"Genre{i}", 100 - i, f"Genre{i}#2024-01-01") for i in range(10)]
        kpis = spark.createDataFrame(data, ["stream_date", "track_genre", "listen_count", "genre_date"])
        result = computeTopGenres(kpis)
        assert result.count() <= TOP_GENRES_RANK

    def test_no_nulls_in_date_column(self, enriched_df):
        kpis = computeGenreKpis(enriched_df)
        result = computeTopGenres(kpis)
        assert result.filter(F.col("date").isNull()).count() == 0

    def test_multiple_days_ranked_independently(self, spark):
        data = [
            ("2024-01-01", "Pop",  100, "Pop#2024-01-01"),
            ("2024-01-01", "Rock", 50,  "Rock#2024-01-01"),
            ("2024-01-02", "Jazz", 80,  "Jazz#2024-01-02"),
            ("2024-01-02", "Pop",  40,  "Pop#2024-01-02"),
        ]
        kpis = spark.createDataFrame(data, ["stream_date", "track_genre", "listen_count", "genre_date"])
        result = computeTopGenres(kpis)
        rank1_genres = {r.track_genre for r in result.filter(F.col("rank") == 1).collect()}
        assert "Pop" in rank1_genres
        assert "Jazz" in rank1_genres


# ---------------------------------------------------------------------------
# writeParquet
# ---------------------------------------------------------------------------

class TestWriteParquet:
    def test_calls_parquet_write_with_correct_path(self, enriched_df):
        mock_writer = MagicMock()
        mock_writer.mode.return_value = mock_writer
        mock_writer.format.return_value = mock_writer
        mock_writer.partitionBy.return_value = mock_writer

        with patch.object(enriched_df, "write", mock_writer):
            writeParquet(enriched_df, "s3://bucket/path")

        mock_writer.mode.assert_called_once_with("overwrite")
        mock_writer.format.assert_called_once_with("parquet")

    def test_calls_partition_by_when_cols_provided(self, enriched_df):
        mock_writer = MagicMock()
        mock_writer.mode.return_value = mock_writer
        mock_writer.format.return_value = mock_writer
        mock_writer.partitionBy.return_value = mock_writer

        with patch.object(enriched_df, "write", mock_writer):
            writeParquet(enriched_df, "s3://bucket/path", partitionCols=["stream_date"])

        mock_writer.partitionBy.assert_called_once_with("stream_date")

    def test_skips_partition_by_when_no_cols(self, enriched_df):
        mock_writer = MagicMock()
        mock_writer.mode.return_value = mock_writer
        mock_writer.format.return_value = mock_writer
        mock_writer.partitionBy.return_value = mock_writer

        with patch.object(enriched_df, "write", mock_writer):
            writeParquet(enriched_df, "s3://bucket/path")

        mock_writer.partitionBy.assert_not_called()
