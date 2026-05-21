"""
Shared fixtures and AWS Glue module stubs.

All glue_jobs modules run Glue bootstrap code at import time (SparkContext,
getResolvedOptions, Job.init …).  We patch those away in fake_glue_env so
that unit tests can import and call the pure functions without a real cluster.
"""

import sys
import pytest
from unittest.mock import MagicMock, patch


# ---------------------------------------------------------------------------
# Stub out awsglue before any test module triggers an import of a glue_job.
# ---------------------------------------------------------------------------

def _build_awsglue_stubs():
    """Install fake awsglue.* packages into sys.modules."""
    glue_utils   = MagicMock()
    glue_context = MagicMock()
    glue_job     = MagicMock()

    glue_utils.getResolvedOptions.return_value = {
        "JOB_NAME":       "test-job",
        "glue_database":  "test_db",
        "curated_bucket": "test-bucket",
        "raw_bucket":     "raw-bucket",
        "archive_bucket": "archive-bucket",
        "aws_region":     "us-east-1",
    }

    sys.modules.setdefault("awsglue",              MagicMock())
    sys.modules.setdefault("awsglue.utils",        glue_utils)
    sys.modules.setdefault("awsglue.context",      glue_context)
    sys.modules.setdefault("awsglue.job",          glue_job)
    sys.modules.setdefault("awsglue.dynamicframe", MagicMock())


_build_awsglue_stubs()


# ---------------------------------------------------------------------------
# Session-scoped local SparkSession (reused across all tests).
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("glue-unit-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


# ---------------------------------------------------------------------------
# Reusable sample DataFrames
# ---------------------------------------------------------------------------

@pytest.fixture()
def streams_df(spark):
    from pyspark.sql.types import StructType, StructField, StringType, LongType

    schema = StructType([
        StructField("user_id",     StringType(), False),
        StructField("track_id",    StringType(), False),
        StructField("listen_time", StringType(), True),
    ])
    data = [
        ("u1", "t1", "2024-01-01 10:00:00"),
        ("u2", "t1", "2024-01-01 11:00:00"),
        ("u1", "t2", "2024-01-01 12:00:00"),
        ("u3", "t2", "2024-01-01 13:00:00"),
        ("u2", "t3", "2024-01-02 09:00:00"),
        ("u1", "t3", "2024-01-02 10:00:00"),
        ("u4", "t1", "2024-01-02 11:00:00"),
    ]
    return spark.createDataFrame(data, schema)


@pytest.fixture()
def songs_df(spark):
    from pyspark.sql.types import StructType, StructField, StringType, LongType

    schema = StructType([
        StructField("track_id",    StringType(), False),
        StructField("track_name",  StringType(), True),
        StructField("track_genre", StringType(), True),
        StructField("duration_ms", LongType(),   True),
    ])
    data = [
        ("t1", "Song A", "Pop",  200000),
        ("t2", "Song B", "Rock", 180000),
        ("t3", "Song C", "Pop",  220000),
    ]
    return spark.createDataFrame(data, schema)


@pytest.fixture()
def enriched_df(spark, streams_df, songs_df):
    """Pre-built enriched streams fixture (avoids re-testing join in every test)."""
    from pyspark.sql import functions as F
    songs_cols = ["track_id", "track_name", "track_genre", "duration_ms"]
    return (
        streams_df
        .join(songs_df.select(songs_cols), on="track_id", how="inner")
        .withColumn("stream_date", F.to_date(F.col("listen_time")))
    )
