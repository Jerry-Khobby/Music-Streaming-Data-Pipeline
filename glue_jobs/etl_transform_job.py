import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.utils import AnalysisException

from monitoring import buildLogger, SlackNotifier, PipelineMonitor
from monitoring.notifier import resolveWebhookUrl

logger = buildLogger(__name__)

SONGS_COLUMNS           = ["track_id", "track_name", "track_genre", "duration_ms"]
STREAM_DEDUP_KEY        = ["user_id", "track_id", "listen_time"]
REQUIRED_STREAM_COLUMNS = {"user_id", "track_id", "listen_time"}
REQUIRED_SONGS_COLUMNS  = {"track_id", "track_name", "track_genre", "duration_ms"}


def load_table(glue_context, database, table_name) -> DataFrame:
    return (
        glue_context
        .create_dynamic_frame
        .from_catalog(database=database, table_name=table_name)
        .toDF()
    )


def validate_columns(df, required, label):
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"{label} is missing required columns: {missing}")
    logger.info(f"{label} — all required fields are present.")


def check_streams_have_data(streams_df):
    """
    Guard against a stale Glue catalog entry.

    When the raw crawler runs on an empty streams/ prefix (i.e. no CSV files
    have arrived yet, or they were archived before the crawler ran) it registers
    the streams table with ZERO columns.  validate_columns() would catch that
    correctly, but only after we've already paid for a Spark job startup.

    A more informative early-exit: if the DataFrame has no columns at all, the
    catalog schema is stale — treat this the same as "no new streams" so the
    pipeline skips cleanly rather than crashing with a confusing AnalysisException.
    """
    if not streams_df.columns:
        logger.warning(
            "The stream table has no columns — the catalog schema is stale. "
            "The crawler likely ran before any new files landed in S3. "
            "Nothing to process; exiting cleanly."
        )
        return False

    if streams_df.rdd.isEmpty():
        logger.info("No new stream events found. Nothing to process; exiting cleanly.")
        return False

    return True


def build_enriched_streams(streams_df, songs_df) -> DataFrame:
    return (
        streams_df
        .join(songs_df.select(SONGS_COLUMNS), on="track_id", how="inner")
        .withColumn("stream_date", F.to_date(F.col("listen_time")))
    )


def load_existing_partitions(spark, path, dates) -> DataFrame | None:
    try:
        return (
            spark.read.parquet(path)
            .filter(F.col("stream_date").isin(dates))
        )
    except AnalysisException:
        # Silver path does not exist yet on the very first run.
        return None


def merge_and_deduplicate(spark, new_df, silver_path) -> DataFrame:
    affected_dates = [
        row.stream_date
        for row in new_df.select("stream_date").distinct().collect()
    ]
    existing_df = load_existing_partitions(spark, silver_path, affected_dates)

    combined_df = new_df if existing_df is None else existing_df.union(new_df)

    # Cache before dedup so count() and write() share one evaluation pass.
    deduped_df = combined_df.dropDuplicates(STREAM_DEDUP_KEY).cache()
    row_count  = deduped_df.count()

    logger.info(
        f"Processed {len(affected_dates)} day(s) of stream data — "
        f"{row_count:,} unique stream events after removing duplicates."
    )
    return deduped_df


def write_silver(df, path, partition_col):
    (
        df.write
        .mode("overwrite")
        .partitionBy(partition_col)
        .parquet(path)
    )
    logger.info("Enriched stream data saved to the Silver layer in S3.")


if __name__ == "__main__":
    args = getResolvedOptions(sys.argv, ["JOB_NAME", "glue_database", "curated_bucket"])

    sc       = SparkContext()
    glue_ctx = GlueContext(sc)
    spark    = glue_ctx.spark_session
    job      = Job(glue_ctx)
    job.init(args["JOB_NAME"], args)

    # Dynamic partition overwrite: only affected stream_date partitions are
    # replaced, not the entire silver prefix.
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    silver_path = f"s3://{args['curated_bucket']}/silver/enriched_streams"
    database    = args["glue_database"]

    webhookUrl = resolveWebhookUrl(sys.argv)
    notifier   = SlackNotifier(webhookUrl) if webhookUrl else None
    monitor    = PipelineMonitor(args["JOB_NAME"], notifier)

    with monitor.stage("Loading stream and song data from the catalog"):
        streams_df = load_table(glue_ctx, database, "streams")
        songs_df   = load_table(glue_ctx, database, "songs")

    if not check_streams_have_data(streams_df):
        job.commit()
        sys.exit(0)

    with monitor.stage("Checking that all required fields are present"):
        validate_columns(streams_df, REQUIRED_STREAM_COLUMNS, "streams")
        validate_columns(songs_df,   REQUIRED_SONGS_COLUMNS,  "songs")

    with monitor.stage("Joining stream events with song metadata"):
        enriched_df = build_enriched_streams(streams_df, songs_df)

    if enriched_df.rdd.isEmpty():
        logger.warning(
            "No matching records after join — the stream track IDs may not exist "
            "in the songs table. Skipping write to the Silver layer."
        )
        job.commit()
        sys.exit(0)

    with monitor.stage("Removing duplicate plays and merging with existing data"):
        merged_df = merge_and_deduplicate(spark, enriched_df, silver_path)

    with monitor.stage("Writing enriched data to the Silver layer in S3"):
        write_silver(merged_df, silver_path, "stream_date")

    monitor.logSummary()
    job.commit()
