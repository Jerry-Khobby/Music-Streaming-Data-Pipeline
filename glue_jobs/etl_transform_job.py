import sys
import logging
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.utils import AnalysisException

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SONGS_COLUMNS     = ["track_id", "track_name", "track_genre", "duration_ms"]
STREAM_DEDUP_KEY  = ["user_id", "track_id", "listen_time"]
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
        return None


def merge_and_deduplicate(spark, new_df, silver_path) -> DataFrame:
    affected_dates = [
        row.stream_date
        for row in new_df.select("stream_date").distinct().collect()
    ]
    existing_df = load_existing_partitions(spark, silver_path, affected_dates)

    combined_df = new_df if existing_df is None else existing_df.union(new_df)

    # Cache before dedup so count() and write() share one evaluation pass
    deduped_df = combined_df.dropDuplicates(STREAM_DEDUP_KEY).cache()
    row_count  = deduped_df.count()

    logger.info(
        f"Merged {len(affected_dates)} date partition(s); "
        f"{row_count} rows after deduplication."
    )
    return deduped_df


def write_silver(df, path, partition_col):
    (
        df.write
        .mode("overwrite")
        .partitionBy(partition_col)
        .parquet(path)
    )
    logger.info(f"Silver layer written to {path}, partitioned by {partition_col}.")


if __name__ == "__main__":
    args = getResolvedOptions(sys.argv, ["JOB_NAME", "glue_database", "curated_bucket"])

    sc         = SparkContext()
    glue_ctx   = GlueContext(sc)
    spark      = glue_ctx.spark_session
    job        = Job(glue_ctx)
    job.init(args["JOB_NAME"], args)

    # Dynamic partition overwrite ensures only affected stream_date partitions
    # are replaced, not the entire silver prefix.
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    silver_path = f"s3://{args['curated_bucket']}/silver/enriched_streams"
    database    = args["glue_database"]

    streams_df = load_table(glue_ctx, database, "streams")
    songs_df   = load_table(glue_ctx, database, "songs")

    validate_columns(streams_df, REQUIRED_STREAM_COLUMNS, "streams")
    validate_columns(songs_df,   REQUIRED_SONGS_COLUMNS,  "songs")

    enriched_df = build_enriched_streams(streams_df, songs_df)

    if enriched_df.rdd.isEmpty():
        logger.warning("No rows produced after join. Skipping silver write.")
        job.commit()
        sys.exit(0)

    merged_df = merge_and_deduplicate(spark, enriched_df, silver_path)
    write_silver(merged_df, silver_path, "stream_date")

    logger.info("Bronze -> Silver complete. Enriched streams written to silver layer.")
    job.commit()
