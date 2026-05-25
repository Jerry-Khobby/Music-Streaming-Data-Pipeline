import sys
import logging
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import DataFrame, functions as F

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SONGS_COLUMNS = ["track_id", "track_name", "track_genre", "duration_ms"]


def loadTable(glueContext, database, tableName) -> DataFrame:
    return (
        glueContext
        .create_dynamic_frame
        .from_catalog(database=database, table_name=tableName)
        .toDF()
    )


def buildEnrichedStreams(streamsDF, songsDF) -> DataFrame:
    return (
        streamsDF
        .join(songsDF.select(SONGS_COLUMNS), on="track_id", how="inner")
        .withColumn("stream_date", F.to_date(F.col("listen_time")))
    )


def writeParquet(df, path, partitionCols=None):
    writer = df.write.mode("overwrite").format("parquet")
    if partitionCols:
        writer = writer.partitionBy(*partitionCols)
    writer.save(path)
    logger.info(f"Written {df.count()} rows to {path}")


if __name__ == "__main__":
    args = getResolvedOptions(sys.argv, ["JOB_NAME", "glue_database", "curated_bucket"])

    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args["JOB_NAME"], args)

    silverBase = f"s3://{args['curated_bucket']}/silver"
    database   = args["glue_database"]

    streamsDF  = loadTable(glueContext, database, "streams")
    songsDF    = loadTable(glueContext, database, "songs")
    enrichedDF = buildEnrichedStreams(streamsDF, songsDF)

    writeParquet(enrichedDF, f"{silverBase}/enriched_streams", partitionCols=["stream_date"])

    logger.info("Bronze → Silver complete. Enriched streams written to silver layer.")
    job.commit()



