import sys
from decimal import Decimal
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import boto3

from monitoring import buildLogger, SlackNotifier, PipelineMonitor
from monitoring.notifier import resolveWebhookUrl

logger = buildLogger(__name__)

GENRE_KPIS_TABLE = "genre_kpis"
TOP_SONGS_TABLE  = "top_songs"
TOP_GENRES_TABLE = "top_genres"


def toDecimal(value):
    if value is None:
        return None
    return Decimal(str(value))


def writePartitionToDynamo(rows, tableName, region):
    dynamodb = boto3.resource("dynamodb", region_name=region)
    table    = dynamodb.Table(tableName)
    with table.batch_writer() as batch:
        for row in rows:
            batch.put_item(Item=row)


def buildGenreKpisItem(row):
    return {
        "genre_date":                  row["genre_date"],
        "stream_date":                 str(row["stream_date"]),
        "track_genre":                 row["track_genre"],
        "listen_count":                int(row["listen_count"]),
        "unique_listeners":            int(row["unique_listeners"]),
        "total_listen_time_ms":        toDecimal(row["total_listen_time_ms"]),
        "avg_listen_time_ms_per_user": toDecimal(row["avg_listen_time_ms_per_user"]),
    }


def buildTopSongsItem(row):
    return {
        "genre_date":  row["genre_date"],
        "rank":        int(row["rank"]),
        "stream_date": str(row["stream_date"]),
        "track_genre": row["track_genre"],
        "track_id":    row["track_id"],
        "track_name":  row["track_name"],
        "play_count":  int(row["play_count"]),
    }


def buildTopGenresItem(row):
    return {
        "date":         str(row["date"]),
        "rank":         int(row["rank"]),
        "track_genre":  row["track_genre"],
        "listen_count": int(row["listen_count"]),
    }


def loadParquet(spark, path):
    return spark.read.parquet(path)


def loadToDynamo(df, tableName, region, itemBuilder):
    def writePartition(rows):
        writePartitionToDynamo(
            [itemBuilder(row.asDict()) for row in rows],
            tableName,
            region,
        )
    df.foreachPartition(writePartition)
    logger.info(f"'{tableName}' — all records written to DynamoDB successfully.")


if __name__ == "__main__":
    args = getResolvedOptions(sys.argv, ["JOB_NAME", "curated_bucket", "aws_region"])

    sc          = SparkContext()
    glueContext = GlueContext(sc)
    spark       = glueContext.spark_session
    job         = Job(glueContext)
    job.init(args["JOB_NAME"], args)

    goldBase = f"s3://{args['curated_bucket']}/gold"
    region   = args["aws_region"]

    webhookUrl = resolveWebhookUrl(sys.argv)
    notifier   = SlackNotifier(webhookUrl) if webhookUrl else None
    monitor    = PipelineMonitor(args["JOB_NAME"], notifier)

    with monitor.stage("Reading aggregated KPI data from the Gold layer"):
        genreKpisDF = loadParquet(spark, f"{goldBase}/genre_kpis").dropDuplicates(["genre_date"])
        topSongsDF  = loadParquet(spark, f"{goldBase}/top_songs").dropDuplicates(["genre_date", "rank"])
        topGenresDF = loadParquet(spark, f"{goldBase}/top_genres").dropDuplicates(["date", "rank"])

    with monitor.stage("Writing genre KPIs to DynamoDB"):
        loadToDynamo(genreKpisDF, GENRE_KPIS_TABLE, region, buildGenreKpisItem)

    with monitor.stage("Writing top songs per genre to DynamoDB"):
        loadToDynamo(topSongsDF, TOP_SONGS_TABLE, region, buildTopSongsItem)

    with monitor.stage("Writing top genres per day to DynamoDB"):
        loadToDynamo(topGenresDF, TOP_GENRES_TABLE, region, buildTopGenresItem)

    monitor.logSummary()
    job.commit()
