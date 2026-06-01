import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.window import Window

from monitoring import buildLogger, SlackNotifier, PipelineMonitor
from monitoring.notifier import resolveWebhookUrl

logger = buildLogger(__name__)

TOP_SONGS_RANK  = 3
TOP_GENRES_RANK = 5


def loadParquet(spark, path) -> DataFrame:
    df = spark.read.parquet(path)
    logger.info(f"Loaded {df.count():,} rows from {path}")
    return df


def computeListenCount(enrichedDF) -> DataFrame:
    return (
        enrichedDF
        .groupBy("stream_date", "track_genre")
        .agg(F.count("*").alias("listen_count"))
    )


def computeUniqueListeners(enrichedDF) -> DataFrame:
    return (
        enrichedDF
        .groupBy("stream_date", "track_genre")
        .agg(F.countDistinct("user_id").alias("unique_listeners"))
    )


def computeListeningTime(enrichedDF) -> DataFrame:
    return (
        enrichedDF
        .groupBy("stream_date", "track_genre")
        .agg(
            F.sum("duration_ms").alias("total_listen_time_ms"),
            (F.sum("duration_ms") / F.countDistinct("user_id")).alias("avg_listen_time_ms_per_user"),
        )
    )


def assembleGenreKpis(enrichedDF) -> DataFrame:
    listenCountDF     = computeListenCount(enrichedDF)
    uniqueListenersDF = computeUniqueListeners(enrichedDF)
    listeningTimeDF   = computeListeningTime(enrichedDF)

    return (
        listenCountDF
        .join(uniqueListenersDF, on=["stream_date", "track_genre"], how="inner")
        .join(listeningTimeDF,   on=["stream_date", "track_genre"], how="inner")
        .withColumn(
            "genre_date",
            F.concat_ws("#", F.col("track_genre"), F.col("stream_date").cast("string"))
        )
    )


def computeTopSongsPerGenre(enrichedDF) -> DataFrame:
    # row_number() guarantees unique ranks even when play_count values are tied
    rankWindow = Window.partitionBy("stream_date", "track_genre").orderBy(F.desc("play_count"), "track_id")
    return (
        enrichedDF
        .groupBy("stream_date", "track_genre", "track_id", "track_name")
        .agg(F.count("*").alias("play_count"))
        .withColumn("rank", F.row_number().over(rankWindow))
        .filter(F.col("rank") <= TOP_SONGS_RANK)
        .withColumn(
            "genre_date",
            F.concat_ws("#", F.col("track_genre"), F.col("stream_date").cast("string"))
        )
    )


def computeTopGenresPerDay(genreKpisDF) -> DataFrame:
    # row_number() guarantees unique ranks even when listen_count values are tied
    rankWindow = Window.partitionBy("stream_date").orderBy(F.desc("listen_count"), "track_genre")
    return (
        genreKpisDF
        .withColumn("rank", F.row_number().over(rankWindow))
        .filter(F.col("rank") <= TOP_GENRES_RANK)
        .select("stream_date", "track_genre", "listen_count", "rank")
        .withColumnRenamed("stream_date", "date")
    )


def writeParquet(df, path, partitionCols=None):
    writer = df.write.mode("overwrite").format("parquet")
    if partitionCols:
        writer = writer.partitionBy(*partitionCols)
    writer.save(path)
    logger.info(f"Results saved to {path}")


if __name__ == "__main__":
    args = getResolvedOptions(sys.argv, ["JOB_NAME", "curated_bucket"])

    sc          = SparkContext()
    glueContext = GlueContext(sc)
    spark       = glueContext.spark_session
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    job = Job(glueContext)
    job.init(args["JOB_NAME"], args)

    silverBase = f"s3://{args['curated_bucket']}/silver"
    goldBase   = f"s3://{args['curated_bucket']}/gold"

    webhookUrl = resolveWebhookUrl(sys.argv)
    notifier   = SlackNotifier(webhookUrl) if webhookUrl else None
    monitor    = PipelineMonitor(args["JOB_NAME"], notifier)

    with monitor.stage("Loading enriched stream data from the Silver layer"):
        enrichedDF = loadParquet(spark, f"{silverBase}/enriched_streams")
        enrichedDF.cache()

    with monitor.stage("Computing genre-level KPIs"):
        genreKpisDF = assembleGenreKpis(enrichedDF)

    with monitor.stage("Ranking the top songs per genre"):
        topSongsDF = computeTopSongsPerGenre(enrichedDF)

    with monitor.stage("Ranking the top genres per day"):
        topGenresDF = computeTopGenresPerDay(genreKpisDF)

    with monitor.stage("Writing KPI results to the Gold layer in S3"):
        writeParquet(genreKpisDF, f"{goldBase}/genre_kpis", partitionCols=["stream_date"])
        writeParquet(topSongsDF,  f"{goldBase}/top_songs",  partitionCols=["stream_date"])
        writeParquet(topGenresDF, f"{goldBase}/top_genres",  partitionCols=["date"])

    enrichedDF.unpersist()
    monitor.logSummary()
    job.commit()
