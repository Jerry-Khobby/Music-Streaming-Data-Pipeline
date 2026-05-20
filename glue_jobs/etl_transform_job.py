import sys
import logging
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.window import Window

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SONGS_COLUMNS  = ["track_id", "track_name", "track_genre", "duration_ms"]
TOP_SONGS_RANK = 3
TOP_GENRES_RANK = 5


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


def buildGenreDate(df) -> DataFrame:
    return df.withColumn(
        "genre_date",
        F.concat_ws("#", F.col("track_genre"), F.col("stream_date").cast("string"))
    )


def computeGenreKpis(enrichedDF) -> DataFrame:
    kpisDF = (
        enrichedDF
        .groupBy("stream_date", "track_genre")
        .agg(
            F.count("*").alias("listen_count"),
            F.countDistinct("user_id").alias("unique_listeners"),
            F.sum("duration_ms").alias("total_listen_time_ms"),
            (F.sum("duration_ms") / F.countDistinct("user_id")).alias("avg_listen_time_ms_per_user"),
        )
    )
    return buildGenreDate(kpisDF)


def computeTopSongs(enrichedDF) -> DataFrame:
    rankWindow = Window.partitionBy("stream_date", "track_genre").orderBy(F.desc("play_count"))
    rankedDF = (
        enrichedDF
        .groupBy("stream_date", "track_genre", "track_id", "track_name")
        .agg(F.count("*").alias("play_count"))
        .withColumn("rank", F.rank().over(rankWindow))
        .filter(F.col("rank") <= TOP_SONGS_RANK)
    )
    return buildGenreDate(rankedDF)


def computeTopGenres(genreKpisDF) -> DataFrame:
    rankWindow = Window.partitionBy("stream_date").orderBy(F.desc("listen_count"))
    return (
        genreKpisDF
        .withColumn("rank", F.rank().over(rankWindow))
        .filter(F.col("rank") <= TOP_GENRES_RANK)
        .select("stream_date", "track_genre", "listen_count", "rank")
        .withColumnRenamed("stream_date", "date")
    )


def writeParquet(df, path, partitionCols=None):
    writer = df.write.mode("overwrite").format("parquet")
    if partitionCols:
        writer = writer.partitionBy(*partitionCols)
    writer.save(path)
    logger.info(f"Written {df.count()} rows to {path}")


args = getResolvedOptions(sys.argv, ["JOB_NAME", "glue_database", "curated_bucket"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

goldBase = f"s3://{args['curated_bucket']}/gold"
database = args["glue_database"]

streamsDF = loadTable(glueContext, database, "streams")
songsDF   = loadTable(glueContext, database, "songs")

enrichedDF = buildEnrichedStreams(streamsDF, songsDF)
enrichedDF.cache()

genreKpisDF = computeGenreKpis(enrichedDF)
topSongsDF  = computeTopSongs(enrichedDF)
topGenresDF = computeTopGenres(genreKpisDF)

writeParquet(genreKpisDF, f"{goldBase}/genre_kpis", partitionCols=["stream_date"])
writeParquet(topSongsDF,  f"{goldBase}/top_songs",  partitionCols=["stream_date"])
writeParquet(topGenresDF, f"{goldBase}/top_genres",  partitionCols=["date"])

enrichedDF.unpersist()
logger.info("ETL transformation complete.")
job.commit()
