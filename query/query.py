import logging
import boto3
from boto3.dynamodb.conditions import Key

logging.basicConfig(
    filename="query_results.log",
    filemode="w",
    level=logging.INFO,
    format="%(message)s",
)

log = logging.getLogger(__name__)

dynamodb = boto3.resource("dynamodb", region_name="us-east-1")


def logGenreKpiScan():
    log.info("\n========== SCANNING GENRE KPIs (first 5 items) ==========")
    table = dynamodb.Table("genre_kpis")
    response = table.scan(Limit=5)
    for item in response["Items"]:
        log.info(item)


def logGenreKpiByKey(genreDate):
    log.info("\n========== GENRE KPIs — specific genre ==========")
    table = dynamodb.Table("genre_kpis")
    response = table.get_item(Key={"genre_date": genreDate})
    item = response.get("Item")
    if item:
        log.info(f"Genre:                  {item['track_genre']}")
        log.info(f"Date:                   {item['stream_date']}")
        log.info(f"Listen Count:           {item['listen_count']}")
        log.info(f"Unique Listeners:       {item['unique_listeners']}")
        log.info(f"Total Listen Time (ms): {item['total_listen_time_ms']}")
        log.info(f"Avg Listen Time (ms):   {item['avg_listen_time_ms_per_user']}")
    else:
        log.info("No item found — check the genre name and date")


def logTopSongsByGenreDate(genreDate):
    log.info("\n========== TOP 3 SONGS — specific genre ==========")
    table = dynamodb.Table("top_songs")
    response = table.query(KeyConditionExpression=Key("genre_date").eq(genreDate))
    for item in sorted(response["Items"], key=lambda x: int(x["rank"])):
        log.info(f"  Rank {item['rank']}: {item['track_name']} | plays: {item['play_count']}")


def logTopGenresByDate(streamDate):
    log.info("\n========== TOP 5 GENRES ==========")
    table = dynamodb.Table("top_genres")
    response = table.query(KeyConditionExpression=Key("date").eq(streamDate))
    for item in sorted(response["Items"], key=lambda x: int(x["rank"])):
        log.info(f"  Rank {item['rank']}: {item['track_genre']} — {item['listen_count']} listens")


def logTableRowCounts():
    log.info("\n========== TABLE ROW COUNTS ==========")
    for tableName in ["genre_kpis", "top_songs", "top_genres"]:
        table = dynamodb.Table(tableName)
        response = table.scan(Select="COUNT")
        log.info(f"  {tableName}: {response['Count']} items")


GENRE_DATE = "bluegrass#2024-06-25"
STREAM_DATE = "2024-06-25"

logGenreKpiScan()
logGenreKpiByKey(GENRE_DATE)
logTopSongsByGenreDate(GENRE_DATE)
logTopGenresByDate(STREAM_DATE)
logTableRowCounts()
