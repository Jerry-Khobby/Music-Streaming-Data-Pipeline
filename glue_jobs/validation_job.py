import sys
import logging
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

REQUIRED_COLUMNS = {
    "streams": {"user_id", "track_id", "listen_time"},
    "songs":   {"track_id", "track_name", "track_genre", "duration_ms"},
    "users":   {"user_id", "user_name", "user_country"},
}


# Custom sentinel exception for "no new streams"
class NoNewStreams(Exception):
    pass


def loadTable(glueContext, database, tableName):
    return (
        glueContext
        .create_dynamic_frame
        .from_catalog(database=database, table_name=tableName)
        .toDF()
    )


def checkMissingColumns(df, tableName):
    missing = REQUIRED_COLUMNS[tableName] - set(df.columns)

    if missing:
        raise ValueError(
            f"[{tableName}] Missing required columns: {missing}"
        )

    logger.info(
        f"[{tableName}] Column check passed — found: {set(df.columns)}"
    )


def checkNonEmpty(df, tableName):
    if df.rdd.isEmpty():

        # Streams table is allowed to be empty
        # because there may simply be no new files
        if tableName == "streams":
            raise NoNewStreams()

        raise ValueError(
            f"[{tableName}] Dataset is empty — pipeline cannot proceed."
        )

    logger.info(f"[{tableName}] Non-empty check passed.")


def validateTable(glueContext, database, tableName):
    df = loadTable(glueContext, database, tableName)

    checkNonEmpty(df, tableName)
    checkMissingColumns(df, tableName)

    logger.info(f"[{tableName}] Validation complete.")


def validateAllTables(glueContext, database):
    for tableName in REQUIRED_COLUMNS:
        validateTable(glueContext, database, tableName)

    logger.info("All datasets passed validation successfully.")


if __name__ == "__main__":

    args = getResolvedOptions(
        sys.argv,
        ["JOB_NAME", "glue_database"]
    )

    sc = SparkContext()
    glueContext = GlueContext(sc)

    job = Job(glueContext)
    job.init(args["JOB_NAME"], args)

    try:
        validateAllTables(
            glueContext,
            args["glue_database"]
        )

    except NoNewStreams:
        logger.info(
            "[streams] No new stream files — exiting cleanly."
        )

        # REQUIRED so Glue marks job as SUCCEEDED
        job.commit()

        sys.exit(0)

    except ValueError as error:
        logger.error(
            f"Validation failed — aborting pipeline: {error}"
        )

        raise

    # Normal successful completion
    job.commit()