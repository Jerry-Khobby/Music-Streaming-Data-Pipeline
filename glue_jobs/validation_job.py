import sys
import time
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from monitoring import buildLogger, SlackNotifier, PipelineMonitor
from monitoring.notifier import resolveWebhookUrl

logger = buildLogger(__name__)

REQUIRED_COLUMNS = {
    "streams": {"user_id", "track_id", "listen_time"},
    "songs":   {"track_id", "track_name", "track_genre", "duration_ms"},
    "users":   {"user_id", "user_name", "user_country"},
}

MAX_RETRIES  = 3
INITIAL_WAIT = 10


class NoNewStreams(Exception):
    pass

class TableNotFound(Exception):
    pass


def loadTable(glueContext, database, tableName):
    try:
        return (
            glueContext
            .create_dynamic_frame
            .from_catalog(database=database, table_name=tableName)
            .toDF()
        )
    except Exception as e:
        error_msg = str(e)
        if "Table not found" in error_msg or "not found" in error_msg or "does not exist" in error_msg:
            raise TableNotFound(
                f"The '{tableName}' table does not exist in database '{database}'. "
                "The Glue crawler may still be running."
            )
        raise


def checkMissingColumns(df, tableName):
    missing = REQUIRED_COLUMNS[tableName] - set(df.columns)
    if missing:
        raise ValueError(
            f"The '{tableName}' table is missing required columns: {missing}"
        )
    logger.info(f"'{tableName}' — all required fields are present.")


def checkNonEmpty(df, tableName):
    if df.rdd.isEmpty():
        if tableName == "streams":
            raise NoNewStreams()
        raise ValueError(
            f"The '{tableName}' table has no data — the pipeline cannot continue."
        )
    logger.info(f"'{tableName}' — data is present and ready.")


def validateTable(glueContext, database, tableName):
    for attempt in range(MAX_RETRIES):
        try:
            df = loadTable(glueContext, database, tableName)
            checkNonEmpty(df, tableName)
            checkMissingColumns(df, tableName)
            logger.info(f"'{tableName}' — passed all checks.")
            return

        except TableNotFound as e:
            logger.warning(f"Attempt {attempt + 1}/{MAX_RETRIES}: {str(e)}")
            if attempt < MAX_RETRIES - 1:
                waitTime = INITIAL_WAIT * (2 ** attempt)
                logger.info(f"Waiting {waitTime}s for the crawler to finish...")
                time.sleep(waitTime)
            else:
                raise ValueError(
                    f"The '{tableName}' table still does not exist after {MAX_RETRIES} attempts. "
                    "Check whether the Glue crawler failed — look in CloudWatch Logs for details."
                )

        except NoNewStreams:
            raise

        except Exception as e:
            logger.error(f"Unexpected error while checking '{tableName}': {str(e)}")
            raise


def validateAllTables(glueContext, database):
    logger.info(f"Checking all required tables in database: {database}")
    for tableName in REQUIRED_COLUMNS:
        validateTable(glueContext, database, tableName)
    logger.info("All tables passed validation — data quality looks good.")


if __name__ == "__main__":
    args = getResolvedOptions(sys.argv, ["JOB_NAME", "glue_database"])

    sc          = SparkContext()
    glueContext = GlueContext(sc)
    job         = Job(glueContext)
    job.init(args["JOB_NAME"], args)

    webhookUrl = resolveWebhookUrl(sys.argv)
    notifier   = SlackNotifier(webhookUrl) if webhookUrl else None
    monitor    = PipelineMonitor(args["JOB_NAME"], notifier)

    try:
        with monitor.stage("Validating all data sources"):
            validateAllTables(glueContext, args["glue_database"])

    except NoNewStreams:
        logger.info("No new stream files have arrived yet — pipeline will skip this run.")
        job.commit()
        sys.exit(0)

    except ValueError as error:
        logger.error(f"Data validation failed — the pipeline cannot continue: {error}")
        raise

    except Exception as error:
        logger.error(f"An unexpected error stopped the validation job: {error}")
        raise

    monitor.logSummary()
    job.commit()
