import sys
import logging
import time
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

# Custom exceptions
class NoNewStreams(Exception):
    pass

class TableNotFound(Exception):
    pass

def table_exists(glueContext, database, tableName):
    """Check if a table exists in the Glue Data Catalog"""
    try:
        glueContext.create_dynamic_frame.from_catalog(
            database=database, 
            table_name=tableName
        )
        return True
    except Exception as e:
        error_msg = str(e)
        if "Table not found" in error_msg or "not found" in error_msg or "does not exist" in error_msg:
            logger.warning(f"Table {tableName} not found in database {database}")
            return False
        raise

def loadTable(glueContext, database, tableName):
    """Load table with better error handling"""
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
            raise TableNotFound(f"Table '{tableName}' does not exist in database '{database}'. The crawler may not have completed yet.")
        raise

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
        # Streams table is allowed to be empty because there may simply be no new files
        if tableName == "streams":
            raise NoNewStreams()
        
        raise ValueError(
            f"[{tableName}] Dataset is empty — pipeline cannot proceed."
        )

    logger.info(f"[{tableName}] Non-empty check passed.")

def validateTable(glueContext, database, tableName):
    """Validate a single table with retry logic for tables not yet created"""
    max_retries = 3
    retry_delay = 10  # seconds
    
    for attempt in range(max_retries):
        try:
            df = loadTable(glueContext, database, tableName)
            checkNonEmpty(df, tableName)
            checkMissingColumns(df, tableName)
            logger.info(f"[{tableName}] Validation complete on attempt {attempt + 1}.")
            return
            
        except TableNotFound as e:
            logger.warning(f"Attempt {attempt + 1}/{max_retries}: {str(e)}")
            if attempt < max_retries - 1:
                wait_time = retry_delay * (2 ** attempt)  # Exponential: 10, 20, 40
                logger.info(f"Waiting {wait_time} seconds for crawler to complete...")
                time.sleep(wait_time)
            else:
                raise ValueError(
                    f"Table '{tableName}' still not found after {max_retries} attempts. "
                    f"The Glue crawler may have failed. Check crawler logs in CloudWatch."
                )
        
        except NoNewStreams:
            # Re-raise to be handled by main
            raise
        
        except Exception as e:
            # Re-raise other exceptions immediately (no retry for other errors)
            logger.error(f"Non-retryable error for table {tableName}: {str(e)}")
            raise

def validateAllTables(glueContext, database):
    """Validate all required tables"""
    logger.info(f"Starting validation of all tables in database: {database}")
    
    for tableName in REQUIRED_COLUMNS:
        logger.info(f"\n--- Validating table: {tableName} ---")
        validateTable(glueContext, database, tableName)
    
    logger.info("\n✅ All datasets passed validation successfully.")

if __name__ == "__main__":
    args = getResolvedOptions(
        sys.argv,
        ["JOB_NAME", "glue_database"]
    )
    
    logger.info(f"Starting validation job with database: {args['glue_database']}")
    
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
        logger.info("[streams] No new stream files — exiting cleanly.")
        job.commit()
        sys.exit(0)
        
    except ValueError as error:
        logger.error(f"❌ Validation failed — aborting pipeline: {error}")
        # Don't catch - let it fail the job
        raise
        
    except Exception as error:
        logger.error(f"❌ Unexpected error during validation: {error}")
        raise
    
    # Normal successful completion
    job.commit()
    logger.info("\n✅ Validation job completed successfully!")