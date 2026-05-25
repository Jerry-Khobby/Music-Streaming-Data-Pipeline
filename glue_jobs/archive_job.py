import sys
import logging
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
from awsglue.context import GlueContext
from pyspark.context import SparkContext
import boto3

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

STREAMS_PREFIX  = "streams/"
S3_DELETE_LIMIT = 1000


def list_stream_objects(s3_client, bucket):
    paginator = s3_client.get_paginator("list_objects_v2")
    pages     = paginator.paginate(Bucket=bucket, Prefix=STREAMS_PREFIX)
    return [
        obj["Key"]
        for page in pages
        for obj in page.get("Contents", [])
        if not obj["Key"].endswith("/")
    ]


def copy_objects(s3_client, source_bucket, archive_bucket, keys):
    for key in keys:
        s3_client.copy_object(
            CopySource={"Bucket": source_bucket, "Key": key},
            Bucket=archive_bucket,
            Key=key,
        )
    logger.info(f"Copied {len(keys)} file(s) to s3://{archive_bucket}/{STREAMS_PREFIX}")


def bulk_delete_objects(s3_client, bucket, keys):
    # S3 delete_objects accepts up to 1000 keys per call
    for i in range(0, len(keys), S3_DELETE_LIMIT):
        batch = [{"Key": k} for k in keys[i : i + S3_DELETE_LIMIT]]
        response = s3_client.delete_objects(
            Bucket=bucket,
            Delete={"Objects": batch, "Quiet": False},
        )
        errors = response.get("Errors", [])
        if errors:
            failed_keys = [e["Key"] for e in errors]
            raise RuntimeError(
                f"S3 bulk delete partially failed for {len(failed_keys)} key(s): {failed_keys}. "
                "These files remain in the raw bucket and will be reprocessed next run. "
                "Deduplication in the ETL job will prevent duplicate KPIs."
            )
    logger.info(f"Deleted {len(keys)} file(s) from s3://{bucket}/{STREAMS_PREFIX}")


def archive_processed_streams(s3_client, raw_bucket, archive_bucket):
    keys = list_stream_objects(s3_client, raw_bucket)
    if not keys:
        logger.info("No stream files found to archive.")
        return

    # Copy all files first; only delete after all copies succeed.
    # If copy fails the raw files remain intact and the next run reprocesses them safely.
    # If delete fails after copy, files exist in both locations — deduplication handles correctness.
    copy_objects(s3_client, raw_bucket, archive_bucket, keys)
    bulk_delete_objects(s3_client, raw_bucket, keys)

    logger.info(f"Archived {len(keys)} file(s) to s3://{archive_bucket}/{STREAMS_PREFIX}")


if __name__ == "__main__":
    args = getResolvedOptions(
        sys.argv, ["JOB_NAME", "raw_bucket", "archive_bucket", "aws_region"]
    )

    sc          = SparkContext()
    glue_ctx    = GlueContext(sc)
    job         = Job(glue_ctx)
    job.init(args["JOB_NAME"], args)

    s3_client = boto3.client("s3", region_name=args["aws_region"])

    try:
        archive_processed_streams(s3_client, args["raw_bucket"], args["archive_bucket"])
    except Exception as error:
        logger.exception(f"Archive job failed: {error}")
        raise

    logger.info("Archive job complete.")
    job.commit()