import sys
import logging
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import boto3

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

STREAMS_PREFIX = "streams/"


def listStreamObjects(s3Client, bucket):
    paginator = s3Client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket, Prefix=STREAMS_PREFIX)
    return [
        obj["Key"]
        for page in pages
        for obj in page.get("Contents", [])
        if not obj["Key"].endswith("/")   # skip folder placeholders
    ]


def copyObject(s3Client, sourceBucket, archiveBucket, key):
    copySource = {"Bucket": sourceBucket, "Key": key}
    s3Client.copy_object(CopySource=copySource, Bucket=archiveBucket, Key=key)
    logger.info(f"Copied s3://{sourceBucket}/{key}  →  s3://{archiveBucket}/{key}")


def deleteObject(s3Client, bucket, key):
    s3Client.delete_object(Bucket=bucket, Key=key)
    logger.info(f"Deleted s3://{bucket}/{key}")


def archiveObject(s3Client, rawBucket, archiveBucket, key):
    copyObject(s3Client, rawBucket, archiveBucket, key)
    deleteObject(s3Client, rawBucket, key)


def archiveProcessedStreams(s3Client, rawBucket, archiveBucket):
    keys = listStreamObjects(s3Client, rawBucket)
    if not keys:
        logger.info("No stream files found to archive.")
        return
    for key in keys:
        archiveObject(s3Client, rawBucket, archiveBucket, key)
    logger.info(f"Archived {len(keys)} file(s) to s3://{archiveBucket}/{STREAMS_PREFIX}")


if __name__ == "__main__":
    args = getResolvedOptions(sys.argv, ["JOB_NAME", "raw_bucket", "archive_bucket", "aws_region"])

    sc = SparkContext()
    glueContext = GlueContext(sc)
    job = Job(glueContext)
    job.init(args["JOB_NAME"], args)

    s3Client = boto3.client("s3", region_name=args["aws_region"])

    try:
        archiveProcessedStreams(s3Client, args["raw_bucket"], args["archive_bucket"])
    except Exception as error:
        logger.error(f"Archive job failed: {error}")
        raise

    logger.info("Archive job complete.")
    job.commit()
