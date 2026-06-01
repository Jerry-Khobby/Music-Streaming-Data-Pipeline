import sys
import json
import logging
from awsglue.utils import getResolvedOptions
import boto3

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

STREAMS_PREFIX  = "streams/"
S3_DELETE_LIMIT = 1000


def copy_objects(s3_client, source_bucket, archive_bucket, keys):
    for key in keys:
        s3_client.copy_object(
            CopySource={"Bucket": source_bucket, "Key": key},
            Bucket=archive_bucket,
            Key=key,
        )
    logger.info(f"Copied {len(keys)} file(s) to s3://{archive_bucket}/{STREAMS_PREFIX}")


def bulk_delete_objects(s3_client, bucket, keys):
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
                f"S3 bulk delete partially failed for {len(failed_keys)} key(s): {failed_keys}."
            )
    logger.info(f"Deleted {len(keys)} file(s) from s3://{bucket}/{STREAMS_PREFIX}")


def archive_processed_streams(s3_client, raw_bucket, archive_bucket, processed_keys):
    if not processed_keys:
        logger.info("No processed keys provided — nothing to archive.")
        return

    logger.info(f"Archiving ONLY these {len(processed_keys)} processed file(s):")
    for key in processed_keys:
        logger.info(f"  → {key}")

    # Copy first, then delete — same safe pattern as before
    # but scoped to ONLY the files this execution actually processed
    copy_objects(s3_client, raw_bucket, archive_bucket, processed_keys)
    bulk_delete_objects(s3_client, raw_bucket, processed_keys)

    logger.info(f"Archived {len(processed_keys)} file(s). "
                f"Any files that arrived AFTER this execution started "
                f"remain in streams/ for the next execution to process.")


if __name__ == "__main__":
    args = getResolvedOptions(
        sys.argv,
        ["JOB_NAME", "raw_bucket", "archive_bucket", "aws_region", "processed_keys"]
    )

    # Step Functions passes the keys as a JSON string — parse it back to a list
    processed_keys = json.loads(args["processed_keys"])

    s3_client = boto3.client("s3", region_name=args["aws_region"])

    try:
        archive_processed_streams(
            s3_client,
            args["raw_bucket"],
            args["archive_bucket"],
            processed_keys,
        )
    except Exception as error:
        logger.exception(f"Archive job failed: {error}")
        raise

    logger.info("Archive job complete.")