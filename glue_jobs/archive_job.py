import sys
import json
from awsglue.utils import getResolvedOptions
import boto3

from monitoring import buildLogger, SlackNotifier, PipelineMonitor
from monitoring.notifier import resolveWebhookUrl

logger = buildLogger(__name__)

STREAMS_PREFIX  = "streams/"
S3_DELETE_LIMIT = 1000


def copy_objects(s3_client, source_bucket, archive_bucket, keys):
    for key in keys:
        s3_client.copy_object(
            CopySource={"Bucket": source_bucket, "Key": key},
            Bucket=archive_bucket,
            Key=key,
        )
    logger.info(f"Copied {len(keys)} file(s) to the archive bucket.")


def bulk_delete_objects(s3_client, bucket, keys):
    for i in range(0, len(keys), S3_DELETE_LIMIT):
        batch    = [{"Key": k} for k in keys[i : i + S3_DELETE_LIMIT]]
        response = s3_client.delete_objects(
            Bucket=bucket,
            Delete={"Objects": batch, "Quiet": False},
        )
        errors = response.get("Errors", [])
        if errors:
            failed_keys = [e["Key"] for e in errors]
            raise RuntimeError(
                f"Failed to delete {len(failed_keys)} file(s) from S3: {failed_keys}"
            )
    logger.info(f"Removed {len(keys)} processed file(s) from the raw bucket.")


def archive_processed_streams(s3_client, raw_bucket, archive_bucket, processed_keys):
    if not processed_keys:
        logger.info("No processed files to archive — skipping.")
        return

    logger.info(f"Archiving {len(processed_keys)} processed file(s):")
    for key in processed_keys:
        logger.info(f"  → {key}")

    # Copy first, then delete — if the copy fails, nothing is lost from the raw bucket.
    copy_objects(s3_client, raw_bucket, archive_bucket, processed_keys)
    bulk_delete_objects(s3_client, raw_bucket, processed_keys)

    logger.info(
        f"{len(processed_keys)} file(s) archived successfully. "
        "Any files that arrived after this run remain in the raw bucket for next time."
    )


if __name__ == "__main__":
    args = getResolvedOptions(
        sys.argv,
        ["JOB_NAME", "raw_bucket", "archive_bucket", "aws_region", "processed_keys"]
    )

    processed_keys = json.loads(args["processed_keys"])
    s3_client      = boto3.client("s3", region_name=args["aws_region"])

    webhookUrl = resolveWebhookUrl(sys.argv)
    notifier   = SlackNotifier(webhookUrl) if webhookUrl else None
    monitor    = PipelineMonitor(args["JOB_NAME"], notifier)

    with monitor.stage("Archiving processed stream files from the raw bucket"):
        archive_processed_streams(
            s3_client,
            args["raw_bucket"],
            args["archive_bucket"],
            processed_keys,
        )

    monitor.logSummary()
