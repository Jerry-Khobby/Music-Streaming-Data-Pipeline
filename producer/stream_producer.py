"""
Stream producer — simulates a music app delivering batch files to Kinesis Data Firehose.

Replaces the manual "upload a CSV to S3" step with a realistic, automated source.
It reads the sample stream files in data/streams/ and sends them to a Firehose Direct PUT
delivery stream ONE FILE AT A TIME, waiting between files. Firehose buffers each file's
records and lands them in S3 as a separate batch object, which triggers the existing
pipeline once per file — exactly modelling the brief's "batch files that arrive at
irregular intervals."

So a single run depicts three distinct arrivals:

    send streams1.csv  →  (wait)  →  send streams2.csv  →  (wait)  →  send streams3.csv
         │                                │                                │
         ▼ Firehose flush                 ▼ Firehose flush                 ▼ Firehose flush
    S3 object + pipeline run         S3 object + pipeline run         S3 object + pipeline run

IMPORTANT — the wait between files must exceed Firehose's flush interval (60 s), or two
files land in the same buffer window and merge into one S3 object. The default 90–150 s
jittered wait guarantees clean separation while keeping arrival times irregular.

Usage:
  python stream_producer.py --stream-name music-streaming-streams-ingestion
  python stream_producer.py --stream-name <name> --min-delay 75 --max-delay 120

Requires only boto3 and AWS credentials with firehose:PutRecordBatch on the stream.
"""

import argparse
import csv
import json
import logging
import random
import time
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
)
logger = logging.getLogger("stream_producer")

# Firehose PutRecordBatch accepts at most 500 records or 4 MiB per call.
FIREHOSE_MAX_BATCH_SIZE = 500

# Firehose flushes a buffered file to S3 at most every 60 s (the AWS minimum, set on
# the delivery stream). The wait between files MUST exceed this, or consecutive files
# share a buffer window and merge into one S3 object instead of landing separately.
FIREHOSE_FLUSH_INTERVAL_SECONDS = 60

DEFAULT_DATA_DIR = Path(__file__).resolve().parent.parent / "data" / "streams"


def read_stream_files(data_dir):
    """Read each streams CSV separately, preserving file boundaries.

    Returns a list of (filename, rows) tuples — one entry per file — so the producer
    can send each file as its own batch rather than merging them into one pool.
    """
    csv_files = sorted(data_dir.glob("*.csv"))
    if not csv_files:
        raise FileNotFoundError(f"No stream CSV files found in {data_dir}")

    files = []
    for csv_file in csv_files:
        with csv_file.open(newline="", encoding="utf-8") as handle:
            files.append((csv_file.name, list(csv.DictReader(handle))))

    total_rows = sum(len(rows) for _, rows in files)
    logger.info(f"Loaded {len(files)} file(s), {total_rows} total rows, from {data_dir}")
    return files


def to_firehose_record(row):
    """Turn one CSV row into a newline-delimited JSON Firehose record.

    The trailing newline makes each delivered S3 file newline-delimited JSON (JSONL),
    which the Glue crawler classifies cleanly into named columns — unlike headerless
    CSV, where concatenated rows would lose their column names.
    """
    payload = json.dumps(row) + "\n"
    return {"Data": payload.encode("utf-8")}


def send_batch(firehose_client, stream_name, records):
    """Send up to FIREHOSE_MAX_BATCH_SIZE records, retrying only the ones that fail."""
    response = firehose_client.put_record_batch(
        DeliveryStreamName=stream_name,
        Records=records,
    )

    failed_count = response.get("FailedPutCount", 0)
    if failed_count == 0:
        return

    # Firehose reports per-record success/failure positionally — resend only the failures.
    failed_records = [
        records[index]
        for index, result in enumerate(response["RequestResponses"])
        if "ErrorCode" in result
    ]
    logger.warning(f"{failed_count} record(s) failed — retrying once.")
    retry = firehose_client.put_record_batch(
        DeliveryStreamName=stream_name,
        Records=failed_records,
    )
    if retry.get("FailedPutCount", 0) > 0:
        raise RuntimeError(
            f"{retry['FailedPutCount']} record(s) still failed after retry — "
            "check the Firehose stream status and IAM permissions."
        )


def send_file(firehose_client, stream_name, rows):
    """Send every row of one file to Firehose in batches of at most FIREHOSE_MAX_BATCH_SIZE."""
    for start in range(0, len(rows), FIREHOSE_MAX_BATCH_SIZE):
        chunk = rows[start : start + FIREHOSE_MAX_BATCH_SIZE]
        send_batch(firehose_client, stream_name, [to_firehose_record(row) for row in chunk])


def run_file_by_file(firehose_client, stream_name, files, min_delay, max_delay):
    """Send each file as its own batch, waiting a jittered interval between files."""
    file_count = len(files)
    for index, (filename, rows) in enumerate(files, start=1):
        logger.info(f"Sending file {index}/{file_count}: {filename} ({len(rows)} rows)")
        send_file(firehose_client, stream_name, rows)
        logger.info(f"Sent {filename} — Firehose will flush it to S3 within {FIREHOSE_FLUSH_INTERVAL_SECONDS}s.")

        if index < file_count:
            delay = random.uniform(min_delay, max_delay)
            logger.info(f"Waiting {delay:.0f}s before the next file (jittered — keeps arrivals irregular)")
            time.sleep(delay)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Send stream files to Kinesis Data Firehose, one file at a time, to model irregular batch arrivals."
    )
    parser.add_argument("--stream-name", required=True, help="Firehose delivery stream name")
    parser.add_argument("--region", default="us-east-1", help="AWS region of the stream")
    parser.add_argument(
        "--min-delay",
        type=float,
        default=300,
        help="Minimum seconds to wait between files (must exceed 60 so files land separately)",
    )
    parser.add_argument(
        "--max-delay",
        type=float,
        default=360,
        help="Maximum seconds to wait between files",
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=DEFAULT_DATA_DIR,
        help="Directory holding the source streams CSV files",
    )
    return parser.parse_args()


def validate_delays(min_delay, max_delay):
    if min_delay > max_delay:
        raise ValueError(f"--min-delay ({min_delay}) cannot exceed --max-delay ({max_delay}).")
    if min_delay <= FIREHOSE_FLUSH_INTERVAL_SECONDS:
        logger.warning(
            f"--min-delay ({min_delay}s) is not above Firehose's {FIREHOSE_FLUSH_INTERVAL_SECONDS}s "
            "flush interval — consecutive files may merge into one S3 object instead of landing "
            "separately. Use --min-delay > 60 for clean per-file arrivals."
        )


def main():
    args = parse_args()
    validate_delays(args.min_delay, args.max_delay)

    files = read_stream_files(args.data_dir)
    firehose_client = boto3.client("firehose", region_name=args.region)

    try:
        run_file_by_file(firehose_client, args.stream_name, files, args.min_delay, args.max_delay)
    except ClientError as error:
        logger.error(f"Firehose request failed: {error}")
        raise
    except KeyboardInterrupt:
        logger.info("Interrupted by user — stopping cleanly.")
        return

    logger.info("Producer finished — all files sent.")


if __name__ == "__main__":
    main()


#113