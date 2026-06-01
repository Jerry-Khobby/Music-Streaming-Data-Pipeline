"""
Stream producer — simulates a music app emitting play events to Kinesis Data Firehose.

Replaces the manual "upload a CSV to S3" step with a realistic, automated source.
It reads the sample stream rows from data/streams/*.csv and sends each one as a JSON
record to a Firehose Direct PUT delivery stream. Firehose buffers the records and lands
them in S3 as batch files under streams/, which triggers the existing pipeline unchanged.

The brief stresses that data arrives at UNPREDICTABLE intervals, so the producer jitters
the cadence between sends and supports three modes that mimic real traffic shapes:

  steady  — moderate, randomly-spaced sends (the everyday case)
  burst   — many records in a short window (the "lots within 4 minutes" case)
  sparse  — long gaps between small sends (the "data only twice a day" case)

Usage:
  python stream_producer.py --stream-name music-streaming-streams-ingestion --mode steady
  python stream_producer.py --stream-name <name> --mode burst  --cycles 3
  python stream_producer.py --stream-name <name> --mode sparse --cycles 2

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

# Inter-send delay ranges (seconds) per mode — the low/high bounds we draw a
# random delay from. Real production gaps would be larger; these are scaled down
# so a demo run shows the behaviour in minutes rather than hours.
DELAY_RANGES_SECONDS = {
    "steady": (5, 30),
    "burst":  (0, 2),
    "sparse": (60, 180),
}

# How many records each mode sends per send-cycle.
RECORDS_PER_CYCLE = {
    "steady": 50,
    "burst":  400,
    "sparse": 10,
}

DEFAULT_DATA_DIR = Path(__file__).resolve().parent.parent / "data" / "streams"


def read_stream_rows(data_dir):
    """Read every streams CSV in data_dir into a list of dict rows."""
    csv_files = sorted(data_dir.glob("*.csv"))
    if not csv_files:
        raise FileNotFoundError(f"No stream CSV files found in {data_dir}")

    rows = []
    for csv_file in csv_files:
        with csv_file.open(newline="", encoding="utf-8") as handle:
            rows.extend(csv.DictReader(handle))

    logger.info(f"Loaded {len(rows)} source rows from {len(csv_files)} file(s) in {data_dir}")
    return rows


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


def send_records(firehose_client, stream_name, rows):
    """Send all rows to Firehose in batches of at most FIREHOSE_MAX_BATCH_SIZE."""
    for start in range(0, len(rows), FIREHOSE_MAX_BATCH_SIZE):
        chunk = rows[start : start + FIREHOSE_MAX_BATCH_SIZE]
        send_batch(firehose_client, stream_name, [to_firehose_record(row) for row in chunk])
    logger.info(f"Sent {len(rows)} record(s) to Firehose stream '{stream_name}'.")


def pick_cycle_rows(all_rows, count):
    """Pick `count` rows at random (with replacement) to simulate a fresh batch of plays."""
    return [random.choice(all_rows) for _ in range(count)]


def run(firehose_client, stream_name, all_rows, mode, cycles):
    """Run `cycles` send-cycles in the given mode, jittering the delay between cycles."""
    delay_low, delay_high = DELAY_RANGES_SECONDS[mode]
    records_per_cycle = RECORDS_PER_CYCLE[mode]

    for cycle in range(1, cycles + 1):
        cycle_rows = pick_cycle_rows(all_rows, records_per_cycle)
        logger.info(f"[{mode}] cycle {cycle}/{cycles}: sending {len(cycle_rows)} record(s)")
        send_records(firehose_client, stream_name, cycle_rows)

        if cycle < cycles:
            delay = random.uniform(delay_low, delay_high)
            logger.info(f"[{mode}] waiting {delay:.1f}s before next cycle (jittered)")
            time.sleep(delay)


def parse_args():
    parser = argparse.ArgumentParser(description="Send simulated stream events to Kinesis Data Firehose.")
    parser.add_argument("--stream-name", required=True, help="Firehose delivery stream name")
    parser.add_argument("--region", default="us-east-1", help="AWS region of the stream")
    parser.add_argument(
        "--mode",
        choices=list(DELAY_RANGES_SECONDS),
        default="steady",
        help="Traffic shape to simulate (steady | burst | sparse)",
    )
    parser.add_argument("--cycles", type=int, default=5, help="Number of send-cycles to run")
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=DEFAULT_DATA_DIR,
        help="Directory holding the source streams CSV files",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    all_rows = read_stream_rows(args.data_dir)
    firehose_client = boto3.client("firehose", region_name=args.region)

    try:
        run(firehose_client, args.stream_name, all_rows, args.mode, args.cycles)
    except ClientError as error:
        logger.error(f"Firehose request failed: {error}")
        raise
    except KeyboardInterrupt:
        logger.info("Interrupted by user — stopping cleanly.")
        return

    logger.info("Producer finished.")


if __name__ == "__main__":
    main()
