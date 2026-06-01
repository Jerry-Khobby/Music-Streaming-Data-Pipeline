"""
Tests for glue_jobs/archive_job.py (Python Shell job)

Covers:
  - copy_objects               - correct CopySource and destination for each key
  - bulk_delete_objects        - batching, error surfacing
  - archive_processed_streams  - full flow, empty list early exit, multiple files
"""

import sys
import pytest
from unittest.mock import MagicMock

sys.argv = [
    "archive_job",
    "--JOB_NAME", "test",
    "--raw_bucket", "raw-bucket",
    "--archive_bucket", "archive-bucket",
    "--aws_region", "us-east-1",
    "--processed_keys", "[]",
]
from glue_jobs.archive_job import (
    copy_objects,
    bulk_delete_objects,
    archive_processed_streams,
)


# ---------------------------------------------------------------------------
# copy_objects
# ---------------------------------------------------------------------------

class TestCopyObjects:
    def test_copies_every_key(self):
        s3 = MagicMock()
        copy_objects(s3, "raw-bucket", "archive-bucket", ["streams/a.csv", "streams/b.csv"])
        assert s3.copy_object.call_count == 2

    def test_uses_correct_source_and_destination(self):
        s3 = MagicMock()
        copy_objects(s3, "raw-bucket", "archive-bucket", ["streams/a.csv"])
        s3.copy_object.assert_called_once_with(
            CopySource={"Bucket": "raw-bucket", "Key": "streams/a.csv"},
            Bucket="archive-bucket",
            Key="streams/a.csv",
        )

    def test_empty_list_makes_no_calls(self):
        s3 = MagicMock()
        copy_objects(s3, "raw-bucket", "archive-bucket", [])
        s3.copy_object.assert_not_called()


# ---------------------------------------------------------------------------
# bulk_delete_objects
# ---------------------------------------------------------------------------

class TestBulkDeleteObjects:
    def test_single_batch_for_small_list(self):
        s3 = MagicMock()
        s3.delete_objects.return_value = {}
        bulk_delete_objects(s3, "raw-bucket", ["streams/a.csv", "streams/b.csv"])
        assert s3.delete_objects.call_count == 1

    def test_splits_into_1000_key_batches(self):
        s3 = MagicMock()
        s3.delete_objects.return_value = {}
        keys = [f"streams/file{i}.csv" for i in range(2500)]
        bulk_delete_objects(s3, "raw-bucket", keys)
        assert s3.delete_objects.call_count == 3

    def test_raises_when_s3_reports_errors(self):
        s3 = MagicMock()
        s3.delete_objects.return_value = {
            "Errors": [{"Key": "streams/a.csv", "Code": "AccessDenied"}],
        }
        with pytest.raises(RuntimeError, match="bulk delete partially failed"):
            bulk_delete_objects(s3, "raw-bucket", ["streams/a.csv"])

    def test_correct_bucket_targeted(self):
        s3 = MagicMock()
        s3.delete_objects.return_value = {}
        bulk_delete_objects(s3, "raw-bucket", ["streams/a.csv"])
        _, kwargs = s3.delete_objects.call_args
        assert kwargs["Bucket"] == "raw-bucket"


# ---------------------------------------------------------------------------
# archive_processed_streams
# ---------------------------------------------------------------------------

class TestArchiveProcessedStreams:
    def _make_s3(self):
        s3 = MagicMock()
        s3.delete_objects.return_value = {}
        return s3

    def test_archives_only_given_keys(self):
        keys = ["streams/a.csv", "streams/b.csv", "streams/c.csv"]
        s3 = self._make_s3()
        archive_processed_streams(s3, "raw-bucket", "archive-bucket", keys)
        assert s3.copy_object.call_count == 3
        assert s3.delete_objects.call_count == 1

    def test_empty_key_list_makes_no_changes(self):
        s3 = self._make_s3()
        archive_processed_streams(s3, "raw-bucket", "archive-bucket", [])
        s3.copy_object.assert_not_called()
        s3.delete_objects.assert_not_called()

    def test_copy_happens_before_delete(self):
        s3 = self._make_s3()
        order = []
        s3.copy_object.side_effect = lambda **kw: order.append("copy")
        s3.delete_objects.side_effect = lambda **kw: order.append("delete") or {}
        archive_processed_streams(s3, "raw-bucket", "archive-bucket", ["streams/a.csv"])
        assert order == ["copy", "delete"]

    def test_does_not_touch_keys_outside_snapshot(self):
        """Verifies archive_processed_streams only deletes what it was given,
        not everything in the bucket."""
        s3 = self._make_s3()
        snapshot_keys = ["streams/a.csv"]
        archive_processed_streams(s3, "raw-bucket", "archive-bucket", snapshot_keys)
        deleted = s3.delete_objects.call_args[1]["Delete"]["Objects"]
        assert deleted == [{"Key": "streams/a.csv"}]
