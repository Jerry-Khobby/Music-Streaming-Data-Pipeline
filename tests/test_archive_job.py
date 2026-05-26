"""
Tests for glue_jobs/archive_job.py (Python Shell job)

Covers:
  - list_stream_objects        - pagination, folder placeholder filtering, empty bucket
  - copy_objects               - correct CopySource and destination for each key
  - bulk_delete_objects        - batching, error surfacing
  - archive_processed_streams  - full flow, empty bucket early exit, multiple files
  - STREAMS_PREFIX constant
"""

import sys
import pytest
from unittest.mock import MagicMock

# archive_job is now a Python Shell job — no SparkContext to patch.
sys.argv = [
    "archive_job",
    "--JOB_NAME", "test",
    "--raw_bucket", "raw-bucket",
    "--archive_bucket", "archive-bucket",
    "--aws_region", "us-east-1",
]
from glue_jobs.archive_job import (
    STREAMS_PREFIX,
    list_stream_objects,
    copy_objects,
    bulk_delete_objects,
    archive_processed_streams,
)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

class TestConstants:
    def test_streams_prefix_value(self):
        assert STREAMS_PREFIX == "streams/"


# ---------------------------------------------------------------------------
# list_stream_objects
# ---------------------------------------------------------------------------

class TestListStreamObjects:
    def _make_s3(self, pages):
        s3 = MagicMock()
        paginator = MagicMock()
        paginator.paginate.return_value = pages
        s3.get_paginator.return_value = paginator
        return s3

    def test_returns_keys_from_single_page(self):
        s3 = self._make_s3([{"Contents": [{"Key": "streams/file1.csv"}]}])
        assert list_stream_objects(s3, "raw-bucket") == ["streams/file1.csv"]

    def test_returns_keys_from_multiple_pages(self):
        s3 = self._make_s3([
            {"Contents": [{"Key": "streams/file1.csv"}]},
            {"Contents": [{"Key": "streams/file2.csv"}]},
        ])
        assert set(list_stream_objects(s3, "raw-bucket")) == {
            "streams/file1.csv", "streams/file2.csv",
        }

    def test_empty_bucket_returns_empty_list(self):
        s3 = self._make_s3([{}])
        assert list_stream_objects(s3, "raw-bucket") == []

    def test_folder_placeholder_excluded(self):
        s3 = self._make_s3([{"Contents": [
            {"Key": "streams/"},
            {"Key": "streams/file1.csv"},
        ]}])
        result = list_stream_objects(s3, "raw-bucket")
        assert result == ["streams/file1.csv"]

    def test_paginator_uses_streams_prefix(self):
        s3 = self._make_s3([{}])
        list_stream_objects(s3, "raw-bucket")
        s3.get_paginator.return_value.paginate.assert_called_once_with(
            Bucket="raw-bucket", Prefix=STREAMS_PREFIX
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
        # 2500 keys / 1000 per batch = 3 calls
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
    def _make_s3(self, keys):
        s3 = MagicMock()
        paginator = MagicMock()
        contents = [{"Key": k} for k in keys]
        paginator.paginate.return_value = [{"Contents": contents}] if contents else [{}]
        s3.get_paginator.return_value = paginator
        s3.delete_objects.return_value = {}
        return s3

    def test_archives_all_listed_files(self):
        keys = ["streams/a.csv", "streams/b.csv", "streams/c.csv"]
        s3 = self._make_s3(keys)
        archive_processed_streams(s3, "raw-bucket", "archive-bucket")
        assert s3.copy_object.call_count == 3
        assert s3.delete_objects.call_count == 1  # one batch

    def test_empty_bucket_makes_no_changes(self):
        s3 = self._make_s3([])
        archive_processed_streams(s3, "raw-bucket", "archive-bucket")
        s3.copy_object.assert_not_called()
        s3.delete_objects.assert_not_called()

    def test_copy_happens_before_delete(self):
        s3 = self._make_s3(["streams/a.csv"])
        order = []
        s3.copy_object.side_effect = lambda **kw: order.append("copy")
        s3.delete_objects.side_effect = lambda **kw: order.append("delete") or {}
        archive_processed_streams(s3, "raw-bucket", "archive-bucket")
        assert order == ["copy", "delete"]
