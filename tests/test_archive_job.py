"""
Tests for glue_jobs/archive_job.py

Covers:
  - listStreamObjects         – pagination, folder placeholder filtering, empty bucket
  - copyObject                – correct CopySource and destination
  - deleteObject              – correct bucket and key
  - archiveObject             – copy then delete ordering
  - archiveProcessedStreams   – full flow, empty bucket early exit, multiple files
  - STREAMS_PREFIX constant
"""

import sys
import pytest
from unittest.mock import MagicMock, patch, call

with patch("pyspark.context.SparkContext"):
    sys.argv = [
        "archive_job",
        "--JOB_NAME", "test",
        "--raw_bucket", "raw-bucket",
        "--archive_bucket", "archive-bucket",
        "--aws_region", "us-east-1",
    ]
    from glue_jobs.archive_job import (
        STREAMS_PREFIX,
        listStreamObjects,
        copyObject,
        deleteObject,
        archiveObject,
        archiveProcessedStreams,
    )


# ---------------------------------------------------------------------------
# Constant
# ---------------------------------------------------------------------------

class TestConstants:
    def test_streams_prefix_value(self):
        assert STREAMS_PREFIX == "streams/"


# ---------------------------------------------------------------------------
# listStreamObjects
# ---------------------------------------------------------------------------

class TestListStreamObjects:
    def _make_paginator(self, pages):
        paginator = MagicMock()
        paginator.paginate.return_value = pages
        return paginator

    def _make_s3(self, pages):
        s3 = MagicMock()
        s3.get_paginator.return_value = self._make_paginator(pages)
        return s3

    def test_returns_keys_from_single_page(self):
        pages = [{"Contents": [{"Key": "streams/file1.json"}]}]
        s3 = self._make_s3(pages)
        result = listStreamObjects(s3, "raw-bucket")
        assert result == ["streams/file1.json"]

    def test_returns_keys_from_multiple_pages(self):
        pages = [
            {"Contents": [{"Key": "streams/file1.json"}]},
            {"Contents": [{"Key": "streams/file2.json"}]},
        ]
        s3 = self._make_s3(pages)
        result = listStreamObjects(s3, "raw-bucket")
        assert set(result) == {"streams/file1.json", "streams/file2.json"}

    def test_empty_bucket_returns_empty_list(self):
        pages = [{}]  # no "Contents" key
        s3 = self._make_s3(pages)
        result = listStreamObjects(s3, "raw-bucket")
        assert result == []

    def test_folder_placeholder_excluded(self):
        pages = [{"Contents": [
            {"Key": "streams/"},            # folder placeholder
            {"Key": "streams/file1.json"},  # real file
        ]}]
        s3 = self._make_s3(pages)
        result = listStreamObjects(s3, "raw-bucket")
        assert "streams/" not in result
        assert "streams/file1.json" in result

    def test_paginator_uses_streams_prefix(self):
        pages = [{}]
        s3 = self._make_s3(pages)
        listStreamObjects(s3, "raw-bucket")
        s3.get_paginator.return_value.paginate.assert_called_once_with(
            Bucket="raw-bucket", Prefix=STREAMS_PREFIX
        )

    def test_multiple_files_all_returned(self):
        files = [{"Key": f"streams/file{i}.json"} for i in range(10)]
        pages = [{"Contents": files}]
        s3 = self._make_s3(pages)
        result = listStreamObjects(s3, "raw-bucket")
        assert len(result) == 10

    def test_returns_list_not_generator(self):
        pages = [{"Contents": [{"Key": "streams/file1.json"}]}]
        s3 = self._make_s3(pages)
        result = listStreamObjects(s3, "raw-bucket")
        assert isinstance(result, list)


# ---------------------------------------------------------------------------
# copyObject
# ---------------------------------------------------------------------------

class TestCopyObject:
    def test_calls_copy_object_with_correct_source(self):
        s3 = MagicMock()
        copyObject(s3, "raw-bucket", "archive-bucket", "streams/file1.json")
        expected_source = {"Bucket": "raw-bucket", "Key": "streams/file1.json"}
        s3.copy_object.assert_called_once_with(
            CopySource=expected_source,
            Bucket="archive-bucket",
            Key="streams/file1.json",
        )

    def test_destination_bucket_is_archive(self):
        s3 = MagicMock()
        copyObject(s3, "raw-bucket", "archive-bucket", "streams/file1.json")
        _, kwargs = s3.copy_object.call_args
        assert kwargs["Bucket"] == "archive-bucket"

    def test_key_preserved_in_destination(self):
        s3 = MagicMock()
        key = "streams/2024/01/file.json"
        copyObject(s3, "raw-bucket", "archive-bucket", key)
        _, kwargs = s3.copy_object.call_args
        assert kwargs["Key"] == key


# ---------------------------------------------------------------------------
# deleteObject
# ---------------------------------------------------------------------------

class TestDeleteObject:
    def test_calls_delete_object_with_correct_bucket(self):
        s3 = MagicMock()
        deleteObject(s3, "raw-bucket", "streams/file1.json")
        s3.delete_object.assert_called_once_with(
            Bucket="raw-bucket", Key="streams/file1.json"
        )

    def test_deletes_from_raw_not_archive(self):
        s3 = MagicMock()
        deleteObject(s3, "raw-bucket", "streams/file1.json")
        _, kwargs = s3.delete_object.call_args
        assert kwargs["Bucket"] == "raw-bucket"

    def test_key_passed_correctly(self):
        s3 = MagicMock()
        key = "streams/nested/path/file.json"
        deleteObject(s3, "raw-bucket", key)
        _, kwargs = s3.delete_object.call_args
        assert kwargs["Key"] == key


# ---------------------------------------------------------------------------
# archiveObject
# ---------------------------------------------------------------------------

class TestArchiveObject:
    def test_copy_called_before_delete(self):
        s3 = MagicMock()
        call_order = []
        s3.copy_object.side_effect = lambda **kwargs: call_order.append("copy")
        s3.delete_object.side_effect = lambda **kwargs: call_order.append("delete")

        archiveObject(s3, "raw-bucket", "archive-bucket", "streams/file1.json")

        assert call_order == ["copy", "delete"]

    def test_both_copy_and_delete_called(self):
        s3 = MagicMock()
        archiveObject(s3, "raw-bucket", "archive-bucket", "streams/file1.json")
        s3.copy_object.assert_called_once()
        s3.delete_object.assert_called_once()

    def test_same_key_used_for_copy_and_delete(self):
        s3 = MagicMock()
        key = "streams/myfile.json"
        archiveObject(s3, "raw-bucket", "archive-bucket", key)

        copy_kwargs = s3.copy_object.call_args[1]
        delete_kwargs = s3.delete_object.call_args[1]

        assert copy_kwargs["CopySource"]["Key"] == key
        assert delete_kwargs["Key"] == key


# ---------------------------------------------------------------------------
# archiveProcessedStreams
# ---------------------------------------------------------------------------

class TestArchiveProcessedStreams:
    def _make_s3_with_files(self, keys):
        s3 = MagicMock()
        paginator = MagicMock()
        contents = [{"Key": k} for k in keys]
        paginator.paginate.return_value = [{"Contents": contents}] if contents else [{}]
        s3.get_paginator.return_value = paginator
        return s3

    def test_archives_all_listed_files(self):
        keys = ["streams/a.json", "streams/b.json", "streams/c.json"]
        s3 = self._make_s3_with_files(keys)
        archiveProcessedStreams(s3, "raw-bucket", "archive-bucket")
        assert s3.copy_object.call_count == 3
        assert s3.delete_object.call_count == 3

    def test_empty_bucket_skips_copy_and_delete(self):
        s3 = self._make_s3_with_files([])
        archiveProcessedStreams(s3, "raw-bucket", "archive-bucket")
        s3.copy_object.assert_not_called()
        s3.delete_object.assert_not_called()

    def test_single_file_copied_and_deleted(self):
        s3 = self._make_s3_with_files(["streams/single.json"])
        archiveProcessedStreams(s3, "raw-bucket", "archive-bucket")
        s3.copy_object.assert_called_once()
        s3.delete_object.assert_called_once()

    def test_copy_uses_correct_source_bucket(self):
        s3 = self._make_s3_with_files(["streams/file.json"])
        archiveProcessedStreams(s3, "my-raw-bucket", "my-archive-bucket")
        _, kwargs = s3.copy_object.call_args
        assert kwargs["CopySource"]["Bucket"] == "my-raw-bucket"

    def test_copy_destination_is_archive_bucket(self):
        s3 = self._make_s3_with_files(["streams/file.json"])
        archiveProcessedStreams(s3, "raw-bucket", "archive-bucket")
        _, kwargs = s3.copy_object.call_args
        assert kwargs["Bucket"] == "archive-bucket"

    def test_delete_targets_raw_bucket(self):
        s3 = self._make_s3_with_files(["streams/file.json"])
        archiveProcessedStreams(s3, "raw-bucket", "archive-bucket")
        _, kwargs = s3.delete_object.call_args
        assert kwargs["Bucket"] == "raw-bucket"

    def test_all_keys_are_individually_archived(self):
        keys = [f"streams/file{i}.json" for i in range(5)]
        s3 = self._make_s3_with_files(keys)
        archiveProcessedStreams(s3, "raw-bucket", "archive-bucket")

        deleted_keys = {call[1]["Key"] for call in s3.delete_object.call_args_list}
        assert deleted_keys == set(keys)
