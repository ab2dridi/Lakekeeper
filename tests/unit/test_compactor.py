"""Tests for lakekeeper.core.compactor module."""

from __future__ import annotations

from datetime import datetime
from unittest.mock import MagicMock

import pytest

from lakekeeper.core.compactor import Compactor
from lakekeeper.models import BackupInfo, CompactionStatus
from lakekeeper.utils.hdfs import HdfsFileInfo


class TestCompactor:
    @pytest.fixture
    def compactor(self, mock_spark, mock_hdfs_client, config, mock_backup_mgr):
        return Compactor(mock_spark, mock_hdfs_client, config, mock_backup_mgr)

    @pytest.fixture
    def backup_info(self, sample_table_info):
        return BackupInfo(
            original_table="mydb.events",
            backup_table="mydb.__bkp_events_20240101_120000",
            original_location="hdfs:///data/mydb/events",
            timestamp=datetime(2024, 1, 1, 12, 0, 0),
        )

    def test_compact_non_partitioned_success(
        self,
        compactor,
        mock_spark,
        mock_hdfs_client,
        sample_table_info,
        backup_info,
    ):
        mock_df = MagicMock()
        mock_df.count.return_value = 1000
        mock_df.coalesce.return_value = mock_df
        mock_df.write = MagicMock()
        mock_df.write.format.return_value = mock_df.write
        mock_df.write.mode.return_value = mock_df.write

        mock_spark.read.format.return_value.load.return_value = mock_df

        mock_hdfs_client.get_file_info.return_value = HdfsFileInfo(
            file_count=24,
            total_size_bytes=3 * 1024 * 1024 * 1024,
        )

        report = compactor.compact_table(sample_table_info, backup_info)

        assert report.status == CompactionStatus.COMPLETED
        assert report.before_file_count == 65000
        assert report.after_file_count == 24
        assert report.row_count_before == 1000
        assert report.row_count_after == 1000
        assert report.duration_seconds > 0

        # Two renames: original→old, then temp→original
        assert mock_hdfs_client.rename_path.call_count == 2
        rename_calls = mock_hdfs_client.rename_path.call_args_list
        assert "__old_" in str(rename_calls[0])
        assert "__compact_tmp_" in str(rename_calls[1])

        # No ALTER TABLE on the main table (location is never changed)
        sql_calls = [str(c) for c in mock_spark.sql.call_args_list]
        assert not any("ALTER TABLE mydb.events SET LOCATION" in c for c in sql_calls)

    def test_compact_non_partitioned_row_mismatch(
        self,
        compactor,
        mock_spark,
        mock_hdfs_client,
        sample_table_info,
        backup_info,
    ):
        call_count = [0]

        def count_side_effect():
            call_count[0] += 1
            return 1000 if call_count[0] <= 1 else 999

        mock_df = MagicMock()
        mock_df.count.side_effect = count_side_effect
        mock_df.coalesce.return_value = mock_df
        mock_df.write = MagicMock()
        mock_df.write.format.return_value = mock_df.write
        mock_df.write.mode.return_value = mock_df.write

        mock_spark.read.format.return_value.load.return_value = mock_df

        report = compactor.compact_table(sample_table_info, backup_info)

        assert report.status == CompactionStatus.FAILED
        assert "Row count mismatch" in report.error

        # Temp dir deleted on mismatch (before any rename)
        mock_hdfs_client.delete_path.assert_called_once()
        # No rename happened (mismatch detected before swap)
        mock_hdfs_client.rename_path.assert_not_called()

    def test_compact_non_partitioned_path_collision(
        self,
        compactor,
        mock_spark,
        mock_hdfs_client,
        sample_table_info,
        backup_info,
    ):
        # Simulate a leftover staging dir from a previous failed run
        mock_hdfs_client.path_exists.return_value = True

        report = compactor.compact_table(sample_table_info, backup_info)

        assert report.status == CompactionStatus.FAILED
        assert "already exists" in report.error
        # Nothing was written or renamed
        mock_hdfs_client.rename_path.assert_not_called()

    def test_compact_partitioned_success(
        self, compactor, mock_spark, mock_hdfs_client, sample_partitioned_table_info, mock_backup_mgr
    ):
        backup_info = BackupInfo(
            original_table="mydb.logs",
            backup_table="mydb.__bkp_logs_20240101_120000",
            original_location="hdfs:///data/mydb/logs",
            timestamp=datetime(2024, 1, 1, 12, 0, 0),
            partition_locations={
                "year=2024/month=01": "hdfs:///data/mydb/logs/year=2024/month=01",
            },
        )

        mock_df = MagicMock()
        mock_df.count.return_value = 500
        mock_df.coalesce.return_value = mock_df
        mock_df.write = MagicMock()
        mock_df.write.format.return_value = mock_df.write
        mock_df.write.mode.return_value = mock_df.write

        mock_spark.read.format.return_value.load.return_value = mock_df

        mock_hdfs_client.get_file_info.return_value = HdfsFileInfo(
            file_count=8,
            total_size_bytes=1 * 1024 * 1024 * 1024,
        )

        report = compactor.compact_table(sample_partitioned_table_info, backup_info)

        assert report.status == CompactionStatus.COMPLETED
        assert report.partitions_compacted == 1
        assert report.partitions_skipped == 1

        # Two renames for the one compacted partition
        assert mock_hdfs_client.rename_path.call_count == 2
        # Backup partition location updated
        mock_backup_mgr.update_partition_location.assert_called_once()

    def test_compact_exception_triggers_rollback(
        self,
        compactor,
        mock_spark,
        mock_hdfs_client,
        sample_table_info,
        backup_info,
    ):
        mock_spark.read.format.return_value.load.side_effect = RuntimeError("Spark error")

        report = compactor.compact_table(sample_table_info, backup_info)

        assert report.status == CompactionStatus.FAILED
        assert "Spark error" in report.error

        # Exception before any write/rename - rollback does nothing
        mock_hdfs_client.rename_path.assert_not_called()
        mock_hdfs_client.delete_path.assert_not_called()

    def test_compact_backup_location_update_warning(
        self,
        compactor,
        mock_spark,
        mock_hdfs_client,
        sample_table_info,
        backup_info,
        mock_backup_mgr,
    ):
        """update_table_location failure is a warning; compaction still completes."""
        mock_df = MagicMock()
        mock_df.count.return_value = 1000
        mock_df.coalesce.return_value = mock_df
        mock_df.write = MagicMock()
        mock_df.write.format.return_value = mock_df.write
        mock_df.write.mode.return_value = mock_df.write
        mock_spark.read.format.return_value.load.return_value = mock_df

        mock_hdfs_client.get_file_info.return_value = HdfsFileInfo(
            file_count=24,
            total_size_bytes=3 * 1024 * 1024 * 1024,
        )
        mock_backup_mgr.update_table_location.side_effect = RuntimeError("Metastore error")

        report = compactor.compact_table(sample_table_info, backup_info)

        assert report.status == CompactionStatus.COMPLETED
        # Both renames still happened
        assert mock_hdfs_client.rename_path.call_count == 2

    def test_compact_partitioned_get_file_info_fallback(
        self, compactor, mock_spark, mock_hdfs_client, sample_partitioned_table_info, mock_backup_mgr
    ):
        """When get_file_info raises after a partition swap, target_files is used as fallback."""
        backup_info = BackupInfo(
            original_table="mydb.logs",
            backup_table="mydb.__bkp_logs_20240101_120000",
            original_location="hdfs:///data/mydb/logs",
            timestamp=datetime(2024, 1, 1, 12, 0, 0),
            partition_locations={
                "year=2024/month=01": "hdfs:///data/mydb/logs/year=2024/month=01",
            },
        )

        mock_df = MagicMock()
        mock_df.count.return_value = 500
        mock_df.coalesce.return_value = mock_df
        mock_df.write = MagicMock()
        mock_df.write.format.return_value = mock_df.write
        mock_df.write.mode.return_value = mock_df.write
        mock_spark.read.format.return_value.load.return_value = mock_df

        mock_hdfs_client.get_file_info.side_effect = RuntimeError("HDFS unavailable")

        report = compactor.compact_table(sample_partitioned_table_info, backup_info)

        assert report.status == CompactionStatus.COMPLETED
        assert report.partitions_compacted == 1
        # Fallback: target_files (8) used instead of actual file count
        assert report.after_file_count > 0

    def test_compact_partition_row_mismatch(
        self, compactor, mock_spark, mock_hdfs_client, sample_partitioned_table_info, mock_backup_mgr
    ):
        """Row count mismatch in a partition aborts and rolls back."""
        backup_info = BackupInfo(
            original_table="mydb.logs",
            backup_table="mydb.__bkp_logs_20240101_120000",
            original_location="hdfs:///data/mydb/logs",
            timestamp=datetime(2024, 1, 1, 12, 0, 0),
            partition_locations={
                "year=2024/month=01": "hdfs:///data/mydb/logs/year=2024/month=01",
            },
        )

        call_count = [0]

        def count_side_effect():
            call_count[0] += 1
            return 500 if call_count[0] <= 1 else 499

        mock_df = MagicMock()
        mock_df.count.side_effect = count_side_effect
        mock_df.coalesce.return_value = mock_df
        mock_df.write = MagicMock()
        mock_df.write.format.return_value = mock_df.write
        mock_df.write.mode.return_value = mock_df.write
        mock_spark.read.format.return_value.load.return_value = mock_df

        report = compactor.compact_table(sample_partitioned_table_info, backup_info)

        assert report.status == CompactionStatus.FAILED
        assert "Row count mismatch" in report.error
        mock_hdfs_client.delete_path.assert_called_once()
        mock_hdfs_client.rename_path.assert_not_called()

    def test_compact_partition_backup_location_update_warning(
        self, compactor, mock_spark, mock_hdfs_client, sample_partitioned_table_info, mock_backup_mgr
    ):
        """Partition backup location update failure is a warning; compaction still completes."""
        backup_info = BackupInfo(
            original_table="mydb.logs",
            backup_table="mydb.__bkp_logs_20240101_120000",
            original_location="hdfs:///data/mydb/logs",
            timestamp=datetime(2024, 1, 1, 12, 0, 0),
            partition_locations={
                "year=2024/month=01": "hdfs:///data/mydb/logs/year=2024/month=01",
            },
        )

        mock_df = MagicMock()
        mock_df.count.return_value = 500
        mock_df.coalesce.return_value = mock_df
        mock_df.write = MagicMock()
        mock_df.write.format.return_value = mock_df.write
        mock_df.write.mode.return_value = mock_df.write
        mock_spark.read.format.return_value.load.return_value = mock_df

        mock_hdfs_client.get_file_info.return_value = HdfsFileInfo(
            file_count=8,
            total_size_bytes=1 * 1024 * 1024 * 1024,
        )
        mock_backup_mgr.update_partition_location.side_effect = RuntimeError("Metastore error")

        report = compactor.compact_table(sample_partitioned_table_info, backup_info)

        assert report.status == CompactionStatus.COMPLETED
        assert report.partitions_compacted == 1
        assert mock_hdfs_client.rename_path.call_count == 2

    def test_rollback_failure_is_logged(
        self,
        compactor,
        mock_spark,
        mock_hdfs_client,
        sample_table_info,
        backup_info,
    ):
        """If rollback itself fails, the error is logged but the report still shows FAILED."""
        mock_df = MagicMock()
        mock_df.count.return_value = 1000
        mock_df.coalesce.return_value = mock_df
        mock_df.write = MagicMock()
        mock_df.write.format.return_value = mock_df.write
        mock_df.write.mode.return_value = mock_df.write
        mock_spark.read.format.return_value.load.return_value = mock_df

        # _check_paths_available passes, then first rename succeeds,
        # second rename fails (triggers rollback), rollback rename also fails.
        mock_hdfs_client.path_exists.side_effect = [
            False,
            False,  # _check_paths_available
            False,  # rollback: temp_path absent (skip delete)
            True,  # rollback: old_path exists → try rename
            False,  # rollback: original path absent (skip pre-delete)
        ]
        mock_hdfs_client.rename_path.side_effect = [
            True,  # original → old (success)
            RuntimeError("second rename failed"),  # temp → original (triggers rollback)
            RuntimeError("rollback rename failed"),  # rollback rename also fails
        ]

        report = compactor.compact_table(sample_table_info, backup_info)

        assert report.status == CompactionStatus.FAILED
        assert "second rename failed" in report.error

    def test_rollback_deletes_temp_and_restores_original(
        self,
        compactor,
        mock_spark,
        mock_hdfs_client,
        sample_table_info,
        backup_info,
    ):
        """Rollback: temp dir deleted, __old_ renamed back to original."""
        mock_df = MagicMock()
        mock_df.count.return_value = 1000
        mock_df.coalesce.return_value = mock_df
        mock_df.write = MagicMock()
        mock_df.write.format.return_value = mock_df.write
        mock_df.write.mode.return_value = mock_df.write
        mock_spark.read.format.return_value.load.return_value = mock_df

        # _check_paths_available: free
        # rollback: temp exists → delete it; old exists, original absent → rename back
        mock_hdfs_client.path_exists.side_effect = [
            False,
            False,  # _check_paths_available
            True,  # rollback: temp_path exists → delete
            True,  # rollback: old_path exists → enter rename block
            False,  # rollback: original_path absent → skip pre-delete
        ]
        mock_hdfs_client.rename_path.side_effect = [
            True,  # original → old (success)
            RuntimeError("second rename failed"),  # temp → original (triggers rollback)
            True,  # rollback: old → original (succeeds)
        ]

        report = compactor.compact_table(sample_table_info, backup_info)

        assert report.status == CompactionStatus.FAILED
        # delete called for temp path during rollback
        mock_hdfs_client.delete_path.assert_called_once()
        # rename called: original→old, temp→original (fails), old→original (rollback)
        assert mock_hdfs_client.rename_path.call_count == 3

    def test_rollback_deletes_partial_data_at_original_path(
        self,
        compactor,
        mock_spark,
        mock_hdfs_client,
        sample_table_info,
        backup_info,
    ):
        """Rollback: if original path exists (partial write), it's deleted before rename."""
        mock_df = MagicMock()
        mock_df.count.return_value = 1000
        mock_df.coalesce.return_value = mock_df
        mock_df.write = MagicMock()
        mock_df.write.format.return_value = mock_df.write
        mock_df.write.mode.return_value = mock_df.write
        mock_spark.read.format.return_value.load.return_value = mock_df

        mock_hdfs_client.path_exists.side_effect = [
            False,
            False,  # _check_paths_available
            False,  # rollback: temp absent (skip delete)
            True,  # rollback: old_path exists
            True,  # rollback: original_path also exists (partial data) → delete it
        ]
        mock_hdfs_client.rename_path.side_effect = [
            True,  # original → old
            RuntimeError("second rename failed"),  # triggers rollback
            True,  # rollback rename succeeds
        ]

        report = compactor.compact_table(sample_table_info, backup_info)

        assert report.status == CompactionStatus.FAILED
        # delete_path called for the partial original path before rollback rename
        mock_hdfs_client.delete_path.assert_called_once()
        assert mock_hdfs_client.rename_path.call_count == 3

    def test_compact_rename_failure_triggers_rollback(
        self,
        compactor,
        mock_spark,
        mock_hdfs_client,
        sample_table_info,
        backup_info,
    ):
        mock_df = MagicMock()
        mock_df.count.return_value = 1000
        mock_df.coalesce.return_value = mock_df
        mock_df.write = MagicMock()
        mock_df.write.format.return_value = mock_df.write
        mock_df.write.mode.return_value = mock_df.write
        mock_spark.read.format.return_value.load.return_value = mock_df

        # Rename step raises (HDFS returned False)
        mock_hdfs_client.rename_path.side_effect = RuntimeError("HDFS rename failed")

        report = compactor.compact_table(sample_table_info, backup_info)

        assert report.status == CompactionStatus.FAILED
        assert "HDFS rename failed" in report.error
        # rename was attempted exactly once (the original→old step that failed)
        assert mock_hdfs_client.rename_path.call_count == 1

    def test_compact_preserves_compression_codec(
        self,
        compactor,
        mock_spark,
        mock_hdfs_client,
        sample_table_info,
        backup_info,
    ):
        """When compression_codec is set on the table, .option('compression', ...) is passed to the writer."""
        sample_table_info.compression_codec = "gzip"

        mock_df = MagicMock()
        mock_df.count.return_value = 500
        mock_df.coalesce.return_value = mock_df
        mock_df.write = MagicMock()
        mock_df.write.format.return_value = mock_df.write
        mock_df.write.mode.return_value = mock_df.write
        mock_df.write.option.return_value = mock_df.write
        mock_spark.read.format.return_value.load.return_value = mock_df
        mock_hdfs_client.get_file_info.return_value = HdfsFileInfo(file_count=1, total_size_bytes=128 * 1024 * 1024)

        report = compactor.compact_table(sample_table_info, backup_info)

        assert report.status == CompactionStatus.COMPLETED
        mock_df.write.option.assert_called_with("compression", "gzip")

    def test_compact_no_compression_option_when_codec_absent(
        self,
        compactor,
        mock_spark,
        mock_hdfs_client,
        sample_table_info,
        backup_info,
    ):
        """When compression_codec is None, .option('compression') is NOT called (Spark uses its default)."""
        assert sample_table_info.compression_codec is None  # fixture default

        mock_df = MagicMock()
        mock_df.count.return_value = 500
        mock_df.coalesce.return_value = mock_df
        mock_df.write = MagicMock()
        mock_df.write.format.return_value = mock_df.write
        mock_df.write.mode.return_value = mock_df.write
        mock_spark.read.format.return_value.load.return_value = mock_df
        mock_hdfs_client.get_file_info.return_value = HdfsFileInfo(file_count=1, total_size_bytes=128 * 1024 * 1024)

        compactor.compact_table(sample_table_info, backup_info)

        mock_df.write.option.assert_not_called()

    def test_compact_sorts_before_coalesce(
        self,
        compactor,
        mock_spark,
        mock_hdfs_client,
        sample_table_info,
        backup_info,
    ):
        """When sort_columns is set, df.sort(*cols) is called before coalesce."""
        sample_table_info.sort_columns = ["date", "user_id"]

        mock_df = MagicMock()
        mock_df.count.return_value = 500
        mock_df.sort.return_value = mock_df  # sort() returns same df
        mock_df.coalesce.return_value = mock_df
        mock_df.write = MagicMock()
        mock_df.write.format.return_value = mock_df.write
        mock_df.write.mode.return_value = mock_df.write
        mock_spark.read.format.return_value.load.return_value = mock_df
        mock_hdfs_client.get_file_info.return_value = HdfsFileInfo(file_count=1, total_size_bytes=128 * 1024 * 1024)

        report = compactor.compact_table(sample_table_info, backup_info)

        assert report.status == CompactionStatus.COMPLETED
        mock_df.sort.assert_called_once_with("date", "user_id")

    def test_compact_no_sort_when_sort_columns_empty(
        self,
        compactor,
        mock_spark,
        mock_hdfs_client,
        sample_table_info,
        backup_info,
    ):
        """When sort_columns is empty, df.sort() is never called."""
        assert sample_table_info.sort_columns == []  # fixture default

        mock_df = MagicMock()
        mock_df.count.return_value = 500
        mock_df.coalesce.return_value = mock_df
        mock_df.write = MagicMock()
        mock_df.write.format.return_value = mock_df.write
        mock_df.write.mode.return_value = mock_df.write
        mock_spark.read.format.return_value.load.return_value = mock_df
        mock_hdfs_client.get_file_info.return_value = HdfsFileInfo(file_count=1, total_size_bytes=128 * 1024 * 1024)

        compactor.compact_table(sample_table_info, backup_info)

        mock_df.sort.assert_not_called()

    def test_compact_partitioned_all_partitions_skipped(
        self,
        compactor,
        mock_spark,
        mock_hdfs_client,
        mock_backup_mgr,
    ):
        """When all partitions have needs_compaction=False, result is SKIPPED with no division by zero."""
        from lakekeeper.models import FileFormat, PartitionInfo, TableInfo

        table_info = TableInfo(
            database="mydb",
            table_name="logs",
            location="hdfs:///data/mydb/logs",
            file_format=FileFormat.ORC,
            is_partitioned=True,
            partition_columns=["year", "month"],
            partitions=[
                PartitionInfo(
                    spec={"year": "2024", "month": "01"},
                    location="hdfs:///data/mydb/logs/year=2024/month=01",
                    file_count=3,
                    total_size_bytes=500 * 1024 * 1024,
                    needs_compaction=False,
                    target_files=4,
                ),
                PartitionInfo(
                    spec={"year": "2024", "month": "02"},
                    location="hdfs:///data/mydb/logs/year=2024/month=02",
                    file_count=2,
                    total_size_bytes=400 * 1024 * 1024,
                    needs_compaction=False,
                    target_files=3,
                ),
            ],
            total_file_count=5,
            total_size_bytes=900 * 1024 * 1024,
            needs_compaction=False,
        )
        backup_info = BackupInfo(
            original_table="mydb.logs",
            backup_table="mydb.__bkp_logs_20240101_120000",
            original_location="hdfs:///data/mydb/logs",
            timestamp=datetime(2024, 1, 1, 12, 0, 0),
            partition_locations={},
        )

        report = compactor.compact_table(table_info, backup_info)

        # All partitions skipped → COMPLETED with zero compacted partitions and no error
        assert report.status == CompactionStatus.COMPLETED
        assert report.partitions_compacted == 0
        assert report.partitions_skipped == 2
        # Skipped partitions still count toward after_file_count (3 + 2 = 5)
        assert report.after_file_count == 5
        # No rename or write should have occurred
        mock_hdfs_client.rename_path.assert_not_called()
        mock_spark.read.format.return_value.load.assert_not_called()

    def test_compact_partitioned_with_sort_columns(
        self,
        compactor,
        mock_spark,
        mock_hdfs_client,
        sample_partitioned_table_info,
        mock_backup_mgr,
    ):
        """sort_columns on a partitioned table triggers df.sort() for each compacted partition."""
        sample_partitioned_table_info.sort_columns = ["year", "month"]

        backup_info = BackupInfo(
            original_table="mydb.logs",
            backup_table="mydb.__bkp_logs_20240101_120000",
            original_location="hdfs:///data/mydb/logs",
            timestamp=datetime(2024, 1, 1, 12, 0, 0),
            partition_locations={
                "year=2024/month=01": "hdfs:///data/mydb/logs/year=2024/month=01",
            },
        )

        mock_df = MagicMock()
        mock_df.count.return_value = 500
        mock_df.sort.return_value = mock_df
        mock_df.coalesce.return_value = mock_df
        mock_df.write = MagicMock()
        mock_df.write.format.return_value = mock_df.write
        mock_df.write.mode.return_value = mock_df.write
        mock_spark.read.format.return_value.load.return_value = mock_df
        mock_hdfs_client.get_file_info.return_value = HdfsFileInfo(
            file_count=8, total_size_bytes=1 * 1024 * 1024 * 1024
        )

        report = compactor.compact_table(sample_partitioned_table_info, backup_info)

        assert report.status == CompactionStatus.COMPLETED
        assert report.partitions_compacted == 1
        mock_df.sort.assert_called_once_with("year", "month")

    def test_compact_partitioned_compression_codec(
        self,
        compactor,
        mock_spark,
        mock_hdfs_client,
        sample_partitioned_table_info,
        mock_backup_mgr,
    ):
        """compression_codec on a partitioned table passes .option('compression', ...) to the writer."""
        sample_partitioned_table_info.compression_codec = "gzip"

        backup_info = BackupInfo(
            original_table="mydb.logs",
            backup_table="mydb.__bkp_logs_20240101_120000",
            original_location="hdfs:///data/mydb/logs",
            timestamp=datetime(2024, 1, 1, 12, 0, 0),
            partition_locations={
                "year=2024/month=01": "hdfs:///data/mydb/logs/year=2024/month=01",
            },
        )

        mock_df = MagicMock()
        mock_df.count.return_value = 500
        mock_df.coalesce.return_value = mock_df
        mock_df.write = MagicMock()
        mock_df.write.format.return_value = mock_df.write
        mock_df.write.mode.return_value = mock_df.write
        mock_df.write.option.return_value = mock_df.write
        mock_spark.read.format.return_value.load.return_value = mock_df
        mock_hdfs_client.get_file_info.return_value = HdfsFileInfo(
            file_count=8, total_size_bytes=1 * 1024 * 1024 * 1024
        )

        report = compactor.compact_table(sample_partitioned_table_info, backup_info)

        assert report.status == CompactionStatus.COMPLETED
        mock_df.write.option.assert_called_with("compression", "gzip")

    def test_compact_partitioned_three_levels_spec_sql(
        self,
        compactor,
        mock_spark,
        mock_hdfs_client,
        mock_backup_mgr,
    ):
        """3-level partition spec is formatted correctly in update_partition_location call."""
        from lakekeeper.models import FileFormat, PartitionInfo, TableInfo

        table_info = TableInfo(
            database="mydb",
            table_name="logs",
            location="hdfs:///data/mydb/logs",
            file_format=FileFormat.ORC,
            is_partitioned=True,
            partition_columns=["year", "month", "day"],
            partitions=[
                PartitionInfo(
                    spec={"year": "2024", "month": "01", "day": "15"},
                    location="hdfs:///data/mydb/logs/year=2024/month=01/day=15",
                    file_count=10,
                    total_size_bytes=10 * 1024 * 1024,
                    needs_compaction=True,
                    target_files=1,
                ),
            ],
            total_file_count=10,
            total_size_bytes=10 * 1024 * 1024,
            needs_compaction=True,
        )
        backup_info = BackupInfo(
            original_table="mydb.logs",
            backup_table="mydb.__bkp_logs_20240101_120000",
            original_location="hdfs:///data/mydb/logs",
            timestamp=datetime(2024, 1, 1, 12, 0, 0),
            partition_locations={
                "year=2024/month=01/day=15": "hdfs:///data/mydb/logs/year=2024/month=01/day=15",
            },
        )

        mock_df = MagicMock()
        mock_df.count.return_value = 100
        mock_df.coalesce.return_value = mock_df
        mock_df.write = MagicMock()
        mock_df.write.format.return_value = mock_df.write
        mock_df.write.mode.return_value = mock_df.write
        mock_spark.read.format.return_value.load.return_value = mock_df
        mock_hdfs_client.get_file_info.return_value = HdfsFileInfo(file_count=1, total_size_bytes=10 * 1024 * 1024)

        report = compactor.compact_table(table_info, backup_info)

        assert report.status == CompactionStatus.COMPLETED
        assert report.partitions_compacted == 1
        # Verify the 3-level SQL spec was passed correctly
        call_args = mock_backup_mgr.update_partition_location.call_args
        assert call_args is not None
        spec_sql_arg = call_args[0][1]  # second positional arg
        assert spec_sql_arg == "year='2024', month='01', day='15'"
