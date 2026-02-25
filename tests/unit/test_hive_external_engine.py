"""Tests for lakekeeper.engine.hive_external module."""

from __future__ import annotations

from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from lakekeeper.engine.hive_external import HiveExternalEngine
from lakekeeper.models import BackupInfo, CompactionReport, CompactionStatus, FileFormat, TableInfo


class TestHiveExternalEngine:
    @pytest.fixture
    def engine(self, mock_spark, config):
        with (
            patch("lakekeeper.engine.hive_external.HdfsClient") as mock_hdfs_cls,
            patch("lakekeeper.engine.hive_external.TableAnalyzer") as mock_analyzer_cls,
            patch("lakekeeper.engine.hive_external.BackupManager") as mock_backup_cls,
            patch("lakekeeper.engine.hive_external.Compactor") as mock_compactor_cls,
        ):
            engine = HiveExternalEngine(mock_spark, config)
            engine._mock_analyzer = mock_analyzer_cls.return_value
            engine._mock_backup_mgr = mock_backup_cls.return_value
            engine._mock_compactor = mock_compactor_cls.return_value
            engine._mock_hdfs = mock_hdfs_cls.return_value
            return engine

    def test_analyze(self, engine):
        expected = TableInfo(
            database="mydb",
            table_name="tbl",
            location="hdfs:///data",
            file_format=FileFormat.PARQUET,
        )
        engine._mock_analyzer.analyze_table.return_value = expected

        result = engine.analyze("mydb", "tbl")
        assert result == expected
        engine._mock_analyzer.analyze_table.assert_called_with("mydb", "tbl")

    def test_create_backup(self, engine, sample_table_info):
        expected = BackupInfo(
            original_table="mydb.events",
            backup_table="mydb.__bkp_events_20240101_120000",
            original_location="hdfs:///data/mydb/events",
            timestamp=datetime.now(),
        )
        engine._mock_backup_mgr.create_backup.return_value = expected

        result = engine.create_backup(sample_table_info)
        assert result == expected

    def test_compact(self, engine, sample_table_info):
        backup_info = BackupInfo(
            original_table="mydb.events",
            backup_table="mydb.__bkp_events_20240101_120000",
            original_location="hdfs:///data/mydb/events",
            timestamp=datetime.now(),
        )
        expected = CompactionReport(
            table_name="mydb.events",
            status=CompactionStatus.COMPLETED,
        )
        engine._mock_compactor.compact_table.return_value = expected

        result = engine.compact(sample_table_info, backup_info)
        assert result.status == CompactionStatus.COMPLETED

    def test_rollback_found(self, engine, mock_spark):
        # Legacy fallback: original_location has no __old_ suffix
        backup_info = BackupInfo(
            original_table="mydb.events",
            backup_table="mydb.__bkp_events_20240101_120000",
            original_location="hdfs:///data/mydb/events",
            timestamp=datetime.now(),
        )
        engine._mock_backup_mgr.find_latest_backup.return_value = backup_info

        result = engine.rollback("mydb", "events")

        assert result == backup_info
        mock_spark.sql.assert_any_call("ALTER TABLE mydb.events SET LOCATION 'hdfs:///data/mydb/events'")

    def test_rollback_rename_based(self, engine, mock_spark):
        # New strategy: original_location points to __old_ dir after compaction
        backup_info = BackupInfo(
            original_table="mydb.events",
            backup_table="mydb.__bkp_events_20240101_120000",
            original_location="hdfs:///data/mydb/events__old_1708001000",
            timestamp=datetime.now(),
        )
        engine._mock_backup_mgr.find_latest_backup.return_value = backup_info
        engine._mock_hdfs.path_exists.return_value = True  # compacted data exists at current loc

        engine.rollback("mydb", "events")

        # Compacted data deleted, original data renamed back
        engine._mock_hdfs.delete_path.assert_called_once_with("hdfs:///data/mydb/events")
        engine._mock_hdfs.rename_path.assert_called_once_with(
            "hdfs:///data/mydb/events__old_1708001000",
            "hdfs:///data/mydb/events",
        )
        # No ALTER TABLE SET LOCATION on the main table
        sql_calls = [str(c) for c in mock_spark.sql.call_args_list]
        assert not any("ALTER TABLE mydb.events SET LOCATION" in c for c in sql_calls)
        engine._mock_backup_mgr.drop_backup.assert_called_once()

    def test_rollback_partitioned_rename_based(self, engine, mock_spark):
        # Partitioned table: partition locations contain __old_
        backup_info = BackupInfo(
            original_table="mydb.logs",
            backup_table="mydb.__bkp_logs_20240101_120000",
            original_location="hdfs:///data/mydb/logs",
            timestamp=datetime.now(),
            partition_locations={
                "year=2024/month=01": "hdfs:///data/mydb/logs/year=2024/month=01__old_1708001000",
            },
        )
        engine._mock_backup_mgr.find_latest_backup.return_value = backup_info
        engine._mock_hdfs.path_exists.return_value = True

        engine.rollback("mydb", "logs")

        engine._mock_hdfs.delete_path.assert_called_once_with("hdfs:///data/mydb/logs/year=2024/month=01")
        engine._mock_hdfs.rename_path.assert_called_once_with(
            "hdfs:///data/mydb/logs/year=2024/month=01__old_1708001000",
            "hdfs:///data/mydb/logs/year=2024/month=01",
        )
        sql_calls = [str(c) for c in mock_spark.sql.call_args_list]
        assert not any("ALTER TABLE mydb.logs PARTITION" in c for c in sql_calls)

    def test_rollback_not_found(self, engine):
        engine._mock_backup_mgr.find_latest_backup.return_value = None

        with pytest.raises(ValueError, match="No backup found"):
            engine.rollback("mydb", "events")

    def test_rollback_partitioned(self, engine, mock_spark):
        backup_info = BackupInfo(
            original_table="mydb.logs",
            backup_table="mydb.__bkp_logs_20240101_120000",
            original_location="hdfs:///data/mydb/logs",
            timestamp=datetime.now(),
            partition_locations={
                "year=2024/month=01": "hdfs:///data/mydb/logs/year=2024/month=01",
            },
        )
        engine._mock_backup_mgr.find_latest_backup.return_value = backup_info

        engine.rollback("mydb", "logs")

        mock_spark.sql.assert_any_call(
            "ALTER TABLE mydb.logs PARTITION(year='2024', month='01') "
            "SET LOCATION 'hdfs:///data/mydb/logs/year=2024/month=01'"
        )

    def test_cleanup(self, engine):
        engine._mock_backup_mgr.list_backups.return_value = [
            "__bkp_events_20240101_120000",
            "__bkp_events_20240102_120000",
        ]

        cleaned = engine.cleanup("mydb", "events")
        assert cleaned == 2

    def test_cleanup_with_unparseable_timestamp_still_cleans(self, engine):
        """Backup with non-standard timestamp format is cleaned even when older_than_days is set."""
        engine._mock_backup_mgr.list_backups.return_value = ["__bkp_events_badformat"]

        # With older_than_days set, the ValueError branch is hit; backup is still cleaned
        cleaned = engine.cleanup("mydb", "events", older_than_days=7)
        assert cleaned == 1
        engine._mock_backup_mgr.drop_backup.assert_called_once_with("mydb", "__bkp_events_badformat")

    def test_delete_old_data_dirs_table_level_deletes_old_path(self, engine, mock_spark):
        """Table-level __old_ location is deleted when path exists."""

        def _row(col0, col1=""):
            r = MagicMock()
            r.__getitem__ = lambda self, i: [col0, col1][i] if i < 2 else ""
            return r

        table_desc = [_row("Location", "hdfs:///data/mydb/events__old_1708001000")]
        mock_spark.sql.return_value.collect.side_effect = [table_desc, Exception("no partitions")]
        engine._mock_hdfs.path_exists.return_value = True

        engine._delete_old_data_dirs("mydb.__bkp_events_20240101_120000")

        engine._mock_hdfs.delete_path.assert_called_once_with("hdfs:///data/mydb/events__old_1708001000")

    def test_list_tables_skips_undescribable_tables(self, engine, mock_spark):
        """Tables that cannot be described are silently skipped."""
        table_rows = [
            MagicMock(**{"__getitem__": lambda s, k: "broken_tbl" if k == "tableName" else None}),
        ]
        mock_spark.sql.return_value.collect.side_effect = [
            table_rows,
            Exception("permission denied"),
        ]

        result = engine.list_tables("mydb")
        assert result == []

    def test_cleanup_deletes_partition_old_dirs(self, engine, mock_spark):
        engine._mock_backup_mgr.list_backups.return_value = ["__bkp_logs_20240101_120000"]

        def _row(col0, col1=""):
            r = MagicMock()
            r.__getitem__ = lambda self, i: [col0, col1][i] if i < 2 else ""
            return r

        # DESCRIBE FORMATTED → table-level location (no __old_)
        table_desc = [_row("Location", "hdfs:///data/mydb/logs")]
        # SHOW PARTITIONS → one partition
        part_rows = [MagicMock(__getitem__=lambda self, i: "year=2024/month=01" if i == 0 else "")]
        # DESCRIBE FORMATTED PARTITION → partition __old_ location
        part_desc = [_row("Location", "hdfs:///data/mydb/logs/year=2024/month=01__old_1708001000")]

        mock_spark.sql.return_value.collect.side_effect = [table_desc, part_rows, part_desc]
        engine._mock_hdfs.path_exists.return_value = True

        cleaned = engine.cleanup("mydb", "logs")

        assert cleaned == 1
        engine._mock_hdfs.delete_path.assert_called_once_with(
            "hdfs:///data/mydb/logs/year=2024/month=01__old_1708001000"
        )

    def test_cleanup_with_age_filter(self, engine, config):
        engine._mock_backup_mgr.list_backups.return_value = [
            "__bkp_events_20200101_120000",  # old
        ]

        cleaned = engine.cleanup("mydb", "events", older_than_days=7)
        assert cleaned == 1

    def test_delete_old_data_dirs_describe_fails(self, engine, mock_spark):
        """DESCRIBE FORMATTED failure is silently ignored."""
        mock_spark.sql.return_value.collect.side_effect = Exception("Metastore down")
        # Should not raise
        engine._delete_old_data_dirs("mydb.__bkp_events_20240101_120000")

    def test_delete_old_data_dirs_partitions_fail(self, engine, mock_spark):
        """SHOW PARTITIONS failure after reading table location is silently ignored."""

        def _row(col0, col1=""):
            r = MagicMock()
            r.__getitem__ = lambda self, i: [col0, col1][i] if i < 2 else ""
            return r

        table_desc = [_row("Location", "hdfs:///data/mydb/logs")]
        mock_spark.sql.return_value.collect.side_effect = [table_desc, Exception("no partitions")]
        # Should not raise
        engine._delete_old_data_dirs("mydb.__bkp_logs_20240101_120000")

    def test_cleanup_orphan_backups(self, engine, mock_spark):
        """Backup tables whose original tables don't exist are dropped."""
        table_rows = [
            MagicMock(**{"__getitem__": lambda s, k: "__bkp_events_20240101_120000" if k == "tableName" else None}),
        ]
        mock_spark.sql.return_value.collect.side_effect = [
            table_rows,  # SHOW TABLES
            Exception("not partitioned"),  # _delete_old_data_dirs: DESCRIBE FORMATTED fails
        ]

        cleaned = engine.cleanup_orphan_backups("mydb")

        assert cleaned == 1
        engine._mock_backup_mgr.drop_backup.assert_called_once_with("mydb", "__bkp_events_20240101_120000")

    def test_cleanup_orphan_backups_skips_existing(self, engine, mock_spark):
        """Backup tables whose original table still exists are not dropped."""
        table_rows = [
            MagicMock(**{"__getitem__": lambda s, k: "__bkp_events_20240101_120000" if k == "tableName" else None}),
            MagicMock(**{"__getitem__": lambda s, k: "events" if k == "tableName" else None}),
        ]
        mock_spark.sql.return_value.collect.return_value = table_rows

        cleaned = engine.cleanup_orphan_backups("mydb")

        assert cleaned == 0
        engine._mock_backup_mgr.drop_backup.assert_not_called()

    def test_list_tables(self, engine, mock_spark):
        table_rows = [
            MagicMock(**{"__getitem__": lambda s, k: "events" if k == "tableName" else None}),
            MagicMock(**{"__getitem__": lambda s, k: "__bkp_events_20240101" if k == "tableName" else None}),
            MagicMock(**{"__getitem__": lambda s, k: "users" if k == "tableName" else None}),
        ]

        def _make_row(col0, col1="", col2=""):
            row = MagicMock()
            row.__getitem__ = lambda self, i: [col0, col1, col2][i]
            return row

        desc_external = [_make_row("Table Type", "EXTERNAL_TABLE")]
        desc_managed = [_make_row("Table Type", "MANAGED_TABLE")]

        # SHOW TABLES returns all, then DESCRIBE each non-backup
        mock_spark.sql.return_value.collect.side_effect = [
            table_rows,
            desc_external,  # events
            desc_managed,  # users (not external)
        ]

        result = engine.list_tables("mydb")
        assert "events" in result
        assert "users" not in result
        assert "__bkp_events_20240101" not in result


class TestAnalyzeAfterCompaction:
    """Tests for the analyze_after_compaction feature."""

    def _make_engine(self, mock_spark, config):
        with (
            patch("lakekeeper.engine.hive_external.HdfsClient"),
            patch("lakekeeper.engine.hive_external.TableAnalyzer"),
            patch("lakekeeper.engine.hive_external.BackupManager"),
            patch("lakekeeper.engine.hive_external.Compactor") as mock_compactor_cls,
        ):
            engine = HiveExternalEngine(mock_spark, config)
            engine._mock_compactor = mock_compactor_cls.return_value
            return engine

    def _backup_info(self):
        return BackupInfo(
            original_table="mydb.events",
            backup_table="mydb.__bkp_events_20240101_120000",
            original_location="hdfs:///data/mydb/events",
            timestamp=datetime(2024, 1, 1, 12, 0, 0),
        )

    def test_analyze_called_after_successful_compaction(self, mock_spark, config):
        """ANALYZE TABLE is issued when analyze_after_compaction=True and compaction succeeds."""
        config.analyze_after_compaction = True
        engine = self._make_engine(mock_spark, config)
        engine._mock_compactor.compact_table.return_value = CompactionReport(
            table_name="mydb.events", status=CompactionStatus.COMPLETED
        )
        table_info = TableInfo(
            database="mydb",
            table_name="events",
            location="hdfs:///data/mydb/events",
            file_format=FileFormat.PARQUET,
            is_partitioned=False,
        )

        engine.compact(table_info, self._backup_info())

        sql_calls = [call.args[0] for call in mock_spark.sql.call_args_list]
        assert any("ANALYZE TABLE mydb.events COMPUTE STATISTICS" in c for c in sql_calls)

    def test_analyze_not_called_when_disabled(self, mock_spark, config):
        """ANALYZE TABLE is NOT issued when analyze_after_compaction=False (default)."""
        assert config.analyze_after_compaction is False
        engine = self._make_engine(mock_spark, config)
        engine._mock_compactor.compact_table.return_value = CompactionReport(
            table_name="mydb.events", status=CompactionStatus.COMPLETED
        )
        table_info = TableInfo(
            database="mydb",
            table_name="events",
            location="hdfs:///data/mydb/events",
            file_format=FileFormat.PARQUET,
            is_partitioned=False,
        )

        engine.compact(table_info, self._backup_info())

        sql_calls = [call.args[0] for call in mock_spark.sql.call_args_list]
        assert not any("ANALYZE TABLE" in c for c in sql_calls)

    def test_analyze_not_called_on_failed_compaction(self, mock_spark, config):
        """ANALYZE TABLE is NOT issued when the compaction failed."""
        config.analyze_after_compaction = True
        engine = self._make_engine(mock_spark, config)
        engine._mock_compactor.compact_table.return_value = CompactionReport(
            table_name="mydb.events", status=CompactionStatus.FAILED
        )
        table_info = TableInfo(
            database="mydb",
            table_name="events",
            location="hdfs:///data/mydb/events",
            file_format=FileFormat.PARQUET,
            is_partitioned=False,
        )

        engine.compact(table_info, self._backup_info())

        sql_calls = [call.args[0] for call in mock_spark.sql.call_args_list]
        assert not any("ANALYZE TABLE" in c for c in sql_calls)

    def test_analyze_not_called_in_dry_run(self, mock_spark, config):
        """ANALYZE TABLE is NOT issued when dry_run=True."""
        config.analyze_after_compaction = True
        config.dry_run = True
        engine = self._make_engine(mock_spark, config)
        engine._mock_compactor.compact_table.return_value = CompactionReport(
            table_name="mydb.events", status=CompactionStatus.COMPLETED
        )
        table_info = TableInfo(
            database="mydb",
            table_name="events",
            location="hdfs:///data/mydb/events",
            file_format=FileFormat.PARQUET,
            is_partitioned=False,
        )

        engine.compact(table_info, self._backup_info())

        sql_calls = [call.args[0] for call in mock_spark.sql.call_args_list]
        assert not any("ANALYZE TABLE" in c for c in sql_calls)

    def test_analyze_partitioned_table_per_partition_and_table(self, mock_spark, config, sample_partitioned_table_info):
        """Partitioned table: per-compacted-partition ANALYZE then table-level ANALYZE."""
        config.analyze_after_compaction = True
        engine = self._make_engine(mock_spark, config)
        engine._mock_compactor.compact_table.return_value = CompactionReport(
            table_name="mydb.logs", status=CompactionStatus.COMPLETED
        )

        engine.compact(
            sample_partitioned_table_info,
            BackupInfo(
                original_table="mydb.logs",
                backup_table="mydb.__bkp_logs_20240101_120000",
                original_location="hdfs:///data/mydb/logs",
                timestamp=datetime(2024, 1, 1),
            ),
        )

        sql_calls = [call.args[0] for call in mock_spark.sql.call_args_list]
        # One compacted partition (year=2024, month=01) — needs_compaction=True
        assert any(
            "ANALYZE TABLE mydb.logs PARTITION(year='2024', month='01') COMPUTE STATISTICS" in c for c in sql_calls
        )
        # Skipped partition (year=2024, month=02) — needs_compaction=False
        assert not any(
            "ANALYZE TABLE mydb.logs PARTITION(year='2024', month='02') COMPUTE STATISTICS" in c for c in sql_calls
        )
        # Table-level stats always run
        assert any("ANALYZE TABLE mydb.logs COMPUTE STATISTICS" in c for c in sql_calls)

    def test_analyze_failure_does_not_raise(self, mock_spark, config):
        """ANALYZE TABLE failure is caught and logged as a warning, not re-raised."""
        config.analyze_after_compaction = True
        engine = self._make_engine(mock_spark, config)
        engine._mock_compactor.compact_table.return_value = CompactionReport(
            table_name="mydb.events", status=CompactionStatus.COMPLETED
        )
        mock_spark.sql.side_effect = Exception("Metastore unavailable")

        table_info = TableInfo(
            database="mydb",
            table_name="events",
            location="hdfs:///data/mydb/events",
            file_format=FileFormat.PARQUET,
            is_partitioned=False,
        )
        # Must not raise — ANALYZE failure is non-fatal
        report = engine.compact(table_info, self._backup_info())
        assert report.status == CompactionStatus.COMPLETED
