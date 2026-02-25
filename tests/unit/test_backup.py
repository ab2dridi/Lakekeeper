"""Tests for lakekeeper.core.backup module."""

from __future__ import annotations

from datetime import datetime
from unittest.mock import MagicMock

import pytest

from lakekeeper.core.backup import BackupManager


class TestBackupManager:
    @pytest.fixture
    def backup_mgr(self, mock_spark, config):
        return BackupManager(mock_spark, config)

    def test_create_backup_non_partitioned(self, backup_mgr, mock_spark, sample_table_info):
        # Mock SHOW CREATE TABLE response (SparkSQL returns backtick-quoted identifiers)
        sample_ddl = (
            "CREATE EXTERNAL TABLE `mydb`.`events` (`id` INT) STORED AS PARQUET LOCATION 'hdfs:///data/mydb/events'"
        )
        ddl_row = MagicMock(__getitem__=lambda self, i, ddl=sample_ddl: ddl)
        mock_spark.sql.return_value.collect.return_value = [ddl_row]

        backup_info = backup_mgr.create_backup(sample_table_info)

        assert backup_info.original_table == "mydb.events"
        assert backup_info.backup_table.startswith("mydb.__bkp_events_")
        assert backup_info.original_location == "hdfs:///data/mydb/events"
        assert backup_info.partition_locations == {}
        assert isinstance(backup_info.timestamp, datetime)

        calls = [str(c) for c in mock_spark.sql.call_args_list]
        # SHOW CREATE TABLE was called first
        assert any("SHOW CREATE TABLE" in c for c in calls)
        # Modified DDL with backup table name was executed
        assert any("__bkp_events_" in c for c in calls)
        # Purge protection via ALTER TABLE SET TBLPROPERTIES (separate call)
        alter_tblprops = [c for c in calls if "SET TBLPROPERTIES" in c]
        assert len(alter_tblprops) == 1
        assert "external.table.purge" in alter_tblprops[0]
        assert "'false'" in alter_tblprops[0]

    def test_create_backup_partitioned(self, backup_mgr, mock_spark, sample_partitioned_table_info):
        # Mock SHOW CREATE TABLE response
        sample_ddl = (
            "CREATE EXTERNAL TABLE `mydb`.`logs` (`id` INT) "
            "PARTITIONED BY (`year` STRING, `month` STRING) "
            "STORED AS ORC "
            "LOCATION 'hdfs:///data/mydb/logs'"
        )
        ddl_row = MagicMock(__getitem__=lambda self, i, ddl=sample_ddl: ddl)
        mock_spark.sql.return_value.collect.return_value = [ddl_row]

        backup_info = backup_mgr.create_backup(sample_partitioned_table_info)

        assert backup_info.original_table == "mydb.logs"
        assert backup_info.backup_table.startswith("mydb.__bkp_logs_")
        # Only the partition that needs compaction should be backed up
        assert len(backup_info.partition_locations) == 1
        assert "year=2024/month=01" in backup_info.partition_locations

        calls = [str(c) for c in mock_spark.sql.call_args_list]
        # SHOW CREATE TABLE was called first
        assert any("SHOW CREATE TABLE" in c for c in calls)
        # Modified DDL with backup table name was executed
        assert any("__bkp_logs_" in c for c in calls)
        # Purge protection via ALTER TABLE SET TBLPROPERTIES (separate call)
        alter_tblprops = [c for c in calls if "SET TBLPROPERTIES" in c]
        assert len(alter_tblprops) == 1
        assert "external.table.purge" in alter_tblprops[0]
        assert "'false'" in alter_tblprops[0]
        # ALTER TABLE ADD PARTITION for the one partition needing compaction
        add_partition_calls = [c for c in calls if "ADD PARTITION" in c]
        assert len(add_partition_calls) == 1

    def test_find_latest_backup_found(self, backup_mgr, mock_spark):
        table_rows = [
            MagicMock(**{"__getitem__": lambda s, k: "__bkp_events_20240101_120000" if k == "tableName" else None}),
            MagicMock(**{"__getitem__": lambda s, k: "__bkp_events_20240102_120000" if k == "tableName" else None}),
            MagicMock(**{"__getitem__": lambda s, k: "events" if k == "tableName" else None}),
        ]

        desc_rows = [
            MagicMock(__getitem__=lambda self, i, vals=("Location", "hdfs:///data/mydb/events", None): vals[i]),
        ]

        mock_spark.sql.return_value.collect.side_effect = [
            table_rows,  # SHOW TABLES
            desc_rows,  # DESCRIBE FORMATTED backup table
            Exception("not partitioned"),  # SHOW PARTITIONS will fail
        ]

        result = backup_mgr.find_latest_backup("mydb", "events")
        assert result is not None
        assert result.backup_table == "mydb.__bkp_events_20240102_120000"

    def test_find_latest_backup_not_found(self, backup_mgr, mock_spark):
        table_rows = [
            MagicMock(**{"__getitem__": lambda s, k: "events" if k == "tableName" else None}),
        ]
        mock_spark.sql.return_value.collect.return_value = table_rows

        result = backup_mgr.find_latest_backup("mydb", "events")
        assert result is None

    def test_list_backups(self, backup_mgr, mock_spark):
        table_rows = [
            MagicMock(**{"__getitem__": lambda s, k: "__bkp_events_20240101_120000" if k == "tableName" else None}),
            MagicMock(**{"__getitem__": lambda s, k: "__bkp_events_20240102_120000" if k == "tableName" else None}),
            MagicMock(**{"__getitem__": lambda s, k: "events" if k == "tableName" else None}),
        ]
        mock_spark.sql.return_value.collect.return_value = table_rows

        result = backup_mgr.list_backups("mydb", "events")
        assert len(result) == 2
        assert result[0] == "__bkp_events_20240102_120000"  # newest first

    def test_drop_backup(self, backup_mgr, mock_spark):
        backup_mgr.drop_backup("mydb", "__bkp_events_20240101_120000")

        mock_spark.sql.assert_called_with("DROP TABLE IF EXISTS mydb.__bkp_events_20240101_120000")

    def test_update_table_location(self, backup_mgr, mock_spark):
        backup_mgr.update_table_location(
            "mydb.__bkp_events_20240101_120000",
            "hdfs:///data/mydb/events__old_1708001000",
        )

        mock_spark.sql.assert_called_with(
            "ALTER TABLE mydb.__bkp_events_20240101_120000 SET LOCATION 'hdfs:///data/mydb/events__old_1708001000'"
        )

    def test_find_latest_backup_unparseable_timestamp(self, backup_mgr, mock_spark):
        """When the backup name has a non-standard suffix, timestamp falls back to now()."""
        table_rows = [
            MagicMock(**{"__getitem__": lambda s, k: "__bkp_events_badformat" if k == "tableName" else None}),
        ]
        desc_rows = [
            MagicMock(__getitem__=lambda self, i, vals=("Location", "hdfs:///data/mydb/events", None): vals[i]),
        ]
        mock_spark.sql.return_value.collect.side_effect = [
            table_rows,
            desc_rows,
            Exception("no partitions"),
        ]

        result = backup_mgr.find_latest_backup("mydb", "events")
        assert result is not None
        assert result.backup_table == "mydb.__bkp_events_badformat"
        # Timestamp is set to something (datetime.now() fallback), not None
        assert result.timestamp is not None

    def test_get_backup_partition_locations_success(self, backup_mgr, mock_spark):
        """Partition locations are built from SHOW PARTITIONS + DESCRIBE FORMATTED."""
        part_row = MagicMock()
        part_row.__getitem__ = lambda self, i: "year=2024/month=01" if i == 0 else ""

        desc_row = MagicMock()
        desc_row.__getitem__ = lambda self, i: ("Location", "hdfs:///data/mydb/logs/year=2024/month=01", "")[i]

        mock_spark.sql.return_value.collect.side_effect = [
            [part_row],  # SHOW PARTITIONS
            [desc_row],  # DESCRIBE FORMATTED … PARTITION(…)
        ]

        result = backup_mgr._get_backup_partition_locations("mydb.__bkp_logs_20240101_120000")
        assert result == {"year=2024/month=01": "hdfs:///data/mydb/logs/year=2024/month=01"}

    def test_get_backup_partition_locations_exception(self, backup_mgr, mock_spark):
        """SHOW PARTITIONS failure returns an empty dict (non-partitioned table)."""
        mock_spark.sql.return_value.collect.side_effect = Exception("not partitioned")

        # Call the private method directly
        result = backup_mgr._get_backup_partition_locations("mydb.__bkp_events_20240101_120000")
        assert result == {}

    def test_update_partition_location(self, backup_mgr, mock_spark):
        backup_mgr.update_partition_location(
            "mydb.__bkp_logs_20240101_120000",
            "year='2024', month='01'",
            "hdfs:///data/mydb/logs/year=2024/month=01__old_1708001000",
        )

        mock_spark.sql.assert_called_with(
            "ALTER TABLE mydb.__bkp_logs_20240101_120000 "
            "PARTITION(year='2024', month='01') "
            "SET LOCATION 'hdfs:///data/mydb/logs/year=2024/month=01__old_1708001000'"
        )

    def test_create_backup_original_purge_true_non_partitioned(self, backup_mgr, mock_spark, sample_table_info):
        """When the original table has purge='true', the backup must still be set to purge='false'.

        On CDP clusters, external.table.purge defaults to 'true'. SHOW CREATE TABLE
        returns a DDL that contains TBLPROPERTIES ('external.table.purge'='true').
        The backup DDL inherits this, but the subsequent ALTER TABLE must override it
        so the backup table never deletes data when dropped.

        CRITICAL: The original table's purge property must NEVER be modified.
        """
        original_table = "mydb.events"
        sample_ddl = (
            "CREATE EXTERNAL TABLE `mydb`.`events` (`id` INT) "
            "STORED AS PARQUET "
            "LOCATION 'hdfs:///data/mydb/events' "
            "TBLPROPERTIES ('external.table.purge'='true')"
        )
        ddl_row = MagicMock(__getitem__=lambda self, i, ddl=sample_ddl: ddl)
        mock_spark.sql.return_value.collect.return_value = [ddl_row]

        backup_info = backup_mgr.create_backup(sample_table_info)

        calls = [str(c) for c in mock_spark.sql.call_args_list]

        # The ALTER TABLE call MUST set purge='false' on the BACKUP table only
        alter_tblprops = [c for c in calls if "SET TBLPROPERTIES" in c]
        assert len(alter_tblprops) == 1
        assert "external.table.purge" in alter_tblprops[0]
        assert "'false'" in alter_tblprops[0]
        assert "__bkp_events_" in alter_tblprops[0], "SET TBLPROPERTIES must target the backup table, not the original"

        # CRITICAL: the original table MUST NEVER be subject to SET TBLPROPERTIES
        original_modified = [c for c in calls if "SET TBLPROPERTIES" in c and original_table in c and "__bkp_" not in c]
        assert original_modified == [], (
            f"Original table '{original_table}' must never have its TBLPROPERTIES modified: {original_modified}"
        )
        assert backup_info.backup_table.startswith("mydb.__bkp_events_")

    def test_create_backup_original_purge_true_partitioned(self, backup_mgr, mock_spark, sample_partitioned_table_info):
        """Partitioned table with purge='true': backup must always have purge='false'.

        CRITICAL: The original table's purge property must NEVER be modified.
        """
        original_table = "mydb.logs"
        sample_ddl = (
            "CREATE EXTERNAL TABLE `mydb`.`logs` (`id` INT) "
            "PARTITIONED BY (`year` STRING, `month` STRING) "
            "STORED AS ORC "
            "LOCATION 'hdfs:///data/mydb/logs' "
            "TBLPROPERTIES ('external.table.purge'='true')"
        )
        ddl_row = MagicMock(__getitem__=lambda self, i, ddl=sample_ddl: ddl)
        mock_spark.sql.return_value.collect.return_value = [ddl_row]

        backup_mgr.create_backup(sample_partitioned_table_info)

        calls = [str(c) for c in mock_spark.sql.call_args_list]

        # The ALTER TABLE call MUST set purge='false' on the BACKUP table only
        alter_tblprops = [c for c in calls if "SET TBLPROPERTIES" in c]
        assert len(alter_tblprops) == 1
        assert "external.table.purge" in alter_tblprops[0]
        assert "'false'" in alter_tblprops[0]
        assert "__bkp_logs_" in alter_tblprops[0], "SET TBLPROPERTIES must target the backup table, not the original"

        # CRITICAL: the original table MUST NEVER be subject to SET TBLPROPERTIES
        original_modified = [c for c in calls if "SET TBLPROPERTIES" in c and original_table in c and "__bkp_" not in c]
        assert original_modified == [], (
            f"Original table '{original_table}' must never have its TBLPROPERTIES modified: {original_modified}"
        )

        # ALTER TABLE for purge=false comes AFTER the CREATE
        create_idx = next(i for i, c in enumerate(calls) if "__bkp_logs_" in c and "SET TBLPROPERTIES" not in c)
        alter_idx = next(i for i, c in enumerate(calls) if "SET TBLPROPERTIES" in c and "external.table.purge" in c)
        assert alter_idx > create_idx, "ALTER TABLE purge='false' must come AFTER the CREATE"

    def test_original_table_tblproperties_never_modified(self, backup_mgr, mock_spark, sample_table_info):
        """Exhaustive regression guard: no SQL call must apply SET TBLPROPERTIES to the original table.

        Lakekeeper NEVER modifies the TBLPROPERTIES of the original table,
        regardless of the original purge setting ('true' or 'false').
        Only the backup table gets SET TBLPROPERTIES ('external.table.purge'='false').
        """
        original_table = "mydb.events"
        for purge_value in ("'true'", "'false'"):
            mock_spark.sql.reset_mock()
            sample_ddl = (
                f"CREATE EXTERNAL TABLE `mydb`.`events` (`id` INT) "
                f"STORED AS PARQUET "
                f"LOCATION 'hdfs:///data/mydb/events' "
                f"TBLPROPERTIES ('external.table.purge'={purge_value})"
            )
            ddl_row = MagicMock(__getitem__=lambda self, i, ddl=sample_ddl: ddl)
            mock_spark.sql.return_value.collect.return_value = [ddl_row]

            backup_mgr.create_backup(sample_table_info)

            calls = [str(c) for c in mock_spark.sql.call_args_list]
            violating = [c for c in calls if "SET TBLPROPERTIES" in c and original_table in c and "__bkp_" not in c]
            assert violating == [], (
                f"With purge={purge_value}: original table TBLPROPERTIES must never be modified, but found: {violating}"
            )
