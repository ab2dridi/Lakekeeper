"""Tests for lakekeeper.cli module."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from lakekeeper.cli import _parse_duration, main


class TestParseDate:
    def test_valid_days(self):
        assert _parse_duration("7d") == 7
        assert _parse_duration("30d") == 30
        assert _parse_duration("1d") == 1

    def test_invalid_format(self):
        from click import BadParameter

        with pytest.raises(BadParameter):
            _parse_duration("7h")

    def test_invalid_number(self):
        from click import BadParameter

        with pytest.raises(BadParameter):
            _parse_duration("abcd")


class TestCLIGroup:
    def test_version(self):
        runner = CliRunner()
        result = runner.invoke(main, ["--version"])
        assert result.exit_code == 0
        assert "0.0.3" in result.output

    def test_help(self):
        runner = CliRunner()
        result = runner.invoke(main, ["--help"])
        assert result.exit_code == 0
        assert "Lakekeeper" in result.output


class TestAnalyzeCommand:
    @patch("lakekeeper.cli._get_engine")
    def test_analyze_single_table(self, mock_get_engine):
        from lakekeeper.models import FileFormat, TableInfo

        mock_engine = MagicMock()
        mock_get_engine.return_value = mock_engine
        mock_engine.analyze.return_value = TableInfo(
            database="mydb",
            table_name="tbl",
            location="hdfs:///data",
            file_format=FileFormat.PARQUET,
            total_file_count=100,
            total_size_bytes=1024,
        )

        runner = CliRunner()
        result = runner.invoke(main, ["analyze", "--table", "mydb.tbl"])
        assert result.exit_code == 0
        assert "1 table(s)" in result.output

    @patch("lakekeeper.cli._get_engine")
    def test_analyze_database(self, mock_get_engine):
        from lakekeeper.models import FileFormat, TableInfo

        mock_engine = MagicMock()
        mock_get_engine.return_value = mock_engine
        mock_engine.list_tables.return_value = ["t1", "t2"]
        mock_engine.analyze.return_value = TableInfo(
            database="mydb",
            table_name="t1",
            location="hdfs:///data",
            file_format=FileFormat.PARQUET,
        )

        runner = CliRunner()
        result = runner.invoke(main, ["analyze", "--database", "mydb"])
        assert result.exit_code == 0
        assert "2 table(s)" in result.output

    def test_analyze_no_args(self):
        runner = CliRunner()
        result = runner.invoke(main, ["analyze"])
        # Should fail with error (no engine, but would need --database or --table)
        assert result.exit_code != 0 or "Error" in result.output


class TestCompactCommand:
    @patch("lakekeeper.cli._get_engine")
    def test_compact_dry_run(self, mock_get_engine):
        from lakekeeper.models import FileFormat, TableInfo

        mock_engine = MagicMock()
        mock_get_engine.return_value = mock_engine
        mock_engine.analyze.return_value = TableInfo(
            database="mydb",
            table_name="tbl",
            location="hdfs:///data",
            file_format=FileFormat.PARQUET,
            total_file_count=65000,
            total_size_bytes=3 * 1024 * 1024 * 1024,
            needs_compaction=True,
        )

        runner = CliRunner()
        result = runner.invoke(main, ["compact", "--table", "mydb.tbl", "--dry-run"])
        assert result.exit_code == 0
        assert "DRY RUN" in result.output
        mock_engine.create_backup.assert_not_called()

    @patch("lakekeeper.cli._get_engine")
    def test_compact_skip_no_compaction(self, mock_get_engine):
        from lakekeeper.models import FileFormat, TableInfo

        mock_engine = MagicMock()
        mock_get_engine.return_value = mock_engine
        mock_engine.analyze.return_value = TableInfo(
            database="mydb",
            table_name="tbl",
            location="hdfs:///data",
            file_format=FileFormat.PARQUET,
            needs_compaction=False,
        )

        runner = CliRunner()
        result = runner.invoke(main, ["compact", "--table", "mydb.tbl"])
        assert result.exit_code == 0
        assert "no compaction needed" in result.output


class TestRollbackCommand:
    @patch("lakekeeper.cli._get_engine")
    def test_rollback(self, mock_get_engine):
        from lakekeeper.models import BackupInfo

        mock_engine = MagicMock()
        mock_get_engine.return_value = mock_engine
        mock_engine.rollback.return_value = BackupInfo(
            original_table="mydb.tbl",
            backup_table="mydb.__bkp_tbl_20240101_120000",
            original_location="hdfs:///data",
            timestamp=MagicMock(),
        )

        runner = CliRunner()
        result = runner.invoke(main, ["rollback", "--table", "mydb.tbl"])
        assert result.exit_code == 0
        assert "__bkp_tbl_20240101_120000" in result.output
        mock_engine.rollback.assert_called_once_with("mydb", "tbl")

    def test_rollback_missing_table(self):
        runner = CliRunner()
        result = runner.invoke(main, ["rollback"])
        assert result.exit_code != 0


class TestCleanupCommand:
    @patch("lakekeeper.cli._get_engine")
    def test_cleanup_table(self, mock_get_engine):
        mock_engine = MagicMock()
        mock_get_engine.return_value = mock_engine
        mock_engine.cleanup.return_value = 2

        runner = CliRunner()
        result = runner.invoke(main, ["cleanup", "--table", "mydb.tbl"])
        assert result.exit_code == 0
        assert "Cleaned 2 backup(s)" in result.output

    @patch("lakekeeper.cli._get_engine")
    def test_cleanup_database_with_older_than(self, mock_get_engine):
        mock_engine = MagicMock()
        mock_get_engine.return_value = mock_engine
        mock_engine.list_tables.return_value = ["t1"]
        mock_engine.cleanup.return_value = 1

        runner = CliRunner()
        result = runner.invoke(main, ["cleanup", "--database", "mydb", "--older-than", "7d"])
        assert result.exit_code == 0
        mock_engine.cleanup.assert_called_once_with("mydb", "t1", 7)

    @patch("lakekeeper.cli._get_engine")
    def test_cleanup_no_args(self, mock_get_engine):
        mock_get_engine.return_value = MagicMock()
        runner = CliRunner()
        result = runner.invoke(main, ["cleanup"])
        assert result.exit_code != 0 or "Error" in result.output

    @patch("lakekeeper.cli._get_engine")
    def test_cleanup_database_includes_orphans(self, mock_get_engine):
        mock_engine = MagicMock()
        mock_get_engine.return_value = mock_engine
        mock_engine.list_tables.return_value = ["t1"]
        mock_engine.cleanup.return_value = 1
        mock_engine.cleanup_orphan_backups.return_value = 2

        runner = CliRunner()
        result = runner.invoke(main, ["cleanup", "--database", "mydb"])
        assert result.exit_code == 0
        assert "Cleaned 3 backup(s)" in result.output
        mock_engine.cleanup_orphan_backups.assert_called_once_with("mydb")


class TestAnalyzeWithTables:
    @patch("lakekeeper.cli._get_engine")
    def test_analyze_comma_separated_tables(self, mock_get_engine):
        from lakekeeper.models import FileFormat, TableInfo

        mock_engine = MagicMock()
        mock_get_engine.return_value = mock_engine
        mock_engine.analyze.return_value = TableInfo(
            database="mydb",
            table_name="t1",
            location="hdfs:///data",
            file_format=FileFormat.PARQUET,
        )

        runner = CliRunner()
        result = runner.invoke(main, ["analyze", "--tables", "mydb.t1,mydb.t2"])
        assert result.exit_code == 0
        assert "2 table(s)" in result.output


class TestCompactWithBackup:
    @patch("lakekeeper.cli._get_engine")
    def test_compact_full_workflow(self, mock_get_engine):
        from lakekeeper.models import BackupInfo, CompactionReport, CompactionStatus, FileFormat, TableInfo

        mock_engine = MagicMock()
        mock_get_engine.return_value = mock_engine
        mock_engine.analyze.return_value = TableInfo(
            database="mydb",
            table_name="tbl",
            location="hdfs:///data",
            file_format=FileFormat.PARQUET,
            total_file_count=65000,
            total_size_bytes=3 * 1024 * 1024 * 1024,
            needs_compaction=True,
        )
        mock_engine.create_backup.return_value = BackupInfo(
            original_table="mydb.tbl",
            backup_table="mydb.__bkp_tbl_20240101_120000",
            original_location="hdfs:///data",
            timestamp=MagicMock(),
        )
        mock_engine.compact.return_value = CompactionReport(
            table_name="mydb.tbl",
            status=CompactionStatus.COMPLETED,
            before_file_count=65000,
            after_file_count=24,
        )

        runner = CliRunner()
        result = runner.invoke(main, ["compact", "--table", "mydb.tbl"])
        assert result.exit_code == 0
        mock_engine.create_backup.assert_called_once()
        mock_engine.compact.assert_called_once()


class TestCompactErrorHandling:
    @patch("lakekeeper.cli._get_engine")
    def test_compact_backup_failure_exits_nonzero(self, mock_get_engine):
        from lakekeeper.models import FileFormat, TableInfo

        mock_engine = MagicMock()
        mock_get_engine.return_value = mock_engine
        mock_engine.analyze.return_value = TableInfo(
            database="mydb",
            table_name="tbl",
            location="hdfs:///data",
            file_format=FileFormat.PARQUET,
            total_file_count=65000,
            total_size_bytes=3 * 1024 * 1024 * 1024,
            needs_compaction=True,
        )
        mock_engine.create_backup.side_effect = RuntimeError("Metastore unavailable")

        runner = CliRunner()
        result = runner.invoke(main, ["compact", "--table", "mydb.tbl"])
        assert result.exit_code != 0
        assert "Error creating backup" in result.output
        mock_engine.compact.assert_not_called()

    @patch("lakekeeper.cli._get_engine")
    def test_compact_failed_report_exits_nonzero(self, mock_get_engine):
        from lakekeeper.models import BackupInfo, CompactionReport, CompactionStatus, FileFormat, TableInfo

        mock_engine = MagicMock()
        mock_get_engine.return_value = mock_engine
        mock_engine.analyze.return_value = TableInfo(
            database="mydb",
            table_name="tbl",
            location="hdfs:///data",
            file_format=FileFormat.PARQUET,
            total_file_count=65000,
            total_size_bytes=3 * 1024 * 1024 * 1024,
            needs_compaction=True,
        )
        mock_engine.create_backup.return_value = BackupInfo(
            original_table="mydb.tbl",
            backup_table="mydb.__bkp_tbl_20240101_120000",
            original_location="hdfs:///data",
            timestamp=MagicMock(),
        )
        mock_engine.compact.return_value = CompactionReport(
            table_name="mydb.tbl",
            status=CompactionStatus.FAILED,
            error="Row count mismatch",
        )

        runner = CliRunner()
        result = runner.invoke(main, ["compact", "--table", "mydb.tbl"])
        assert result.exit_code != 0

    @patch("lakekeeper.cli._get_engine")
    def test_compact_partial_failure_exits_nonzero(self, mock_get_engine):
        """If one table fails and another succeeds, exit code is still non-zero."""
        from lakekeeper.models import BackupInfo, CompactionReport, CompactionStatus, FileFormat, TableInfo

        mock_engine = MagicMock()
        mock_get_engine.return_value = mock_engine
        mock_engine.list_tables.return_value = ["t1", "t2"]

        good_table = TableInfo(
            database="mydb",
            table_name="t1",
            location="hdfs:///data/t1",
            file_format=FileFormat.PARQUET,
            total_file_count=100,
            needs_compaction=True,
        )
        bad_table = TableInfo(
            database="mydb",
            table_name="t2",
            location="hdfs:///data/t2",
            file_format=FileFormat.PARQUET,
            total_file_count=200,
            needs_compaction=True,
        )
        mock_engine.analyze.side_effect = [good_table, bad_table]
        mock_engine.create_backup.side_effect = [
            BackupInfo(
                original_table="mydb.t1",
                backup_table="mydb.__bkp_t1_20240101_120000",
                original_location="hdfs:///data/t1",
                timestamp=MagicMock(),
            ),
            BackupInfo(
                original_table="mydb.t2",
                backup_table="mydb.__bkp_t2_20240101_120000",
                original_location="hdfs:///data/t2",
                timestamp=MagicMock(),
            ),
        ]
        mock_engine.compact.side_effect = [
            CompactionReport(table_name="mydb.t1", status=CompactionStatus.COMPLETED),
            CompactionReport(table_name="mydb.t2", status=CompactionStatus.FAILED, error="HDFS error"),
        ]

        runner = CliRunner()
        result = runner.invoke(main, ["compact", "--database", "mydb"])
        assert result.exit_code != 0


class TestCompactWithConfigFile:
    @patch("lakekeeper.cli._get_engine")
    def test_compact_with_yaml_config(self, mock_get_engine, tmp_path):
        from lakekeeper.models import FileFormat, TableInfo

        config_file = tmp_path / "config.yaml"
        config_file.write_text("block_size_mb: 256\ndry_run: true\n")

        mock_engine = MagicMock()
        mock_get_engine.return_value = mock_engine
        mock_engine.analyze.return_value = TableInfo(
            database="mydb",
            table_name="tbl",
            location="hdfs:///data",
            file_format=FileFormat.PARQUET,
            total_file_count=100,
            total_size_bytes=1024,
            needs_compaction=True,
        )

        runner = CliRunner()
        result = runner.invoke(main, ["compact", "--table", "mydb.tbl", "--config-file", str(config_file), "--dry-run"])
        assert result.exit_code == 0
        assert "DRY RUN" in result.output


class TestMaybeSubmit:
    @patch("lakekeeper.cli._get_engine")
    def test_no_submit_when_disabled(self, mock_get_engine, tmp_path):
        """When spark_submit.enabled=False, behaves normally without calling spark-submit."""
        from lakekeeper.models import FileFormat, TableInfo

        mock_engine = MagicMock()
        mock_get_engine.return_value = mock_engine
        mock_engine.analyze.return_value = TableInfo(
            database="mydb",
            table_name="tbl",
            location="hdfs:///data",
            file_format=FileFormat.PARQUET,
            total_file_count=5,
            total_size_bytes=1024,
        )
        config_file = tmp_path / "config.yaml"
        config_file.write_text("spark_submit:\n  enabled: false\n")

        runner = CliRunner()
        with patch("subprocess.run") as mock_run:
            result = runner.invoke(main, ["analyze", "--table", "mydb.tbl", "--config-file", str(config_file)])
        assert result.exit_code == 0
        mock_run.assert_not_called()

    @patch("lakekeeper.cli._get_engine")
    def test_submit_launches_spark_submit(self, mock_get_engine, tmp_path):
        """When spark_submit.enabled=True and not already submitted, launches spark-submit."""
        config_file = tmp_path / "config.yaml"
        config_file.write_text(
            "spark_submit:\n  enabled: true\n  master: yarn\n  queue: my-queue\n  script_path: /opt/run_lakekeeper.py\n"
        )
        runner = CliRunner()
        with patch("subprocess.run") as mock_run, patch("sys.exit") as mock_exit:
            mock_run.return_value = MagicMock(returncode=0)
            runner.invoke(
                main,
                ["compact", "--database", "mydb", "--config-file", str(config_file)],
                env={"LAKEKEEPER_SUBMITTED": ""},
                catch_exceptions=False,
            )
        mock_run.assert_called_once()
        cmd = mock_run.call_args[0][0]
        assert "spark-submit" in cmd
        assert "--master" in cmd
        assert "yarn" in cmd
        assert "spark.yarn.queue=my-queue" in cmd
        assert "/opt/run_lakekeeper.py" in cmd
        mock_exit.assert_any_call(0)

    @patch("lakekeeper.cli._get_engine")
    def test_no_submit_when_already_submitted(self, mock_get_engine, tmp_path):
        """When LAKEKEEPER_SUBMITTED=1 is set, does not re-launch spark-submit."""
        from lakekeeper.models import FileFormat, TableInfo

        mock_engine = MagicMock()
        mock_get_engine.return_value = mock_engine
        mock_engine.analyze.return_value = TableInfo(
            database="mydb",
            table_name="tbl",
            location="hdfs:///data",
            file_format=FileFormat.PARQUET,
            total_file_count=5,
            total_size_bytes=1024,
        )
        config_file = tmp_path / "config.yaml"
        config_file.write_text("spark_submit:\n  enabled: true\n")

        runner = CliRunner()
        with patch("subprocess.run") as mock_run:
            result = runner.invoke(
                main,
                ["analyze", "--table", "mydb.tbl", "--config-file", str(config_file)],
                env={"LAKEKEEPER_SUBMITTED": "1"},
            )
        assert result.exit_code == 0
        mock_run.assert_not_called()


class TestConfigFileGroupLevel:
    @patch("lakekeeper.cli._get_engine")
    def test_config_file_before_subcommand(self, mock_get_engine, tmp_path):
        """--config-file before the subcommand name must be accepted."""
        from lakekeeper.models import FileFormat, TableInfo

        config_file = tmp_path / "config.yaml"
        config_file.write_text("block_size_mb: 256\n")

        mock_engine = MagicMock()
        mock_get_engine.return_value = mock_engine
        mock_engine.analyze.return_value = TableInfo(
            database="mydb",
            table_name="tbl",
            location="hdfs:///data",
            file_format=FileFormat.PARQUET,
        )

        runner = CliRunner()
        result = runner.invoke(main, ["--config-file", str(config_file), "analyze", "--table", "mydb.tbl"])
        assert result.exit_code == 0


class TestSkipTableError:
    @patch("lakekeeper.cli._get_engine")
    def test_analyze_skips_non_external(self, mock_get_engine):
        """A SkipTableError from the engine is caught and the table is skipped gracefully."""
        from lakekeeper.models import SkipTableError

        mock_engine = MagicMock()
        mock_get_engine.return_value = mock_engine
        mock_engine.analyze.side_effect = SkipTableError("not an external table (Table Type: MANAGED_TABLE)")

        runner = CliRunner()
        result = runner.invoke(main, ["analyze", "--table", "mydb.managed"])
        assert result.exit_code == 0
        assert "Skipping" in result.output

    @patch("lakekeeper.cli._get_engine")
    def test_compact_skips_non_external(self, mock_get_engine):
        """compact also skips non-external tables without error."""
        from lakekeeper.models import SkipTableError

        mock_engine = MagicMock()
        mock_get_engine.return_value = mock_engine
        mock_engine.analyze.side_effect = SkipTableError("not an external table (Table Type: MANAGED_TABLE)")

        runner = CliRunner()
        result = runner.invoke(main, ["compact", "--table", "mydb.managed"])
        assert result.exit_code == 0
        assert "Skipping" in result.output
        mock_engine.create_backup.assert_not_called()


class TestResolveTablesErrors:
    @patch("lakekeeper.cli._get_engine")
    def test_invalid_table_format(self, mock_get_engine):
        mock_get_engine.return_value = MagicMock()
        runner = CliRunner()
        result = runner.invoke(main, ["analyze", "--table", "no_dot_table"])
        assert result.exit_code != 0 or "Error" in result.output

    @patch("lakekeeper.cli._get_engine")
    def test_invalid_tables_format(self, mock_get_engine):
        mock_get_engine.return_value = MagicMock()
        runner = CliRunner()
        result = runner.invoke(main, ["analyze", "--tables", "mydb.t1,bad_table"])
        assert result.exit_code != 0 or "Error" in result.output
