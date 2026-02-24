"""Tests for lakekeeper.core.analyzer module."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from lakekeeper.core.analyzer import TableAnalyzer
from lakekeeper.models import FileFormat
from lakekeeper.utils.hdfs import HdfsFileInfo


def _make_row(col0, col1="", col2=""):
    """Create a mock Row for DESCRIBE FORMATTED output."""
    row = MagicMock()
    row.__getitem__ = lambda self, i: [col0, col1, col2][i]
    return row


class TestTableAnalyzer:
    @pytest.fixture
    def analyzer(self, mock_spark, mock_hdfs_client, config):
        return TableAnalyzer(mock_spark, mock_hdfs_client, config)

    def test_analyze_non_partitioned_parquet(self, analyzer, mock_spark, mock_hdfs_client):
        desc_rows = [
            _make_row("col1", "string", ""),
            _make_row("", None, None),
            _make_row("# Detailed Table Information", None, None),
            _make_row("Location", "hdfs:///data/mydb/events", None),
            _make_row("InputFormat", "org.apache.hadoop.hive.ql.io.parquet.MapRedParquetInputFormat", None),
        ]
        mock_spark.sql.return_value.collect.return_value = desc_rows

        mock_hdfs_client.get_file_info.return_value = HdfsFileInfo(
            file_count=65000,
            total_size_bytes=3 * 1024 * 1024 * 1024,
        )

        result = analyzer.analyze_table("mydb", "events")
        assert result.database == "mydb"
        assert result.table_name == "events"
        assert result.file_format == FileFormat.PARQUET
        assert result.is_partitioned is False
        assert result.total_file_count == 65000
        assert result.needs_compaction is True

    def test_analyze_non_partitioned_orc(self, analyzer, mock_spark, mock_hdfs_client):
        desc_rows = [
            _make_row("Location", "hdfs:///data/mydb/orc_table", None),
            _make_row("InputFormat", "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat", None),
        ]
        mock_spark.sql.return_value.collect.return_value = desc_rows

        mock_hdfs_client.get_file_info.return_value = HdfsFileInfo(
            file_count=100,
            total_size_bytes=50 * 1024 * 1024 * 1024,
        )

        result = analyzer.analyze_table("mydb", "orc_table")
        assert result.file_format == FileFormat.ORC
        assert result.needs_compaction is False  # large avg file size

    def test_analyze_partitioned_table(self, analyzer, mock_spark, mock_hdfs_client, config):
        desc_rows = [
            _make_row("Location", "hdfs:///data/mydb/logs", None),
            _make_row("InputFormat", "org.apache.hadoop.hive.ql.io.parquet.MapRedParquetInputFormat", None),
            _make_row("# Partition Information", None, None),
            _make_row("# col_name", "data_type", "comment"),
            _make_row("year", "string", ""),
            _make_row("# Another Section", None, None),
        ]

        partition_rows = [
            MagicMock(__getitem__=lambda self, i, v=("year=2024",): v[i]),
            MagicMock(__getitem__=lambda self, i, v=("year=2023",): v[i]),
        ]

        partition_desc_rows = [
            _make_row("Location", "hdfs:///data/mydb/logs/year=2024", None),
            _make_row("InputFormat", "org.apache.hadoop.hive.ql.io.parquet.MapRedParquetInputFormat", None),
        ]

        # First call: DESCRIBE FORMATTED, second: SHOW PARTITIONS, then per-partition DESCRIBE
        mock_spark.sql.return_value.collect.side_effect = [
            desc_rows,
            partition_rows,
            partition_desc_rows,
            [
                _make_row("Location", "hdfs:///data/mydb/logs/year=2023", None),
                _make_row("InputFormat", "org.apache.hadoop.hive.ql.io.parquet.MapRedParquetInputFormat", None),
            ],
        ]

        small_partition = HdfsFileInfo(file_count=5000, total_size_bytes=500 * 1024 * 1024)
        large_partition = HdfsFileInfo(file_count=2, total_size_bytes=256 * 1024 * 1024)
        mock_hdfs_client.get_file_info.side_effect = [small_partition, large_partition]

        result = analyzer.analyze_table("mydb", "logs")
        assert result.is_partitioned is True
        assert result.partition_columns == ["year"]
        assert len(result.partitions) == 2
        assert result.partitions[0].needs_compaction is True
        assert result.partitions[1].needs_compaction is False
        assert result.needs_compaction is True

    def test_analyze_missing_location(self, analyzer, mock_spark):
        desc_rows = [
            _make_row("InputFormat", "parquet", None),
        ]
        mock_spark.sql.return_value.collect.return_value = desc_rows

        with pytest.raises(ValueError, match="Could not determine table location"):
            analyzer.analyze_table("mydb", "bad_table")

    def test_analyze_unknown_format(self, analyzer, mock_spark):
        desc_rows = [
            _make_row("Location", "hdfs:///data/mydb/t", None),
            _make_row("InputFormat", "com.unknown.Format", None),
        ]
        mock_spark.sql.return_value.collect.return_value = desc_rows

        with pytest.raises(ValueError, match="Could not detect file format"):
            analyzer.analyze_table("mydb", "t")

    def test_analyze_managed_table_raises(self, analyzer, mock_spark):
        """MANAGED tables must be rejected — only EXTERNAL tables are supported."""
        from lakekeeper.models import SkipTableError

        desc_rows = [
            _make_row("Location", "hdfs:///data/mydb/managed", None),
            _make_row("Table Type", "MANAGED_TABLE", None),
            _make_row("InputFormat", "org.apache.hadoop.hive.ql.io.parquet.MapRedParquetInputFormat", None),
        ]
        mock_spark.sql.return_value.collect.return_value = desc_rows

        with pytest.raises(SkipTableError, match="not an external table"):
            analyzer.analyze_table("mydb", "managed")

    def test_analyze_external_table_not_skipped(self, analyzer, mock_spark, mock_hdfs_client):
        """EXTERNAL_TABLE must not be skipped."""
        desc_rows = [
            _make_row("Location", "hdfs:///data/mydb/ext", None),
            _make_row("Table Type", "EXTERNAL_TABLE", None),
            _make_row("InputFormat", "org.apache.hadoop.hive.ql.io.parquet.MapRedParquetInputFormat", None),
        ]
        mock_spark.sql.return_value.collect.return_value = desc_rows
        mock_hdfs_client.get_file_info.return_value = HdfsFileInfo(file_count=10, total_size_bytes=1024)

        result = analyzer.analyze_table("mydb", "ext")
        assert result.database == "mydb"

    def test_analyze_unknown_type_not_skipped(self, analyzer, mock_spark, mock_hdfs_client):
        """When Table Type is absent, do not skip (be permissive about unknown clusters)."""
        desc_rows = [
            _make_row("Location", "hdfs:///data/mydb/ext", None),
            _make_row("InputFormat", "org.apache.hadoop.hive.ql.io.parquet.MapRedParquetInputFormat", None),
        ]
        mock_spark.sql.return_value.collect.return_value = desc_rows
        mock_hdfs_client.get_file_info.return_value = HdfsFileInfo(file_count=10, total_size_bytes=1024)

        result = analyzer.analyze_table("mydb", "ext")
        assert result.database == "mydb"


class TestParsePartitionSpec:
    def test_single_partition(self):
        result = TableAnalyzer._parse_partition_spec("year=2024")
        assert result == {"year": "2024"}

    def test_multiple_partitions(self):
        result = TableAnalyzer._parse_partition_spec("year=2024/month=01")
        assert result == {"year": "2024", "month": "01"}

    def test_empty_value(self):
        result = TableAnalyzer._parse_partition_spec("key=")
        assert result == {"key": ""}
