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

    def test_analyze_iceberg_table_raises(self, analyzer, mock_spark):
        """Iceberg EXTERNAL tables must be rejected — the rename-swap would corrupt snapshot metadata."""
        from lakekeeper.models import SkipTableError

        desc_rows = [
            _make_row("Location", "hdfs:///data/mydb/iceberg_tbl", None),
            _make_row("Table Type", "EXTERNAL_TABLE", None),
            _make_row("InputFormat", "org.apache.iceberg.mr.hive.HiveIcebergInputFormat", None),
            _make_row("SerDe Library", "org.apache.iceberg.mr.hive.HiveIcebergSerDe", None),
        ]
        mock_spark.sql.return_value.collect.return_value = desc_rows

        with pytest.raises(SkipTableError, match="Iceberg table detected"):
            analyzer.analyze_table("mydb", "iceberg_tbl")

    def test_analyze_iceberg_detected_via_serde(self, analyzer, mock_spark):
        """Iceberg detection falls back to SerDe Library when InputFormat is absent."""
        from lakekeeper.models import SkipTableError

        desc_rows = [
            _make_row("Location", "hdfs:///data/mydb/iceberg_tbl", None),
            _make_row("Table Type", "EXTERNAL_TABLE", None),
            _make_row("SerDe Library", "org.apache.iceberg.mr.hive.HiveIcebergSerDe", None),
        ]
        mock_spark.sql.return_value.collect.return_value = desc_rows

        with pytest.raises(SkipTableError, match="Iceberg table detected"):
            analyzer.analyze_table("mydb", "iceberg_tbl")

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

    def test_compression_codec_detected_from_parquet_property(self, analyzer, mock_spark, mock_hdfs_client):
        """parquet.compression in TBLPROPERTIES is stored on TableInfo as lowercase."""
        desc_rows = [
            _make_row("Location", "hdfs:///data/mydb/tbl", None),
            _make_row("Table Type", "EXTERNAL_TABLE", None),
            _make_row("InputFormat", "org.apache.hadoop.hive.ql.io.parquet.MapRedParquetInputFormat", None),
            _make_row("parquet.compression", "GZIP", None),
        ]
        mock_spark.sql.return_value.collect.return_value = desc_rows
        mock_hdfs_client.get_file_info.return_value = HdfsFileInfo(file_count=10, total_size_bytes=1024)

        result = analyzer.analyze_table("mydb", "tbl")
        assert result.compression_codec == "gzip"

    def test_compression_codec_detected_from_orc_property(self, analyzer, mock_spark, mock_hdfs_client):
        """orc.compress in TBLPROPERTIES is stored on TableInfo as lowercase."""
        desc_rows = [
            _make_row("Location", "hdfs:///data/mydb/tbl", None),
            _make_row("Table Type", "EXTERNAL_TABLE", None),
            _make_row("InputFormat", "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat", None),
            _make_row("orc.compress", "ZLIB", None),
        ]
        mock_spark.sql.return_value.collect.return_value = desc_rows
        mock_hdfs_client.get_file_info.return_value = HdfsFileInfo(file_count=10, total_size_bytes=1024)

        result = analyzer.analyze_table("mydb", "tbl")
        assert result.compression_codec == "zlib"

    def test_compression_codec_none_when_absent(self, analyzer, mock_spark, mock_hdfs_client):
        """When no compression property is in TBLPROPERTIES, codec is None (Spark uses its default)."""
        desc_rows = [
            _make_row("Location", "hdfs:///data/mydb/tbl", None),
            _make_row("Table Type", "EXTERNAL_TABLE", None),
            _make_row("InputFormat", "org.apache.hadoop.hive.ql.io.parquet.MapRedParquetInputFormat", None),
        ]
        mock_spark.sql.return_value.collect.return_value = desc_rows
        mock_hdfs_client.get_file_info.return_value = HdfsFileInfo(file_count=10, total_size_bytes=1024)

        result = analyzer.analyze_table("mydb", "tbl")
        assert result.compression_codec is None

    def test_two_level_partition_detected_from_describe(self, analyzer, mock_spark, mock_hdfs_client):
        """Two-level partition (date+ref) detected correctly from DESCRIBE FORMATTED."""
        desc_rows = [
            _make_row("Location", "hdfs:///data/mydb/events_2p", None),
            _make_row("InputFormat", "org.apache.hadoop.hive.ql.io.parquet.MapRedParquetInputFormat", None),
            _make_row("# Partition Information", None, None),
            _make_row("# col_name", "data_type", "comment"),
            _make_row("date", "string", ""),
            _make_row("ref", "string", ""),
            _make_row("# Another Section", None, None),
        ]
        partition_rows = [
            MagicMock(__getitem__=lambda self, i, v=("date=2024-01-01/ref=A",): v[i]),
        ]
        partition_desc = [
            _make_row("Location", "hdfs:///data/mydb/events_2p/date=2024-01-01/ref=A", None),
            _make_row("InputFormat", "org.apache.hadoop.hive.ql.io.parquet.MapRedParquetInputFormat", None),
        ]
        mock_spark.sql.return_value.collect.side_effect = [
            desc_rows,
            partition_rows,  # SHOW PARTITIONS
            partition_desc,  # DESCRIBE FORMATTED ... PARTITION(...)
        ]
        mock_hdfs_client.get_file_info.return_value = HdfsFileInfo(file_count=100, total_size_bytes=100 * 1024 * 1024)
        result = analyzer.analyze_table("mydb", "events_2p")
        assert result.is_partitioned is True
        assert result.partition_columns == ["date", "ref"]
        assert len(result.partitions) == 1

    def test_partition_detection_fallback_via_show_partitions(self, analyzer, mock_spark, mock_hdfs_client):
        """When DESCRIBE FORMATTED omits partition info, SHOW PARTITIONS is used as fallback."""
        desc_rows = [
            _make_row("Location", "hdfs:///data/mydb/events_2p", None),
            _make_row("Table Type", "EXTERNAL_TABLE", None),
            _make_row("InputFormat", "org.apache.hadoop.hive.ql.io.parquet.MapRedParquetInputFormat", None),
            # Deliberately NO "# Partition Information" section
        ]
        partition_rows = [
            MagicMock(__getitem__=lambda self, i, v=("date=2024-01-01/ref=A",): v[i]),
            MagicMock(__getitem__=lambda self, i, v=("date=2024-01-01/ref=B",): v[i]),
        ]
        partition_desc_a = [
            _make_row("Location", "hdfs:///data/mydb/events_2p/date=2024-01-01/ref=A", None),
            _make_row("InputFormat", "org.apache.hadoop.hive.ql.io.parquet.MapRedParquetInputFormat", None),
        ]
        partition_desc_b = [
            _make_row("Location", "hdfs:///data/mydb/events_2p/date=2024-01-01/ref=B", None),
            _make_row("InputFormat", "org.apache.hadoop.hive.ql.io.parquet.MapRedParquetInputFormat", None),
        ]
        # Call sequence (SHOW PARTITIONS called once — rows reused for _analyze_partitions):
        # 1. DESCRIBE FORMATTED           (main)
        # 2. SHOW PARTITIONS              (fallback detection + _analyze_partitions)
        # 3. DESCRIBE FORMATTED PARTITION (ref=A location)
        # 4. DESCRIBE FORMATTED PARTITION (ref=B location)
        mock_spark.sql.return_value.collect.side_effect = [
            desc_rows,
            partition_rows,  # fallback — reused for _analyze_partitions
            partition_desc_a,
            partition_desc_b,
        ]
        mock_hdfs_client.get_file_info.side_effect = [
            HdfsFileInfo(file_count=50, total_size_bytes=50 * 1024 * 1024),
            HdfsFileInfo(file_count=50, total_size_bytes=50 * 1024 * 1024),
        ]
        result = analyzer.analyze_table("mydb", "events_2p")
        assert result.is_partitioned is True
        assert result.partition_columns == ["date", "ref"]
        assert len(result.partitions) == 2

    def test_partition_fallback_non_partitioned_table(self, analyzer, mock_spark, mock_hdfs_client):
        """Non-partitioned table stays non-partitioned when SHOW PARTITIONS raises."""
        desc_rows = [
            _make_row("Location", "hdfs:///data/mydb/flat", None),
            _make_row("InputFormat", "org.apache.hadoop.hive.ql.io.parquet.MapRedParquetInputFormat", None),
            # No partition section
        ]
        # DESCRIBE FORMATTED returns desc_rows, then SHOW PARTITIONS raises (non-partitioned)
        mock_spark.sql.return_value.collect.side_effect = [
            desc_rows,
            Exception("SHOW PARTITIONS is not allowed on a table that is not partitioned"),
        ]
        # Actually, side_effect on collect; but the sql() call itself raises — need to mock differently
        desc_result = MagicMock()
        desc_result.collect.return_value = desc_rows
        show_partitions_result = MagicMock()
        show_partitions_result.collect.side_effect = Exception(
            "SHOW PARTITIONS is not allowed on a table that is not partitioned"
        )
        mock_spark.sql.side_effect = [desc_result, show_partitions_result]
        mock_hdfs_client.get_file_info.return_value = HdfsFileInfo(file_count=10, total_size_bytes=10 * 1024 * 1024)
        result = analyzer.analyze_table("mydb", "flat")
        assert result.is_partitioned is False

    def test_detect_partition_columns_fallback_extracts_columns(self, analyzer, mock_spark):
        """_detect_partition_columns_fallback correctly parses multi-level spec."""
        rows = [
            MagicMock(__getitem__=lambda self, i, v=("year=2024/month=01/day=15",): v[i]),
        ]
        mock_spark.sql.return_value.collect.return_value = rows
        result = analyzer._detect_partition_columns_fallback("mydb.t")
        assert result == ["year", "month", "day"]

    def test_get_partition_location_takes_first_occurrence(self, analyzer, mock_spark, mock_hdfs_client):
        """On Hive 3 / CDP, DESCRIBE FORMATTED PARTITION emits Location twice.
        The first occurrence is the partition path; the second is the table root.
        We must take the first one or all compaction renames target the table root.
        """
        desc_rows = [
            _make_row("Location", "hdfs:///data/mydb/events_2p", None),
            _make_row("InputFormat", "org.apache.hadoop.hive.ql.io.parquet.MapRedParquetInputFormat", None),
            _make_row("# Partition Information", None, None),
            _make_row("# col_name", "data_type", "comment"),
            _make_row("date", "string", ""),
            _make_row("ref", "string", ""),
            _make_row("# Another Section", None, None),
        ]
        # DESCRIBE FORMATTED PARTITION output with two Location entries:
        # 1st = partition path (correct), 2nd = table root (wrong)
        partition_desc_rows = [
            _make_row("# Detailed Partition Information", None, None),
            _make_row("Location", "hdfs:///data/mydb/events_2p/date=2024-01-01/ref=A", None),
            _make_row("# Detailed Table Information", None, None),
            _make_row("Location", "hdfs:///data/mydb/events_2p", None),  # table root — must NOT win
        ]
        partition_list = [
            MagicMock(__getitem__=lambda self, i, v=("date=2024-01-01/ref=A",): v[i]),
        ]
        mock_spark.sql.return_value.collect.side_effect = [
            desc_rows,
            partition_list,  # SHOW PARTITIONS
            partition_desc_rows,  # DESCRIBE FORMATTED ... PARTITION(...)
        ]
        mock_hdfs_client.get_file_info.return_value = HdfsFileInfo(file_count=900, total_size_bytes=50 * 1024 * 1024)
        result = analyzer.analyze_table("mydb", "events_2p")
        assert result.is_partitioned is True
        assert len(result.partitions) == 1
        # partition.location must be the partition path, NOT the table root
        assert result.partitions[0].location == "hdfs:///data/mydb/events_2p/date=2024-01-01/ref=A"

    def test_three_level_partition_detected_from_describe(self, analyzer, mock_spark, mock_hdfs_client):
        """Three-level partition (year+month+day) detected correctly from DESCRIBE FORMATTED."""
        desc_rows = [
            _make_row("Location", "hdfs:///data/mydb/logs_3p", None),
            _make_row("InputFormat", "org.apache.hadoop.hive.ql.io.parquet.MapRedParquetInputFormat", None),
            _make_row("# Partition Information", None, None),
            _make_row("# col_name", "data_type", "comment"),
            _make_row("year", "string", ""),
            _make_row("month", "string", ""),
            _make_row("day", "string", ""),
            _make_row("# Another Section", None, None),
        ]
        partition_rows = [
            MagicMock(__getitem__=lambda self, i, v=("year=2024/month=01/day=15",): v[i]),
            MagicMock(__getitem__=lambda self, i, v=("year=2024/month=01/day=16",): v[i]),
        ]
        partition_desc_a = [
            _make_row("Location", "hdfs:///data/mydb/logs_3p/year=2024/month=01/day=15", None),
            _make_row("InputFormat", "org.apache.hadoop.hive.ql.io.parquet.MapRedParquetInputFormat", None),
        ]
        partition_desc_b = [
            _make_row("Location", "hdfs:///data/mydb/logs_3p/year=2024/month=01/day=16", None),
            _make_row("InputFormat", "org.apache.hadoop.hive.ql.io.parquet.MapRedParquetInputFormat", None),
        ]
        mock_spark.sql.return_value.collect.side_effect = [
            desc_rows,
            partition_rows,  # SHOW PARTITIONS
            partition_desc_a,  # DESCRIBE FORMATTED PARTITION (day=15)
            partition_desc_b,  # DESCRIBE FORMATTED PARTITION (day=16)
        ]
        mock_hdfs_client.get_file_info.side_effect = [
            HdfsFileInfo(file_count=800, total_size_bytes=80 * 1024 * 1024),
            HdfsFileInfo(file_count=2, total_size_bytes=200 * 1024 * 1024),
        ]
        result = analyzer.analyze_table("mydb", "logs_3p")
        assert result.is_partitioned is True
        assert result.partition_columns == ["year", "month", "day"]
        assert len(result.partitions) == 2
        assert result.partitions[0].location == "hdfs:///data/mydb/logs_3p/year=2024/month=01/day=15"
        assert result.partitions[1].location == "hdfs:///data/mydb/logs_3p/year=2024/month=01/day=16"
        assert result.partitions[0].needs_compaction is True  # 800 small files
        assert result.partitions[1].needs_compaction is False  # 2 large files

    def test_four_level_partition_detected_from_describe(self, analyzer, mock_spark, mock_hdfs_client):
        """Four-level partition (year+month+day+ref) detected correctly from DESCRIBE FORMATTED."""
        desc_rows = [
            _make_row("Location", "hdfs:///data/mydb/events_4p", None),
            _make_row("InputFormat", "org.apache.hadoop.hive.ql.io.parquet.MapRedParquetInputFormat", None),
            _make_row("# Partition Information", None, None),
            _make_row("# col_name", "data_type", "comment"),
            _make_row("year", "string", ""),
            _make_row("month", "string", ""),
            _make_row("day", "string", ""),
            _make_row("ref", "string", ""),
            _make_row("# Another Section", None, None),
        ]
        partition_rows = [
            MagicMock(__getitem__=lambda self, i, v=("year=2024/month=01/day=15/ref=A",): v[i]),
            MagicMock(__getitem__=lambda self, i, v=("year=2024/month=01/day=15/ref=B",): v[i]),
            MagicMock(__getitem__=lambda self, i, v=("year=2024/month=01/day=16/ref=A",): v[i]),
        ]
        partition_desc_a = [
            _make_row("Location", "hdfs:///data/mydb/events_4p/year=2024/month=01/day=15/ref=A", None),
            _make_row("InputFormat", "org.apache.hadoop.hive.ql.io.parquet.MapRedParquetInputFormat", None),
        ]
        partition_desc_b = [
            _make_row("Location", "hdfs:///data/mydb/events_4p/year=2024/month=01/day=15/ref=B", None),
            _make_row("InputFormat", "org.apache.hadoop.hive.ql.io.parquet.MapRedParquetInputFormat", None),
        ]
        partition_desc_c = [
            _make_row("Location", "hdfs:///data/mydb/events_4p/year=2024/month=01/day=16/ref=A", None),
            _make_row("InputFormat", "org.apache.hadoop.hive.ql.io.parquet.MapRedParquetInputFormat", None),
        ]
        mock_spark.sql.return_value.collect.side_effect = [
            desc_rows,
            partition_rows,  # SHOW PARTITIONS
            partition_desc_a,
            partition_desc_b,
            partition_desc_c,
        ]
        mock_hdfs_client.get_file_info.return_value = HdfsFileInfo(file_count=10, total_size_bytes=100 * 1024 * 1024)
        result = analyzer.analyze_table("mydb", "events_4p")
        assert result.is_partitioned is True
        assert result.partition_columns == ["year", "month", "day", "ref"]
        assert len(result.partitions) == 3
        assert result.partitions[0].location == "hdfs:///data/mydb/events_4p/year=2024/month=01/day=15/ref=A"
        assert result.partitions[1].location == "hdfs:///data/mydb/events_4p/year=2024/month=01/day=15/ref=B"
        assert result.partitions[2].location == "hdfs:///data/mydb/events_4p/year=2024/month=01/day=16/ref=A"

    def test_three_level_partition_fallback_via_show_partitions(self, analyzer, mock_spark, mock_hdfs_client):
        """3-level partition detected via SHOW PARTITIONS when DESCRIBE FORMATTED omits section."""
        desc_rows = [
            _make_row("Location", "hdfs:///data/mydb/logs_3p", None),
            _make_row("Table Type", "EXTERNAL_TABLE", None),
            _make_row("InputFormat", "org.apache.hadoop.hive.ql.io.parquet.MapRedParquetInputFormat", None),
            # Deliberately NO "# Partition Information" section
        ]
        partition_rows = [
            MagicMock(__getitem__=lambda self, i, v=("year=2024/month=01/day=15",): v[i]),
            MagicMock(__getitem__=lambda self, i, v=("year=2024/month=02/day=01",): v[i]),
        ]
        partition_desc_a = [
            _make_row("Location", "hdfs:///data/mydb/logs_3p/year=2024/month=01/day=15", None),
            _make_row("InputFormat", "org.apache.hadoop.hive.ql.io.parquet.MapRedParquetInputFormat", None),
        ]
        partition_desc_b = [
            _make_row("Location", "hdfs:///data/mydb/logs_3p/year=2024/month=02/day=01", None),
            _make_row("InputFormat", "org.apache.hadoop.hive.ql.io.parquet.MapRedParquetInputFormat", None),
        ]
        # Call sequence (SHOW PARTITIONS called once — rows reused for _analyze_partitions):
        # 1. DESCRIBE FORMATTED   (main)
        # 2. SHOW PARTITIONS      (fallback detection + _analyze_partitions)
        # 3. DESCRIBE FORMATTED PARTITION (day=15)
        # 4. DESCRIBE FORMATTED PARTITION (day=01)
        mock_spark.sql.return_value.collect.side_effect = [
            desc_rows,
            partition_rows,  # fallback — reused for _analyze_partitions
            partition_desc_a,
            partition_desc_b,
        ]
        mock_hdfs_client.get_file_info.side_effect = [
            HdfsFileInfo(file_count=500, total_size_bytes=50 * 1024 * 1024),
            HdfsFileInfo(file_count=500, total_size_bytes=50 * 1024 * 1024),
        ]
        result = analyzer.analyze_table("mydb", "logs_3p")
        assert result.is_partitioned is True
        assert result.partition_columns == ["year", "month", "day"]
        assert len(result.partitions) == 2

    def test_four_level_partition_fallback_via_show_partitions(self, analyzer, mock_spark, mock_hdfs_client):
        """4-level partition detected via SHOW PARTITIONS when DESCRIBE FORMATTED omits section."""
        desc_rows = [
            _make_row("Location", "hdfs:///data/mydb/events_4p", None),
            _make_row("Table Type", "EXTERNAL_TABLE", None),
            _make_row("InputFormat", "org.apache.hadoop.hive.ql.io.parquet.MapRedParquetInputFormat", None),
            # No partition section
        ]
        partition_rows = [
            MagicMock(__getitem__=lambda self, i, v=("year=2024/month=01/day=15/ref=A",): v[i]),
        ]
        partition_desc = [
            _make_row("Location", "hdfs:///data/mydb/events_4p/year=2024/month=01/day=15/ref=A", None),
            _make_row("InputFormat", "org.apache.hadoop.hive.ql.io.parquet.MapRedParquetInputFormat", None),
        ]
        # SHOW PARTITIONS called once — rows reused for _analyze_partitions
        mock_spark.sql.return_value.collect.side_effect = [
            desc_rows,
            partition_rows,  # fallback — reused for _analyze_partitions
            partition_desc,
        ]
        mock_hdfs_client.get_file_info.return_value = HdfsFileInfo(file_count=50, total_size_bytes=50 * 1024 * 1024)
        result = analyzer.analyze_table("mydb", "events_4p")
        assert result.is_partitioned is True
        assert result.partition_columns == ["year", "month", "day", "ref"]
        assert len(result.partitions) == 1

    def test_first_occurrence_location_three_level_partition(self, analyzer, mock_spark, mock_hdfs_client):
        """First-occurrence Location fix works correctly for a 3-level partition.

        DESCRIBE FORMATTED PARTITION emits Location twice on Hive 3 / CDP:
        first = partition path (correct), second = table root (wrong).
        Verify we return the partition path for year/month/day partitions.
        """
        desc_rows = [
            _make_row("Location", "hdfs:///data/mydb/logs_3p", None),
            _make_row("InputFormat", "org.apache.hadoop.hive.ql.io.parquet.MapRedParquetInputFormat", None),
            _make_row("# Partition Information", None, None),
            _make_row("# col_name", "data_type", "comment"),
            _make_row("year", "string", ""),
            _make_row("month", "string", ""),
            _make_row("day", "string", ""),
            _make_row("# Another Section", None, None),
        ]
        partition_list = [
            MagicMock(__getitem__=lambda self, i, v=("year=2024/month=01/day=15",): v[i]),
        ]
        # Dual-Location DESCRIBE FORMATTED PARTITION output (CDP Hive 3 behaviour)
        partition_desc_rows = [
            _make_row("# Detailed Partition Information", None, None),
            _make_row("Location", "hdfs:///data/mydb/logs_3p/year=2024/month=01/day=15", None),
            _make_row("# Detailed Table Information", None, None),
            _make_row("Location", "hdfs:///data/mydb/logs_3p", None),  # table root — must NOT win
        ]
        mock_spark.sql.return_value.collect.side_effect = [
            desc_rows,
            partition_list,  # SHOW PARTITIONS
            partition_desc_rows,  # DESCRIBE FORMATTED PARTITION
        ]
        mock_hdfs_client.get_file_info.return_value = HdfsFileInfo(file_count=600, total_size_bytes=60 * 1024 * 1024)
        result = analyzer.analyze_table("mydb", "logs_3p")
        assert result.is_partitioned is True
        assert len(result.partitions) == 1
        assert result.partitions[0].location == "hdfs:///data/mydb/logs_3p/year=2024/month=01/day=15"

    def test_first_occurrence_location_four_level_partition(self, analyzer, mock_spark, mock_hdfs_client):
        """First-occurrence Location fix works correctly for a 4-level partition.

        Verifies that when DESCRIBE FORMATTED PARTITION emits Location twice,
        we take the partition-specific path (deeper) and not the table root.
        """
        desc_rows = [
            _make_row("Location", "hdfs:///data/mydb/events_4p", None),
            _make_row("InputFormat", "org.apache.hadoop.hive.ql.io.parquet.MapRedParquetInputFormat", None),
            _make_row("# Partition Information", None, None),
            _make_row("# col_name", "data_type", "comment"),
            _make_row("year", "string", ""),
            _make_row("month", "string", ""),
            _make_row("day", "string", ""),
            _make_row("ref", "string", ""),
            _make_row("# Another Section", None, None),
        ]
        partition_list = [
            MagicMock(__getitem__=lambda self, i, v=("year=2024/month=01/day=15/ref=A",): v[i]),
            MagicMock(__getitem__=lambda self, i, v=("year=2024/month=01/day=15/ref=B",): v[i]),
        ]
        partition_desc_a = [
            _make_row("# Detailed Partition Information", None, None),
            _make_row("Location", "hdfs:///data/mydb/events_4p/year=2024/month=01/day=15/ref=A", None),
            _make_row("# Detailed Table Information", None, None),
            _make_row("Location", "hdfs:///data/mydb/events_4p", None),  # table root — must NOT win
        ]
        partition_desc_b = [
            _make_row("# Detailed Partition Information", None, None),
            _make_row("Location", "hdfs:///data/mydb/events_4p/year=2024/month=01/day=15/ref=B", None),
            _make_row("# Detailed Table Information", None, None),
            _make_row("Location", "hdfs:///data/mydb/events_4p", None),  # table root — must NOT win
        ]
        mock_spark.sql.return_value.collect.side_effect = [
            desc_rows,
            partition_list,  # SHOW PARTITIONS
            partition_desc_a,
            partition_desc_b,
        ]
        mock_hdfs_client.get_file_info.return_value = HdfsFileInfo(file_count=10, total_size_bytes=100 * 1024 * 1024)
        result = analyzer.analyze_table("mydb", "events_4p")
        assert result.is_partitioned is True
        assert len(result.partitions) == 2
        assert result.partitions[0].location == "hdfs:///data/mydb/events_4p/year=2024/month=01/day=15/ref=A"
        assert result.partitions[1].location == "hdfs:///data/mydb/events_4p/year=2024/month=01/day=15/ref=B"

    def test_empty_partition_skipped(self, analyzer, mock_spark, mock_hdfs_client):
        """A partition with 0 files must never be marked for compaction.

        An empty partition has nothing to coalesce. Compacting it would
        perform a pointless rename-swap and could confuse downstream tools.
        """
        desc_rows = [
            _make_row("Location", "hdfs:///data/mydb/events", None),
            _make_row("InputFormat", "org.apache.hadoop.hive.ql.io.parquet.MapRedParquetInputFormat", None),
            _make_row("# Partition Information", None, None),
            _make_row("# col_name", "data_type", "comment"),
            _make_row("date", "string", ""),
            _make_row("# Another Section", None, None),
        ]
        partition_rows = [
            MagicMock(__getitem__=lambda self, i, v=("date=2024-01-01",): v[i]),
            MagicMock(__getitem__=lambda self, i, v=("date=2024-01-02",): v[i]),
        ]
        partition_desc_a = [
            _make_row("Location", "hdfs:///data/mydb/events/date=2024-01-01", None),
            _make_row("InputFormat", "org.apache.hadoop.hive.ql.io.parquet.MapRedParquetInputFormat", None),
        ]
        partition_desc_b = [
            _make_row("Location", "hdfs:///data/mydb/events/date=2024-01-02", None),
            _make_row("InputFormat", "org.apache.hadoop.hive.ql.io.parquet.MapRedParquetInputFormat", None),
        ]
        mock_spark.sql.return_value.collect.side_effect = [
            desc_rows,
            partition_rows,
            partition_desc_a,
            partition_desc_b,
        ]
        # First partition: empty; second: 200 small files
        mock_hdfs_client.get_file_info.side_effect = [
            HdfsFileInfo(file_count=0, total_size_bytes=0),  # empty — must be skipped
            HdfsFileInfo(file_count=200, total_size_bytes=2 * 1024 * 1024),  # needs compaction
        ]
        result = analyzer.analyze_table("mydb", "events")
        assert len(result.partitions) == 2
        assert result.partitions[0].needs_compaction is False, "Empty partition must not need compaction"
        assert result.partitions[0].file_count == 0
        assert result.partitions[1].needs_compaction is True
        # Table-level totals exclude empty partitions
        assert result.total_file_count == 200
        assert result.total_size_bytes == 2 * 1024 * 1024

    def test_single_file_partition_not_compacted(self, analyzer, mock_spark, mock_hdfs_client):
        """A partition with exactly 1 file must never be marked for compaction.

        Even if that file is smaller than the threshold, coalesce(1 → 1)
        is a no-op rename-swap with no benefit.
        """
        desc_rows = [
            _make_row("Location", "hdfs:///data/mydb/events", None),
            _make_row("InputFormat", "org.apache.hadoop.hive.ql.io.parquet.MapRedParquetInputFormat", None),
            _make_row("# Partition Information", None, None),
            _make_row("# col_name", "data_type", "comment"),
            _make_row("date", "string", ""),
            _make_row("# Another Section", None, None),
        ]
        partition_rows = [
            MagicMock(__getitem__=lambda self, i, v=("date=2024-01-01",): v[i]),
            MagicMock(__getitem__=lambda self, i, v=("date=2024-01-02",): v[i]),
        ]
        partition_desc_a = [
            _make_row("Location", "hdfs:///data/mydb/events/date=2024-01-01", None),
            _make_row("InputFormat", "org.apache.hadoop.hive.ql.io.parquet.MapRedParquetInputFormat", None),
        ]
        partition_desc_b = [
            _make_row("Location", "hdfs:///data/mydb/events/date=2024-01-02", None),
            _make_row("InputFormat", "org.apache.hadoop.hive.ql.io.parquet.MapRedParquetInputFormat", None),
        ]
        mock_spark.sql.return_value.collect.side_effect = [
            desc_rows,
            partition_rows,
            partition_desc_a,
            partition_desc_b,
        ]
        # Both partitions have 1 file, one tiny and one large
        mock_hdfs_client.get_file_info.side_effect = [
            HdfsFileInfo(file_count=1, total_size_bytes=5 * 1024),  # 5 KB — tiny but 1 file
            HdfsFileInfo(file_count=1, total_size_bytes=500 * 1024 * 1024),  # 500 MB — large, 1 file
        ]
        result = analyzer.analyze_table("mydb", "events")
        assert len(result.partitions) == 2
        assert result.partitions[0].needs_compaction is False, (
            "Single tiny file must not trigger compaction (no files to merge with)"
        )
        assert result.partitions[1].needs_compaction is False, "Single large file must not trigger compaction"
        assert result.needs_compaction is False


class TestParsePartitionSpec:
    def test_single_partition(self):
        result = TableAnalyzer._parse_partition_spec("year=2024")
        assert result == {"year": "2024"}

    def test_multiple_partitions(self):
        result = TableAnalyzer._parse_partition_spec("year=2024/month=01")
        assert result == {"year": "2024", "month": "01"}

    def test_three_level_partition(self):
        result = TableAnalyzer._parse_partition_spec("year=2024/month=01/day=15")
        assert result == {"year": "2024", "month": "01", "day": "15"}

    def test_four_level_partition(self):
        result = TableAnalyzer._parse_partition_spec("year=2024/month=01/day=15/ref=A")
        assert result == {"year": "2024", "month": "01", "day": "15", "ref": "A"}

    def test_empty_value(self):
        result = TableAnalyzer._parse_partition_spec("key=")
        assert result == {"key": ""}


class TestDetectSortColumnsFromDdl:
    """Unit tests for TableAnalyzer._detect_sort_columns_from_ddl (static method)."""

    def test_single_sort_column(self):
        desc_map = {"Sort Columns": "[{col:event_time, order:1}]"}
        assert TableAnalyzer._detect_sort_columns_from_ddl(desc_map) == ["event_time"]

    def test_multi_sort_columns(self):
        desc_map = {"Sort Columns": "[{col:date, order:1}, {col:user_id, order:1}]"}
        assert TableAnalyzer._detect_sort_columns_from_ddl(desc_map) == ["date", "user_id"]

    def test_empty_brackets(self):
        desc_map = {"Sort Columns": "[]"}
        assert TableAnalyzer._detect_sort_columns_from_ddl(desc_map) == []

    def test_key_absent(self):
        assert TableAnalyzer._detect_sort_columns_from_ddl({}) == []

    def test_key_with_colon_suffix(self):
        """Some Hive/Spark versions emit 'Sort Columns:' with a trailing colon."""
        desc_map = {"Sort Columns:": "[{col:ts, order:0}]"}
        assert TableAnalyzer._detect_sort_columns_from_ddl(desc_map) == ["ts"]

    def test_desc_order_column_included(self):
        """Columns with order=0 (DESC) are still returned — caller decides sort direction."""
        desc_map = {"Sort Columns": "[{col:ts, order:0}]"}
        assert TableAnalyzer._detect_sort_columns_from_ddl(desc_map) == ["ts"]


class TestSortColumnsIntegration:
    """Integration tests for sort column resolution in analyze_table."""

    @pytest.fixture
    def analyzer(self, mock_spark, mock_hdfs_client, config):
        return TableAnalyzer(mock_spark, mock_hdfs_client, config)

    def _base_desc_rows(self, sort_columns_value=""):
        """Base DESCRIBE FORMATTED rows; include Sort Columns when provided."""
        rows = [
            _make_row("Location", "hdfs:///data/mydb/tbl", None),
            _make_row("Table Type", "EXTERNAL_TABLE", None),
            _make_row("InputFormat", "org.apache.hadoop.hive.ql.io.parquet.MapRedParquetInputFormat", None),
        ]
        if sort_columns_value:
            rows.append(_make_row("Sort Columns", sort_columns_value, None))
        return rows

    def test_sort_columns_detected_from_ddl(self, analyzer, mock_spark, mock_hdfs_client):
        """Sort Columns in DESCRIBE FORMATTED are populated on TableInfo."""
        desc_rows = self._base_desc_rows("[{col:event_time, order:1}]")
        mock_spark.sql.return_value.collect.return_value = desc_rows
        mock_hdfs_client.get_file_info.return_value = HdfsFileInfo(file_count=10, total_size_bytes=1024)

        result = analyzer.analyze_table("mydb", "tbl")

        assert result.sort_columns == ["event_time"]

    def test_sort_columns_multi_detected_from_ddl(self, analyzer, mock_spark, mock_hdfs_client):
        """Multiple Sort Columns in DDL are returned in order."""
        desc_rows = self._base_desc_rows("[{col:date, order:1}, {col:user_id, order:1}]")
        mock_spark.sql.return_value.collect.return_value = desc_rows
        mock_hdfs_client.get_file_info.return_value = HdfsFileInfo(file_count=10, total_size_bytes=1024)

        result = analyzer.analyze_table("mydb", "tbl")

        assert result.sort_columns == ["date", "user_id"]

    def test_sort_columns_empty_when_absent(self, analyzer, mock_spark, mock_hdfs_client):
        """When Sort Columns is absent from DESCRIBE FORMATTED, sort_columns is []."""
        desc_rows = self._base_desc_rows()  # no Sort Columns row
        mock_spark.sql.return_value.collect.return_value = desc_rows
        mock_hdfs_client.get_file_info.return_value = HdfsFileInfo(file_count=10, total_size_bytes=1024)

        result = analyzer.analyze_table("mydb", "tbl")

        assert result.sort_columns == []

    def test_config_sort_columns_overrides_ddl(self, mock_spark, mock_hdfs_client, config):
        """Config sort_columns takes priority over DDL Sort Columns."""
        config.sort_columns["mydb.tbl"] = ["date", "region"]
        analyzer = TableAnalyzer(mock_spark, mock_hdfs_client, config)

        # DDL has a different sort column
        desc_rows = self._base_desc_rows("[{col:event_time, order:1}]")
        mock_spark.sql.return_value.collect.return_value = desc_rows
        mock_hdfs_client.get_file_info.return_value = HdfsFileInfo(file_count=10, total_size_bytes=1024)

        result = analyzer.analyze_table("mydb", "tbl")

        # Config wins — DDL value is ignored
        assert result.sort_columns == ["date", "region"]

    def test_config_sort_columns_used_when_ddl_absent(self, mock_spark, mock_hdfs_client, config):
        """Config sort_columns is used when DDL has no Sort Columns."""
        config.sort_columns["mydb.tbl"] = ["ts"]
        analyzer = TableAnalyzer(mock_spark, mock_hdfs_client, config)

        desc_rows = self._base_desc_rows()  # no Sort Columns in DDL
        mock_spark.sql.return_value.collect.return_value = desc_rows
        mock_hdfs_client.get_file_info.return_value = HdfsFileInfo(file_count=10, total_size_bytes=1024)

        result = analyzer.analyze_table("mydb", "tbl")

        assert result.sort_columns == ["ts"]


class TestMedianCompactionDetection:
    """Tests for skewed file-size distribution detection using min(avg, median)."""

    @pytest.fixture
    def analyzer(self, mock_spark, mock_hdfs_client, config):
        return TableAnalyzer(mock_spark, mock_hdfs_client, config)

    def _desc_rows(self):
        return [
            _make_row("Location", "hdfs:///data/mydb/events", None),
            _make_row("Table Type", "EXTERNAL_TABLE", None),
            _make_row("InputFormat", "org.apache.hadoop.hive.ql.io.parquet.MapRedParquetInputFormat", None),
        ]

    def test_skewed_distribution_triggers_compaction(self, analyzer, mock_spark, mock_hdfs_client):
        """2 large files + 98 tiny files: avg looks OK but median is tiny → compaction triggered."""
        mock_spark.sql.return_value.collect.return_value = self._desc_rows()

        large = 2 * 1024 * 1024 * 1024  # 2 GB
        tiny = 100  # 100 bytes
        sizes = [large, large] + [tiny] * 98
        mock_hdfs_client.get_file_info.return_value = HdfsFileInfo(
            file_count=100,
            total_size_bytes=sum(sizes),
            file_sizes=sizes,
        )

        result = analyzer.analyze_table("mydb", "events")

        # avg is dominated by the two large files → would NOT trigger compaction alone
        assert result.avg_file_size_bytes > 40 * 1024 * 1024
        # but median is 100 bytes → effective_size is tiny → compaction IS needed
        assert result.median_file_size_bytes == tiny
        assert result.needs_compaction is True

    def test_uniform_distribution_below_threshold_still_compacts(self, analyzer, mock_spark, mock_hdfs_client):
        """Uniform small files: avg == median == effective → compaction triggered as before."""
        mock_spark.sql.return_value.collect.return_value = self._desc_rows()

        sizes = [5 * 1024 * 1024] * 50  # 50 × 5 MB files
        mock_hdfs_client.get_file_info.return_value = HdfsFileInfo(
            file_count=50,
            total_size_bytes=sum(sizes),
            file_sizes=sizes,
        )

        result = analyzer.analyze_table("mydb", "events")
        assert result.needs_compaction is True

    def test_uniform_distribution_above_threshold_no_compaction(self, analyzer, mock_spark, mock_hdfs_client):
        """All files already large: avg == median → no compaction."""
        mock_spark.sql.return_value.collect.return_value = self._desc_rows()

        sizes = [200 * 1024 * 1024] * 5  # 5 × 200 MB files
        mock_hdfs_client.get_file_info.return_value = HdfsFileInfo(
            file_count=5,
            total_size_bytes=sum(sizes),
            file_sizes=sizes,
        )

        result = analyzer.analyze_table("mydb", "events")
        assert result.needs_compaction is False

    def test_median_stored_on_table_info(self, analyzer, mock_spark, mock_hdfs_client):
        """median_file_size_bytes is propagated from HdfsFileInfo to TableInfo."""
        mock_spark.sql.return_value.collect.return_value = self._desc_rows()

        sizes = [1000, 2000, 3000]
        mock_hdfs_client.get_file_info.return_value = HdfsFileInfo(
            file_count=3,
            total_size_bytes=sum(sizes),
            file_sizes=sizes,
        )

        result = analyzer.analyze_table("mydb", "events")
        assert result.median_file_size_bytes == 2000

    def test_skewed_partition_triggers_compaction(self, analyzer, mock_spark, mock_hdfs_client, config):
        """Partitioned table: skewed partition detected via effective_file_size_bytes."""
        desc_rows = [
            _make_row("Location", "hdfs:///data/mydb/logs", None),
            _make_row("Table Type", "EXTERNAL_TABLE", None),
            _make_row("InputFormat", "org.apache.hadoop.hive.ql.io.parquet.MapRedParquetInputFormat", None),
            _make_row("# Partition Information", None, None),
            _make_row("# col_name", "data_type", "comment"),
            _make_row("date", "string", ""),
            _make_row("# Another Section", None, None),
        ]
        partition_rows = [
            MagicMock(__getitem__=lambda self, i, v=("date=2024-01-01",): v[i]),
        ]
        partition_desc = [
            _make_row("Location", "hdfs:///data/mydb/logs/date=2024-01-01", None),
            _make_row("InputFormat", "org.apache.hadoop.hive.ql.io.parquet.MapRedParquetInputFormat", None),
        ]
        mock_spark.sql.return_value.collect.side_effect = [
            desc_rows,
            partition_rows,
            partition_desc,
        ]

        # Skewed: 1 large file (2 GB) + 99 tiny files (100 bytes) → avg > threshold, median tiny
        large = 2 * 1024 * 1024 * 1024
        tiny = 100
        sizes = [large] + [tiny] * 99
        mock_hdfs_client.get_file_info.return_value = HdfsFileInfo(
            file_count=100,
            total_size_bytes=sum(sizes),
            file_sizes=sizes,
        )

        result = analyzer.analyze_table("mydb", "logs")
        assert result.is_partitioned is True
        assert result.partitions[0].needs_compaction is True
