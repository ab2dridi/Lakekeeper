"""Table analysis: format detection, partition discovery, size analysis, compaction threshold."""

from __future__ import annotations

import logging
import math
import re
from typing import TYPE_CHECKING

from lakekeeper.models import FileFormat, PartitionInfo, SkipTableError, TableInfo

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

    from lakekeeper.config import LakekeeperConfig
    from lakekeeper.utils.hdfs import HdfsClient

logger = logging.getLogger(__name__)

_FORMAT_MAP = {
    "parquet": FileFormat.PARQUET,
    "org.apache.hadoop.hive.ql.io.parquet.mapredparquetinputformat": FileFormat.PARQUET,
    "org.apache.hadoop.hive.ql.io.parquet.serde.parquethiveserde": FileFormat.PARQUET,
    "orc": FileFormat.ORC,
    "org.apache.hadoop.hive.ql.io.orc.orcinputformat": FileFormat.ORC,
    "org.apache.hadoop.hive.ql.io.orc.orcserde": FileFormat.ORC,
}


class TableAnalyzer:
    """Analyzes Hive tables to determine if compaction is needed."""

    def __init__(self, spark: SparkSession, hdfs_client: HdfsClient, config: LakekeeperConfig) -> None:
        """Initialize the analyzer.

        Args:
            spark: Active SparkSession.
            hdfs_client: HDFS client for file operations.
            config: Beekeeper configuration.
        """
        self._spark = spark
        self._hdfs = hdfs_client
        self._config = config

    def analyze_table(self, database: str, table_name: str) -> TableInfo:
        """Analyze a single table.

        Args:
            database: Database name.
            table_name: Table name.

        Returns:
            TableInfo with complete analysis.
        """
        full_name = f"{database}.{table_name}"
        logger.info("Analyzing table: %s", full_name)

        desc_rows = self._spark.sql(f"DESCRIBE FORMATTED {full_name}").collect()
        desc_map = {row[0].strip(): (row[1] or "").strip() for row in desc_rows if row[0]}

        # Only EXTERNAL tables are supported — MANAGED tables would be altered
        # by the rename swap, risking Metastore inconsistency.
        table_type = desc_map.get("Table Type", desc_map.get("Table Type:", "")).strip().upper()
        if table_type and "EXTERNAL" not in table_type:
            raise SkipTableError(
                f"not an external table (Table Type: {table_type}). "
                "Lakekeeper only supports EXTERNAL tables."
            )

        location = self._extract_location(desc_map)
        file_format = self._detect_format(desc_map)
        partition_columns = self._detect_partition_columns(desc_rows)

        table_info = TableInfo(
            database=database,
            table_name=table_name,
            location=location,
            file_format=file_format,
            is_partitioned=len(partition_columns) > 0,
            partition_columns=partition_columns,
        )

        if table_info.is_partitioned:
            self._analyze_partitions(table_info)
        else:
            self._analyze_non_partitioned(table_info)

        self._determine_compaction_need(table_info)
        logger.info(
            "Table %s: %d files, %d bytes, avg %d bytes, needs_compaction=%s",
            full_name,
            table_info.total_file_count,
            table_info.total_size_bytes,
            table_info.avg_file_size_bytes,
            table_info.needs_compaction,
        )
        return table_info

    def _extract_location(self, desc_map: dict[str, str]) -> str:
        """Extract HDFS location from DESCRIBE FORMATTED output."""
        for key in ("Location", "Location:"):
            if key in desc_map and desc_map[key]:
                return desc_map[key]
        msg = "Could not determine table location from DESCRIBE FORMATTED"
        raise ValueError(msg)

    def _detect_format(self, desc_map: dict[str, str]) -> FileFormat:
        """Detect file format from DESCRIBE FORMATTED output."""
        for key in ("InputFormat", "InputFormat:", "SerDe Library", "SerDe Library:"):
            value = desc_map.get(key, "").lower()
            if value in _FORMAT_MAP:
                return _FORMAT_MAP[value]

        msg = f"Could not detect file format. Available keys: {list(desc_map.keys())}"
        raise ValueError(msg)

    def _detect_partition_columns(self, desc_rows: list) -> list[str]:
        """Detect partition columns from DESCRIBE FORMATTED output."""
        in_partition_section = False
        partition_cols = []

        for row in desc_rows:
            col_name = (row[0] or "").strip()
            if col_name == "# Partition Information":
                in_partition_section = True
                continue
            if in_partition_section and col_name.startswith("# "):
                if col_name == "# col_name":
                    continue
                in_partition_section = False
                continue
            if in_partition_section and col_name and col_name != "# col_name":
                if not col_name.startswith("#") and col_name not in ("", " "):
                    partition_cols.append(col_name)

        return partition_cols

    def _analyze_non_partitioned(self, table_info: TableInfo) -> None:
        """Analyze a non-partitioned table."""
        file_info = self._hdfs.get_file_info(table_info.location)
        table_info.total_file_count = file_info.file_count
        table_info.total_size_bytes = file_info.total_size_bytes

    def _analyze_partitions(self, table_info: TableInfo) -> None:
        """Analyze all partitions of a partitioned table."""
        full_name = table_info.full_name
        partitions_rows = self._spark.sql(f"SHOW PARTITIONS {full_name}").collect()

        total_files = 0
        total_size = 0

        for row in partitions_rows:
            partition_spec_str = row[0]
            spec = self._parse_partition_spec(partition_spec_str)

            partition_location = self._get_partition_location(full_name, spec)
            file_info = self._hdfs.get_file_info(partition_location)

            target_files = max(1, math.ceil(file_info.total_size_bytes / self._config.block_size_bytes))

            partition_info = PartitionInfo(
                spec=spec,
                location=partition_location,
                file_count=file_info.file_count,
                total_size_bytes=file_info.total_size_bytes,
                needs_compaction=file_info.avg_file_size_bytes < self._config.compaction_threshold_bytes,
                target_files=target_files,
            )

            table_info.partitions.append(partition_info)
            total_files += file_info.file_count
            total_size += file_info.total_size_bytes

        table_info.total_file_count = total_files
        table_info.total_size_bytes = total_size

    def _get_partition_location(self, full_name: str, spec: dict[str, str]) -> str:
        """Get the HDFS location of a specific partition."""
        spec_sql = ", ".join(f"{k}='{v}'" for k, v in spec.items())
        desc = self._spark.sql(f"DESCRIBE FORMATTED {full_name} PARTITION({spec_sql})").collect()
        desc_map = {row[0].strip(): (row[1] or "").strip() for row in desc if row[0]}
        return self._extract_location(desc_map)

    @staticmethod
    def _parse_partition_spec(spec_str: str) -> dict[str, str]:
        """Parse a partition spec string like 'year=2024/month=01' into a dict."""
        result = {}
        for part in spec_str.split("/"):
            match = re.match(r"(\w+)=(.*)", part)
            if match:
                result[match.group(1)] = match.group(2)
        return result

    def _determine_compaction_need(self, table_info: TableInfo) -> None:
        """Determine if a table needs compaction."""
        if table_info.is_partitioned:
            table_info.needs_compaction = any(p.needs_compaction for p in table_info.partitions)
        else:
            table_info.needs_compaction = table_info.avg_file_size_bytes < self._config.compaction_threshold_bytes
