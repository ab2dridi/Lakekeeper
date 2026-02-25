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

        # Iceberg tables are EXTERNAL in the Metastore but use a completely different
        # storage layout (metadata JSON + manifest files).  The HDFS rename-swap used
        # by Lakekeeper would corrupt the Iceberg snapshot chain.  Detect via
        # InputFormat / SerDe Library and skip them explicitly.
        input_format = desc_map.get("InputFormat", desc_map.get("InputFormat:", "")).lower()
        serde = desc_map.get("SerDe Library", desc_map.get("SerDe Library:", "")).lower()
        if "iceberg" in input_format or "iceberg" in serde:
            raise SkipTableError(
                "Iceberg table detected. Lakekeeper only supports Hive-native EXTERNAL "
                "tables (Parquet/ORC). Use Iceberg's built-in compaction instead."
            )

        location = self._extract_location(desc_map)
        file_format = self._detect_format(desc_map)
        partition_columns = self._detect_partition_columns(desc_rows)
        compression_codec = self._detect_compression(desc_map, file_format)
        sort_columns = self._resolve_sort_columns(full_name, desc_map)

        table_info = TableInfo(
            database=database,
            table_name=table_name,
            location=location,
            file_format=file_format,
            is_partitioned=len(partition_columns) > 0,
            partition_columns=partition_columns,
            compression_codec=compression_codec,
            sort_columns=sort_columns,
        )

        if table_info.is_partitioned:
            self._analyze_partitions(table_info)
        else:
            # Safety net: some Hive 3 / SparkSQL 3.x versions omit the
            # '# Partition Information' block from DESCRIBE FORMATTED output.
            # SHOW PARTITIONS raises AnalysisException on non-partitioned tables,
            # so catching that exception is a reliable disambiguation.
            # Fetch the rows once and reuse them in _analyze_partitions to avoid
            # a redundant second round-trip to the Metastore.
            try:
                fallback_rows = self._spark.sql(f"SHOW PARTITIONS {full_name}").collect()
            except Exception:
                fallback_rows = []

            if fallback_rows:
                first_spec = fallback_rows[0][0]
                fallback_cols = [part.split("=")[0] for part in first_spec.split("/") if "=" in part]
            else:
                fallback_cols = []

            if fallback_cols:
                logger.warning(
                    "Partition columns for %s not found in DESCRIBE FORMATTED output; "
                    "detected via SHOW PARTITIONS fallback: %s. "
                    "Treating as a partitioned table.",
                    full_name,
                    fallback_cols,
                )
                table_info.is_partitioned = True
                table_info.partition_columns = fallback_cols
                self._analyze_partitions(table_info, partition_rows=fallback_rows)
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

    def _detect_partition_columns_fallback(self, full_name: str) -> list[str]:
        """Detect partition columns via SHOW PARTITIONS when DESCRIBE FORMATTED misses them.

        On some Hive 3 / SparkSQL 3.x combinations the '# Partition Information' block
        is absent from DESCRIBE FORMATTED output.  SHOW PARTITIONS raises an
        AnalysisException for non-partitioned tables, so the try/except reliably
        distinguishes the two cases.

        Returns column names in partition order, e.g. ['date', 'ref'], or [] if the
        table is not partitioned or the query fails for any other reason.
        """
        try:
            rows = self._spark.sql(f"SHOW PARTITIONS {full_name}").collect()
            if rows:
                # Spec format: "date=2024-01-01/ref=A"
                first_spec = rows[0][0]
                cols = [part.split("=")[0] for part in first_spec.split("/") if "=" in part]
                if cols:
                    return cols
        except Exception:
            logger.debug("SHOW PARTITIONS failed for %s — table is not partitioned", full_name)
        return []

    def _analyze_non_partitioned(self, table_info: TableInfo) -> None:
        """Analyze a non-partitioned table."""
        file_info = self._hdfs.get_file_info(table_info.location)
        table_info.total_file_count = file_info.file_count
        table_info.total_size_bytes = file_info.total_size_bytes

    def _analyze_partitions(self, table_info: TableInfo, partition_rows: list | None = None) -> None:
        """Analyze all partitions of a partitioned table.

        Args:
            table_info: Table information to populate.
            partition_rows: Pre-fetched SHOW PARTITIONS rows. If None, the query
                is issued here. Passing pre-fetched rows avoids a redundant
                Metastore round-trip when the caller already ran SHOW PARTITIONS
                for partition-column detection (fallback path).
        """
        full_name = table_info.full_name
        if partition_rows is None:
            partition_rows = self._spark.sql(f"SHOW PARTITIONS {full_name}").collect()

        total_files = 0
        total_size = 0

        for row in partition_rows:
            partition_spec_str = row[0]
            spec = self._parse_partition_spec(partition_spec_str)

            partition_location = self._get_partition_location(full_name, spec)
            file_info = self._hdfs.get_file_info(partition_location)

            if file_info.file_count == 0:
                # Empty partition — nothing to compact; skip without logging noise.
                logger.debug("Partition %s of %s is empty, skipping.", partition_spec_str, full_name)
                partition_info = PartitionInfo(
                    spec=spec,
                    location=partition_location,
                    file_count=0,
                    total_size_bytes=0,
                    needs_compaction=False,
                    target_files=0,
                )
                table_info.partitions.append(partition_info)
                continue

            target_files = max(1, math.ceil(file_info.total_size_bytes / self._config.block_size_bytes))

            # A single-file partition cannot be reduced further regardless of size:
            # coalesce(1 → 1) would be a no-op rename-swap with no benefit.
            has_small_files = file_info.avg_file_size_bytes < self._config.compaction_threshold_bytes
            needs_compaction = has_small_files and file_info.file_count > 1

            partition_info = PartitionInfo(
                spec=spec,
                location=partition_location,
                file_count=file_info.file_count,
                total_size_bytes=file_info.total_size_bytes,
                needs_compaction=needs_compaction,
                target_files=target_files,
            )

            table_info.partitions.append(partition_info)
            total_files += file_info.file_count
            total_size += file_info.total_size_bytes

        table_info.total_file_count = total_files
        table_info.total_size_bytes = total_size

    def _get_partition_location(self, full_name: str, spec: dict[str, str]) -> str:
        """Get the HDFS location of a specific partition.

        DESCRIBE FORMATTED … PARTITION(…) on Hive 3 / CDP emits the 'Location'
        field twice in its output:
          1. The partition-specific path  (under '# Detailed Partition Information')
          2. The table-level path         (under '# Detailed Table Information')

        A plain dict comprehension keeps the *last* value, returning the table
        root instead of the partition directory.  Building the map with
        first-occurrence semantics gives the correct partition location.
        """
        spec_sql = ", ".join(f"{k}='{v}'" for k, v in spec.items())
        desc = self._spark.sql(f"DESCRIBE FORMATTED {full_name} PARTITION({spec_sql})").collect()
        desc_map: dict[str, str] = {}
        for row in desc:
            if row[0]:
                key = row[0].strip()
                if key not in desc_map:
                    desc_map[key] = (row[1] or "").strip()
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

    def _detect_compression(self, desc_map: dict[str, str], file_format: FileFormat) -> str | None:
        """Detect compression codec from DESCRIBE FORMATTED table properties.

        Hive stores the codec in TBLPROPERTIES:
          - Parquet: ``parquet.compression`` (SNAPPY, GZIP, ZSTD, LZ4, UNCOMPRESSED)
          - ORC:     ``orc.compress``        (SNAPPY, ZLIB, LZ4, ZSTD, NONE)

        Returns the codec in lowercase (Spark's expected format), or None if the
        property is absent (Spark will then use its session-level default).
        """
        if file_format == FileFormat.PARQUET:
            raw = desc_map.get("parquet.compression", desc_map.get("parquet.compression:", ""))
        else:
            raw = desc_map.get("orc.compress", desc_map.get("orc.compress:", ""))

        codec = raw.strip().lower() if raw.strip() else None
        if codec:
            logger.debug("Detected compression codec from table properties: %s", codec)
        return codec

    def _resolve_sort_columns(self, full_name: str, desc_map: dict[str, str]) -> list[str]:
        """Resolve sort columns with priority: config (CLI/YAML) > DDL SORTED BY > none.

        Args:
            full_name: Fully qualified table name (db.table).
            desc_map: DESCRIBE FORMATTED key/value pairs.

        Returns:
            List of column names to sort by before coalescing, or empty list.
        """
        # Highest priority: explicit config (CLI --sort-columns injects here, overriding YAML)
        configured = self._config.sort_columns.get(full_name)
        if configured:
            logger.debug("Using configured sort columns for %s: %s", full_name, configured)
            return list(configured)

        # Fallback: DDL SORTED BY from DESCRIBE FORMATTED
        ddl_cols = self._detect_sort_columns_from_ddl(desc_map)
        if ddl_cols:
            logger.debug("Using DDL sort columns for %s: %s", full_name, ddl_cols)
        return ddl_cols

    @staticmethod
    def _detect_sort_columns_from_ddl(desc_map: dict[str, str]) -> list[str]:
        """Detect sort columns from the 'Sort Columns' field in DESCRIBE FORMATTED.

        Hive stores CLUSTERED BY … SORTED BY … DDL metadata as:
            Sort Columns:    [{col:event_time, order:1}, {col:user_id, order:1}]
        where order=1 is ASC and order=0 is DESC.

        Returns column names in DDL order, or an empty list if absent.
        """
        raw = desc_map.get("Sort Columns", desc_map.get("Sort Columns:", "")).strip()
        if not raw or raw in ("[]", ""):
            return []
        # Extract (col_name, order) pairs: "{col:event_time, order:1}"
        pairs = re.findall(r"\{col:(\w+),\s*order:(\d+)\}", raw)
        return [col for col, _order in pairs]

    def _determine_compaction_need(self, table_info: TableInfo) -> None:
        """Determine if a table needs compaction."""
        if table_info.is_partitioned:
            table_info.needs_compaction = any(p.needs_compaction for p in table_info.partitions)
        else:
            table_info.needs_compaction = table_info.avg_file_size_bytes < self._config.compaction_threshold_bytes
