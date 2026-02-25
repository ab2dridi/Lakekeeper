"""Compaction orchestration - the main workflow engine."""

from __future__ import annotations

import logging
import math
import time
from typing import TYPE_CHECKING

from lakekeeper.models import CompactionReport, CompactionStatus

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

    from lakekeeper.config import LakekeeperConfig
    from lakekeeper.core.backup import BackupManager
    from lakekeeper.models import BackupInfo, PartitionInfo, TableInfo
    from lakekeeper.utils.hdfs import HdfsClient

logger = logging.getLogger(__name__)


class Compactor:
    """Orchestrates the compaction workflow for Hive external tables."""

    def __init__(
        self,
        spark: SparkSession,
        hdfs_client: HdfsClient,
        config: LakekeeperConfig,
        backup_mgr: BackupManager,
    ) -> None:
        """Initialize the compactor.

        Args:
            spark: Active SparkSession.
            hdfs_client: HDFS client for file operations.
            config: Beekeeper configuration.
            backup_mgr: Backup manager to update backup table locations.
        """
        self._spark = spark
        self._hdfs = hdfs_client
        self._config = config
        self._backup_mgr = backup_mgr
        # Rollback state: tracks in-flight renames and temp paths
        self._pending_renames: list[tuple[str, str]] = []  # (current_path, original_path)
        self._temp_paths: list[str] = []

    def compact_table(self, table_info: TableInfo, backup_info: BackupInfo) -> CompactionReport:
        """Compact a table (partitioned or non-partitioned).

        Uses HDFS rename for an atomic swap so the table always keeps its
        original location in the Metastore.  The sequence is:
          1. Write compacted data to a sibling ``__compact_tmp_<ts>`` directory.
          2. Verify row count.
          3. Rename original → ``__old_<ts>`` (data preserved).
          4. Update the backup table to point at the ``__old_<ts>`` directory.
          5. Rename ``__compact_tmp_<ts>`` → original path.

        On any failure the rollback undoes outstanding renames and cleans up
        temporary directories.

        Args:
            table_info: Table information from analysis.
            backup_info: Backup information.

        Returns:
            CompactionReport with before/after metrics.
        """
        self._pending_renames = []
        self._temp_paths = []
        start_time = time.time()
        full_name = table_info.full_name
        format_str = table_info.file_format.value

        logger.info("Starting compaction for %s (format=%s)", full_name, format_str)

        report = CompactionReport(
            table_name=full_name,
            status=CompactionStatus.IN_PROGRESS,
            before_file_count=table_info.total_file_count,
            before_size_bytes=table_info.total_size_bytes,
            before_avg_file_size=table_info.avg_file_size_bytes,
        )

        # Single timestamp for the entire run: all __old_TS and __compact_tmp_TS
        # directories from this compaction share the same suffix, making them
        # identifiable as a group for cleanup and debugging.
        run_ts = int(time.time())

        try:
            if table_info.is_partitioned:
                self._compact_partitioned(table_info, format_str, report, backup_info, run_ts)
            else:
                self._compact_non_partitioned(table_info, format_str, report, backup_info, run_ts)

            report.status = CompactionStatus.COMPLETED
            logger.info("Compaction completed for %s", full_name)

        except Exception as e:
            report.status = CompactionStatus.FAILED
            report.error = str(e)
            logger.exception("Compaction failed for %s", full_name)
            self._rollback_on_failure(table_info)

        report.duration_seconds = time.time() - start_time
        return report

    def _compact_non_partitioned(
        self,
        table_info: TableInfo,
        format_str: str,
        report: CompactionReport,
        backup_info: BackupInfo,
        run_ts: int,
    ) -> None:
        """Compact a non-partitioned table using HDFS rename for atomic swap."""
        full_name = table_info.full_name
        location = table_info.location.rstrip("/")
        target_files = max(1, math.ceil(table_info.total_size_bytes / self._config.block_size_bytes))
        temp_location = f"{location}__compact_tmp_{run_ts}"
        old_location = f"{location}__old_{run_ts}"

        self._check_paths_available(temp_location, old_location)

        # Phase 1: Count original rows and write compacted data to temp
        logger.info("Reading data from %s", location)
        original_count = self._spark.read.format(format_str).load(location).count()
        report.row_count_before = original_count

        logger.info("Writing %d target files to %s", target_files, temp_location)
        self._temp_paths.append(temp_location)
        df = self._spark.read.format(format_str).load(location)
        if table_info.sort_columns:
            logger.info("Sorting by %s before coalescing", table_info.sort_columns)
            df = df.sort(*table_info.sort_columns)
        writer = df.coalesce(target_files).write.format(format_str).mode("overwrite")
        if table_info.compression_codec:
            writer = writer.option("compression", table_info.compression_codec)
        writer.save(temp_location)

        # Phase 2: Verify row count before touching original data
        new_count = self._spark.read.format(format_str).load(temp_location).count()
        report.row_count_after = new_count

        if new_count != original_count:
            self._hdfs.delete_path(temp_location)
            self._temp_paths.remove(temp_location)
            msg = f"Row count mismatch for {full_name}: original={original_count}, new={new_count}"
            raise ValueError(msg)

        # Phase 3: Atomic swap via HDFS rename
        # 3a. Move original → old  (original data is safe)
        self._hdfs.rename_path(location, old_location)
        self._pending_renames.append((old_location, location))

        # 3b. Update the backup table so rollback can find the original data
        try:
            self._backup_mgr.update_table_location(backup_info.backup_table, old_location)
        except Exception:
            logger.warning("Could not update backup table location for %s, continuing", full_name)

        # 3c. Move temp → original path  (table keeps pointing to original location)
        self._hdfs.rename_path(temp_location, location)
        self._temp_paths.remove(temp_location)
        self._pending_renames.pop()  # swap completed successfully

        logger.info("Compaction swap complete for %s (location unchanged: %s)", full_name, location)

        new_file_info = self._hdfs.get_file_info(location)
        report.after_file_count = new_file_info.file_count
        report.after_size_bytes = new_file_info.total_size_bytes
        report.after_avg_file_size = new_file_info.avg_file_size_bytes

    def _compact_partitioned(
        self,
        table_info: TableInfo,
        format_str: str,
        report: CompactionReport,
        backup_info: BackupInfo,
        run_ts: int,
    ) -> None:
        """Compact a partitioned table, partition by partition."""
        partitions_compacted = 0
        partitions_skipped = 0
        total_after_files = 0
        total_after_size = 0

        for partition in table_info.partitions:
            if not partition.needs_compaction:
                partitions_skipped += 1
                total_after_files += partition.file_count
                total_after_size += partition.total_size_bytes
                continue

            self._compact_single_partition(table_info, partition, format_str, backup_info, run_ts)
            partitions_compacted += 1

            try:
                new_file_info = self._hdfs.get_file_info(partition.location)
                total_after_files += new_file_info.file_count
                total_after_size += new_file_info.total_size_bytes
            except Exception:
                total_after_files += partition.target_files
                total_after_size += partition.total_size_bytes

        report.partitions_compacted = partitions_compacted
        report.partitions_skipped = partitions_skipped
        report.after_file_count = total_after_files
        report.after_size_bytes = total_after_size
        if report.after_file_count > 0:
            report.after_avg_file_size = report.after_size_bytes // report.after_file_count

    def _compact_single_partition(
        self,
        table_info: TableInfo,
        partition: PartitionInfo,
        format_str: str,
        backup_info: BackupInfo,
        run_ts: int,
    ) -> None:
        """Compact a single partition using HDFS rename for atomic swap."""
        full_name = table_info.full_name
        location = partition.location.rstrip("/")
        target_files = partition.target_files
        temp_location = f"{location}__compact_tmp_{run_ts}"
        old_location = f"{location}__old_{run_ts}"

        self._check_paths_available(temp_location, old_location)

        logger.info(
            "Compacting partition %s (%d -> %d files)",
            partition.partition_spec_str,
            partition.file_count,
            target_files,
        )

        original_count = self._spark.read.format(format_str).load(location).count()
        self._temp_paths.append(temp_location)
        df = self._spark.read.format(format_str).load(location)
        if table_info.sort_columns:
            df = df.sort(*table_info.sort_columns)
        writer = df.coalesce(target_files).write.format(format_str).mode("overwrite")
        if table_info.compression_codec:
            writer = writer.option("compression", table_info.compression_codec)
        writer.save(temp_location)

        new_count = self._spark.read.format(format_str).load(temp_location).count()

        if new_count != original_count:
            self._hdfs.delete_path(temp_location)
            self._temp_paths.remove(temp_location)
            msg = (
                f"Row count mismatch for partition {partition.partition_spec_str}: "
                f"original={original_count}, new={new_count}"
            )
            raise ValueError(msg)

        # Atomic swap
        self._hdfs.rename_path(location, old_location)
        self._pending_renames.append((old_location, location))

        try:
            spec_sql = partition.partition_sql_spec
            self._backup_mgr.update_partition_location(backup_info.backup_table, spec_sql, old_location)
        except Exception:
            logger.warning("Could not update backup partition location for %s, continuing", full_name)

        self._hdfs.rename_path(temp_location, location)
        self._temp_paths.remove(temp_location)
        self._pending_renames.pop()  # swap completed successfully

    def _check_paths_available(self, *paths: str) -> None:
        """Raise if any of the given HDFS paths already exist.

        This guards against timestamp collisions or leftover directories from
        a previous failed run being silently overwritten.

        Args:
            paths: HDFS paths to check.

        Raises:
            RuntimeError: If any path already exists.
        """
        for path in paths:
            if self._hdfs.path_exists(path):
                msg = f"Path already exists, cannot use as compaction staging area: {path}"
                raise RuntimeError(msg)

    def _rollback_on_failure(self, table_info: TableInfo) -> None:
        """Rollback in-flight HDFS renames and clean up temporary directories."""
        full_name = table_info.full_name
        logger.warning("Rolling back %s to original state", full_name)

        try:
            # Clean up any temp paths (unverified or failed compacted data)
            for temp_path in self._temp_paths:
                if self._hdfs.path_exists(temp_path):
                    self._hdfs.delete_path(temp_path)
                    logger.info("Deleted temp path: %s", temp_path)

            # Undo renames in reverse order
            for old_path, original_path in reversed(self._pending_renames):
                if self._hdfs.path_exists(old_path):
                    # If original_path now has partial/bad data, remove it first
                    if self._hdfs.path_exists(original_path):
                        self._hdfs.delete_path(original_path)
                    self._hdfs.rename_path(old_path, original_path)
                    logger.info("Restored %s -> %s", old_path, original_path)

            logger.info("Rollback successful for %s", full_name)
        except Exception:
            logger.exception("Rollback FAILED for %s - manual intervention required", full_name)
        finally:
            self._pending_renames = []
            self._temp_paths = []
