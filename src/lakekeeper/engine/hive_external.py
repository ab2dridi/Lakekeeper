"""Hive external table compaction engine."""

from __future__ import annotations

import logging
import re
from datetime import datetime, timedelta
from typing import TYPE_CHECKING

from lakekeeper.core.analyzer import TableAnalyzer
from lakekeeper.core.backup import BackupManager
from lakekeeper.core.compactor import Compactor
from lakekeeper.engine.base import CompactionEngine
from lakekeeper.models import CompactionStatus
from lakekeeper.utils.hdfs import HdfsClient

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

    from lakekeeper.config import LakekeeperConfig
    from lakekeeper.models import BackupInfo, CompactionReport, TableInfo

logger = logging.getLogger(__name__)


class HiveExternalEngine(CompactionEngine):
    """Compaction engine for Hive external tables."""

    def __init__(self, spark: SparkSession, config: LakekeeperConfig) -> None:
        """Initialize the engine.

        Args:
            spark: Active SparkSession with Hive support.
            config: Beekeeper configuration.
        """
        self._spark = spark
        self._config = config
        self._hdfs = HdfsClient(spark)
        self._analyzer = TableAnalyzer(spark, self._hdfs, config)
        self._backup_mgr = BackupManager(spark, config)
        self._compactor = Compactor(spark, self._hdfs, config, self._backup_mgr)

    def analyze(self, database: str, table_name: str) -> TableInfo:
        """Analyze a table and return detailed information.

        Args:
            database: Database name.
            table_name: Table name.

        Returns:
            TableInfo with analysis results.
        """
        return self._analyzer.analyze_table(database, table_name)

    def create_backup(self, table_info: TableInfo) -> BackupInfo:
        """Create a zero-copy backup of a table.

        Args:
            table_info: Table information from analysis.

        Returns:
            BackupInfo with backup details.
        """
        return self._backup_mgr.create_backup(table_info)

    def compact(self, table_info: TableInfo, backup_info: BackupInfo) -> CompactionReport:
        """Perform compaction on a table.

        Args:
            table_info: Table information from analysis.
            backup_info: Backup information.

        Returns:
            CompactionReport with results.
        """
        report = self._compactor.compact_table(table_info, backup_info)
        if (
            self._config.analyze_after_compaction
            and not self._config.dry_run
            and report.status == CompactionStatus.COMPLETED
        ):
            self._run_analyze_stats(table_info)
        return report

    def rollback(self, database: str, table_name: str, backup_table: str | None = None) -> BackupInfo:
        """Rollback a table to its pre-compaction state.

        After a successful compaction the backup table points at an
        ``__old_<ts>`` sibling directory that holds the original data.
        Rollback:
          1. Deletes the current (compacted) data at the table's location.
          2. Renames ``__old_<ts>`` back to the original path.
          3. Drops the backup table.

        For backups created before the rename strategy (original_location has
        no ``__old_`` suffix) the legacy ``ALTER TABLE SET LOCATION`` path is
        used as a fallback.

        Args:
            database: Database name.
            table_name: Table name.
            backup_table: Specific backup table to use. If None, uses most recent.

        Returns:
            BackupInfo of the backup that was used.

        Raises:
            ValueError: If no backup is found.
        """
        full_name = f"{database}.{table_name}"
        logger.info("Rolling back %s", full_name)

        backup_info = self._backup_mgr.find_latest_backup(database, table_name)
        if backup_info is None:
            msg = f"No backup found for {full_name}"
            raise ValueError(msg)

        if backup_info.partition_locations:
            for spec_str, backup_loc in backup_info.partition_locations.items():
                if "__old_" in backup_loc:
                    current_loc = re.sub(r"__old_\d+$", "", backup_loc.rstrip("/"))
                    if self._hdfs.path_exists(current_loc):
                        self._hdfs.delete_path(current_loc)
                    self._hdfs.rename_path(backup_loc, current_loc)
                    logger.info("Restored partition %s -> %s", backup_loc, current_loc)
                else:
                    # Legacy fallback: ALTER TABLE PARTITION SET LOCATION
                    parts = spec_str.split("/")
                    spec_sql = ", ".join(f"{p.split('=')[0]}='{p.split('=')[1]}'" for p in parts)
                    self._spark.sql(f"ALTER TABLE {full_name} PARTITION({spec_sql}) SET LOCATION '{backup_loc}'")
                    logger.info("Restored partition %s -> %s", spec_str, backup_loc)
        else:
            backup_loc = backup_info.original_location
            if "__old_" in backup_loc:
                current_loc = re.sub(r"__old_\d+$", "", backup_loc.rstrip("/"))
                if self._hdfs.path_exists(current_loc):
                    self._hdfs.delete_path(current_loc)
                self._hdfs.rename_path(backup_loc, current_loc)
                logger.info("Restored table location %s -> %s", backup_loc, current_loc)
            else:
                # Legacy fallback: ALTER TABLE SET LOCATION
                self._spark.sql(f"ALTER TABLE {full_name} SET LOCATION '{backup_loc}'")
                logger.info("Restored table location -> %s", backup_loc)

        self._backup_mgr.drop_backup(database, backup_info.backup_table.split(".")[-1])
        logger.info("Rollback complete for %s", full_name)
        return backup_info

    def cleanup(self, database: str, table_name: str, older_than_days: int | None = None) -> int:
        """Clean up backup tables and associated ``__old_*`` data directories.

        Args:
            database: Database name.
            table_name: Table name.
            older_than_days: Only clean backups older than this many days.

        Returns:
            Number of backups cleaned up.
        """
        backups = self._backup_mgr.list_backups(database, table_name)
        cleaned = 0
        cutoff = None
        if older_than_days is not None:
            cutoff = datetime.now() - timedelta(days=older_than_days)

        for backup_name in backups:
            if cutoff is not None:
                prefix = f"{self._config.backup_prefix}_{table_name}_"
                ts_str = backup_name.replace(prefix, "")
                try:
                    backup_ts = datetime.strptime(ts_str, "%Y%m%d_%H%M%S")
                    if backup_ts >= cutoff:
                        continue
                except ValueError:
                    pass

            # Delete __old_* directories tracked by this backup before dropping it
            full_backup = f"{database}.{backup_name}"
            self._delete_old_data_dirs(full_backup)

            self._backup_mgr.drop_backup(database, backup_name)
            cleaned += 1

        logger.info("Cleaned up %d backups for %s.%s", cleaned, database, table_name)
        return cleaned

    def cleanup_orphan_backups(self, database: str) -> int:
        """Clean up backup tables whose original tables no longer exist.

        Scans all tables in the database for backup tables (matching the
        backup prefix pattern) and drops those whose original table is absent.

        Args:
            database: Database to scan.

        Returns:
            Number of orphan backups cleaned up.
        """
        tables = self._spark.sql(f"SHOW TABLES IN {database}").collect()
        all_table_names = {row["tableName"] for row in tables}

        cleaned = 0
        pattern = re.compile(rf"^{re.escape(self._config.backup_prefix)}_(.+)_\d{{8}}_\d{{6}}$")
        for tbl in list(all_table_names):
            m = pattern.match(tbl)
            if not m:
                continue
            original_name = m.group(1)
            if original_name in all_table_names:
                continue  # original still exists — not an orphan
            logger.info(
                "Cleaning up orphan backup %s.%s (original table '%s' no longer exists)",
                database,
                tbl,
                original_name,
            )
            self._delete_old_data_dirs(f"{database}.{tbl}")
            self._backup_mgr.drop_backup(database, tbl)
            cleaned += 1

        logger.info("Cleaned up %d orphan backups in %s", cleaned, database)
        return cleaned

    def list_tables(self, database: str) -> list[str]:
        """List external tables in a database.

        Args:
            database: Database name.

        Returns:
            List of external table names (excluding backup tables).
        """
        tables = self._spark.sql(f"SHOW TABLES IN {database}").collect()
        external_tables = []

        for row in tables:
            tbl = row["tableName"]
            if tbl.startswith(self._config.backup_prefix):
                continue

            try:
                desc_rows = self._spark.sql(f"DESCRIBE FORMATTED {database}.{tbl}").collect()
                desc_map = {r[0].strip(): (r[1] or "").strip() for r in desc_rows if r[0]}
                table_type = desc_map.get("Table Type", desc_map.get("Table Type:", ""))
                if "EXTERNAL" in table_type.upper():
                    external_tables.append(tbl)
            except Exception:
                logger.debug("Skipping table %s.%s (could not describe)", database, tbl)

        return external_tables

    def _run_analyze_stats(self, table_info: TableInfo) -> None:
        """Run ANALYZE TABLE COMPUTE STATISTICS after a successful compaction.

        For partitioned tables, statistics are updated per compacted partition
        first, then at the table level.  For non-partitioned tables only the
        table-level command is issued.

        These commands refresh rowCount / numFiles / totalSize in the Hive
        Metastore so that the query planner uses accurate statistics for the
        freshly compacted data.

        Args:
            table_info: TableInfo from the compaction run (partitions carry
                ``needs_compaction=True`` for partitions that were compacted).
        """
        full_name = table_info.full_name

        if table_info.is_partitioned:
            compacted = [p for p in table_info.partitions if p.needs_compaction]
            for part in compacted:
                sql = f"ANALYZE TABLE {full_name} PARTITION({part.partition_sql_spec}) COMPUTE STATISTICS"
                logger.info("Analyzing partition stats: %s", sql)
                try:
                    self._spark.sql(sql)
                except Exception:
                    logger.warning(
                        "ANALYZE TABLE partition failed for %s (%s)",
                        full_name,
                        part.partition_spec_str,
                        exc_info=True,
                    )

        sql = f"ANALYZE TABLE {full_name} COMPUTE STATISTICS"
        logger.info("Analyzing table stats: %s", sql)
        try:
            self._spark.sql(sql)
        except Exception:
            logger.warning("ANALYZE TABLE failed for %s", full_name, exc_info=True)

    def _delete_old_data_dirs(self, backup_table: str) -> None:
        """Delete HDFS ``__old_*`` directories referenced by a backup table.

        Checks both the table-level location (non-partitioned tables) and each
        partition's location (partitioned tables) for ``__old_*`` paths.
        """
        # Non-partitioned: table-level location
        try:
            desc_rows = self._spark.sql(f"DESCRIBE FORMATTED {backup_table}").collect()
            desc_map = {row[0].strip(): (row[1] or "").strip() for row in desc_rows if row[0]}
            location = ""
            for key in ("Location", "Location:"):
                if key in desc_map and desc_map[key]:
                    location = desc_map[key]
                    break
            if location and "__old_" in location and self._hdfs.path_exists(location):
                logger.info("Deleting old data dir: %s", location)
                self._hdfs.delete_path(location)
        except Exception:
            logger.debug("Could not read top-level location for backup %s", backup_table)

        # Partitioned: iterate over partition locations
        try:
            partitions = self._spark.sql(f"SHOW PARTITIONS {backup_table}").collect()
            for row in partitions:
                spec_str = row[0]
                spec_parts = spec_str.split("/")
                spec_sql = ", ".join(f"{p.split('=')[0]}='{p.split('=')[1]}'" for p in spec_parts)
                part_desc = self._spark.sql(f"DESCRIBE FORMATTED {backup_table} PARTITION({spec_sql})").collect()
                # First-occurrence: DESCRIBE FORMATTED PARTITION emits 'Location'
                # twice on Hive 3 / CDP (partition path first, table path second).
                part_map: dict[str, str] = {}
                for r in part_desc:
                    if r[0]:
                        k = r[0].strip()
                        if k not in part_map:
                            part_map[k] = (r[1] or "").strip()
                part_loc = ""
                for key in ("Location", "Location:"):
                    if key in part_map and part_map[key]:
                        part_loc = part_map[key]
                        break
                if part_loc and "__old_" in part_loc and self._hdfs.path_exists(part_loc):
                    logger.info("Deleting old partition data dir: %s", part_loc)
                    self._hdfs.delete_path(part_loc)
        except Exception:
            logger.debug("No partitions or could not read partition locations for backup %s", backup_table)
