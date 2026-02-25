"""Data models for Beekeeper."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum


class SkipTableError(Exception):
    """Raised when a table must be skipped (e.g. not an external table)."""


class FileFormat(Enum):
    """Supported file formats."""

    PARQUET = "parquet"
    ORC = "orc"


class CompactionStatus(Enum):
    """Status of a compaction operation."""

    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    ROLLED_BACK = "rolled_back"


@dataclass
class PartitionInfo:
    """Information about a single partition."""

    spec: dict[str, str]
    location: str
    file_count: int = 0
    total_size_bytes: int = 0
    needs_compaction: bool = False
    target_files: int = 1

    @property
    def avg_file_size_bytes(self) -> int:
        """Average file size in bytes."""
        if self.file_count == 0:
            return 0
        return self.total_size_bytes // self.file_count

    @property
    def partition_spec_str(self) -> str:
        """Partition spec as a Hive-style string (e.g., 'year=2024/month=01')."""
        return "/".join(f"{k}={v}" for k, v in self.spec.items())

    @property
    def partition_sql_spec(self) -> str:
        """Partition spec as SQL (e.g., "year='2024', month='01'")."""
        return ", ".join(f"{k}='{v}'" for k, v in self.spec.items())


@dataclass
class TableInfo:
    """Complete information about a Hive table."""

    database: str
    table_name: str
    location: str
    file_format: FileFormat
    is_partitioned: bool = False
    partition_columns: list[str] = field(default_factory=list)
    partitions: list[PartitionInfo] = field(default_factory=list)
    total_file_count: int = 0
    total_size_bytes: int = 0
    schema_columns: list[str] = field(default_factory=list)
    needs_compaction: bool = False
    row_count: int | None = None
    compression_codec: str | None = None
    sort_columns: list[str] = field(default_factory=list)

    @property
    def full_name(self) -> str:
        """Fully qualified table name."""
        return f"{self.database}.{self.table_name}"

    @property
    def avg_file_size_bytes(self) -> int:
        """Average file size in bytes."""
        if self.total_file_count == 0:
            return 0
        return self.total_size_bytes // self.total_file_count

    @property
    def target_files(self) -> int:
        """Target number of files after compaction."""
        if self.total_size_bytes == 0:
            return 1
        block_size = 128 * 1024 * 1024  # default, recalculated by analyzer
        return max(1, -(-self.total_size_bytes // block_size))  # ceil division


@dataclass
class BackupInfo:
    """Information about a backup table."""

    original_table: str
    backup_table: str
    original_location: str
    timestamp: datetime
    partition_locations: dict[str, str] = field(default_factory=dict)
    row_count: int | None = None


@dataclass
class CompactionReport:
    """Report of a compaction operation."""

    table_name: str
    status: CompactionStatus
    before_file_count: int = 0
    after_file_count: int = 0
    before_size_bytes: int = 0
    after_size_bytes: int = 0
    before_avg_file_size: int = 0
    after_avg_file_size: int = 0
    row_count_before: int | None = None
    row_count_after: int | None = None
    partitions_compacted: int = 0
    partitions_skipped: int = 0
    duration_seconds: float = 0.0
    error: str | None = None
