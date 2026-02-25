"""HDFS operations using Hadoop FileSystem API via Spark's JVM gateway."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


@dataclass
class HdfsFileInfo:
    """Information about files in an HDFS directory."""

    file_count: int
    total_size_bytes: int
    file_sizes: list[int] = field(default_factory=list)

    @property
    def avg_file_size_bytes(self) -> int:
        """Average file size in bytes."""
        if self.file_count == 0:
            return 0
        return self.total_size_bytes // self.file_count

    @property
    def median_file_size_bytes(self) -> int:
        """Median file size in bytes. Falls back to avg when no sizes collected."""
        if not self.file_sizes:
            return self.avg_file_size_bytes
        sorted_sizes = sorted(self.file_sizes)
        n = len(sorted_sizes)
        mid = n // 2
        if n % 2 == 1:
            return sorted_sizes[mid]
        return (sorted_sizes[mid - 1] + sorted_sizes[mid]) // 2

    @property
    def effective_file_size_bytes(self) -> int:
        """Min of avg and median file size.

        Catches skewed distributions where a few large files inflate the
        average while many tiny files remain undetected.
        """
        return min(self.avg_file_size_bytes, self.median_file_size_bytes)


class HdfsClient:
    """Client for HDFS operations using Spark's Hadoop FileSystem API."""

    def __init__(self, spark: SparkSession) -> None:
        """Initialize HDFS client.

        Args:
            spark: Active SparkSession with access to Hadoop APIs.
        """
        self._spark = spark
        self._jvm = spark._jvm  # type: ignore[union-attr]
        self._jsc = spark._jsc  # type: ignore[union-attr]

    def _get_filesystem(self, path: str) -> object:
        """Get the Hadoop FileSystem for a given path.

        Args:
            path: HDFS path.

        Returns:
            Hadoop FileSystem instance.
        """
        hadoop_path = self._jvm.org.apache.hadoop.fs.Path(path)
        conf = self._jsc.hadoopConfiguration()
        return hadoop_path.getFileSystem(conf)

    def get_file_info(self, path: str) -> HdfsFileInfo:
        """Get file count and total size for an HDFS directory.

        Args:
            path: HDFS directory path.

        Returns:
            HdfsFileInfo with file count and total size.
        """
        hadoop_path = self._jvm.org.apache.hadoop.fs.Path(path)
        fs = self._get_filesystem(path)

        file_count = 0
        total_size = 0
        file_sizes: list[int] = []

        iterator = fs.listFiles(hadoop_path, True)
        while iterator.hasNext():
            file_status = iterator.next()
            file_path = file_status.getPath().getName()
            if not file_path.startswith("_") and not file_path.startswith("."):
                file_count += 1
                # getLen() returns the logical file size (data bytes only).
                # It does NOT include the HDFS replication factor.
                # A 50 MB file with replication=3 reports 50 MB here, not 150 MB.
                size = file_status.getLen()
                total_size += size
                file_sizes.append(size)

        logger.debug("HDFS path %s: %d files, %d bytes", path, file_count, total_size)
        return HdfsFileInfo(file_count=file_count, total_size_bytes=total_size, file_sizes=file_sizes)

    def path_exists(self, path: str) -> bool:
        """Check if an HDFS path exists.

        Args:
            path: HDFS path to check.

        Returns:
            True if the path exists.
        """
        hadoop_path = self._jvm.org.apache.hadoop.fs.Path(path)
        fs = self._get_filesystem(path)
        return bool(fs.exists(hadoop_path))

    def delete_path(self, path: str, recursive: bool = True) -> bool:
        """Delete an HDFS path.

        Args:
            path: HDFS path to delete.
            recursive: Whether to delete recursively.

        Returns:
            True if deletion was successful.
        """
        hadoop_path = self._jvm.org.apache.hadoop.fs.Path(path)
        fs = self._get_filesystem(path)
        result = bool(fs.delete(hadoop_path, recursive))
        logger.info("Deleted HDFS path: %s (recursive=%s, success=%s)", path, recursive, result)
        return result

    def mkdirs(self, path: str) -> bool:
        """Create directories on HDFS.

        Args:
            path: HDFS directory path to create.

        Returns:
            True if creation was successful.
        """
        hadoop_path = self._jvm.org.apache.hadoop.fs.Path(path)
        fs = self._get_filesystem(path)
        return bool(fs.mkdirs(hadoop_path))

    def rename_path(self, src: str, dst: str) -> bool:
        """Rename/move an HDFS path (atomic within the same namespace).

        Args:
            src: Source HDFS path.
            dst: Destination HDFS path.

        Returns:
            True if rename was successful.
        """
        src_path = self._jvm.org.apache.hadoop.fs.Path(src)
        dst_path = self._jvm.org.apache.hadoop.fs.Path(dst)
        fs = self._get_filesystem(src)
        result = bool(fs.rename(src_path, dst_path))
        if not result:
            msg = f"HDFS rename failed: {src} -> {dst}"
            raise RuntimeError(msg)
        logger.info("Renamed HDFS path: %s -> %s", src, dst)
        return result
