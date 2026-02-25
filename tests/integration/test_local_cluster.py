"""End-to-end integration tests for lakekeeper on a local Spark cluster.

Simulates the Cloudera CDP 7.1.9 SP1 environment:
  - PySpark 3.3.2
  - Python 3.9
  - Hive Metastore backed by Derby (embedded, in-process)
  - Local filesystem acting as HDFS (file:// paths)

Run with:
    conda run -n lakekeeper_test python tests/integration/test_local_cluster.py

Each test scenario prints a clear PASS / FAIL summary.
"""

from __future__ import annotations

import os
import shutil
import tempfile
import traceback
from pathlib import Path

# PySpark 3.3.x uses Hadoop 3.3 which calls javax.security.auth.Subject.getSubject(),
# a method removed in Java 21+.  Set JAVA_TOOL_OPTIONS before the JVM boots so the
# gateway process starts with the required --add-opens flags.
_JAVA_OPENS = " ".join(
    [
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
        "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
        "--add-opens=java.base/java.io=ALL-UNNAMED",
        "--add-opens=java.base/java.net=ALL-UNNAMED",
        "--add-opens=java.base/java.nio=ALL-UNNAMED",
        "--add-opens=java.base/java.util=ALL-UNNAMED",
        "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
        "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
        "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
        "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
        "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
        "--add-opens=java.base/javax.security.auth=ALL-UNNAMED",
        "--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED",
    ]
)
os.environ["JAVA_TOOL_OPTIONS"] = _JAVA_OPENS

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402,N812
from pyspark.sql.types import LongType  # noqa: E402

from lakekeeper.config import LakekeeperConfig  # noqa: E402
from lakekeeper.engine.hive_external import HiveExternalEngine  # noqa: E402
from lakekeeper.models import CompactionStatus  # noqa: E402

# ── helpers ──────────────────────────────────────────────────────────────────

BLOCK_MB = 1  # 1 MB blocks → forces compaction even on tiny test data
RATIO = 2.0  # compact if avg file < block/2


def make_spark(warehouse: str, metastore: str) -> SparkSession:
    """Create a local SparkSession with embedded Derby Hive Metastore."""
    return (
        SparkSession.builder.master("local[2]")
        .appName("lakekeeper-integration-test")
        .config("spark.sql.warehouse.dir", warehouse)
        .config(
            "javax.jdo.option.ConnectionURL",
            f"jdbc:derby:{metastore};create=true",
        )
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .enableHiveSupport()
        .getOrCreate()
    )


def make_config(**kwargs) -> LakekeeperConfig:
    return LakekeeperConfig(
        block_size_mb=BLOCK_MB,
        compaction_ratio_threshold=RATIO,
        **kwargs,
    )


def write_small_files(
    spark: SparkSession,
    location: str,
    n_rows: int,
    n_files: int,
) -> None:
    """Write n_rows split across n_files small Parquet files to location."""
    (
        spark.range(n_rows)
        .withColumnRenamed("id", "event_id")
        .withColumn("user_id", (F.rand() * 1000).cast(LongType()))
        .withColumn("event_type", F.when(F.rand() < 0.5, "view").otherwise("click"))
        .repartition(n_files)
        .write.mode("overwrite")
        .parquet(location)
    )


def setup_db(spark: SparkSession, db: str) -> None:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS `{db}`")


def _sep(char: str = "─", width: int = 60) -> str:
    return char * width


# ── Scenario 1: non-partitioned table ────────────────────────────────────────


def test_nonpartitioned(spark: SparkSession, warehouse: str) -> None:
    """Compact a non-partitioned table: analyze → compact → verify → cleanup."""
    db = "inttest_np"
    table = "events_flat"
    full = f"{db}.{table}"
    loc = f"{warehouse}/{db}.db/{table}"

    setup_db(spark, db)
    spark.sql(f"DROP TABLE IF EXISTS `{db}`.`{table}`")
    spark.sql(f"""
        CREATE EXTERNAL TABLE `{db}`.`{table}` (
            event_id   BIGINT,
            user_id    BIGINT,
            event_type STRING
        )
        STORED AS PARQUET
        LOCATION '{loc}'
        TBLPROPERTIES ('external.table.purge' = 'false')
    """)

    write_small_files(spark, loc, n_rows=2_000, n_files=40)
    rows_before = spark.table(full).count()
    assert rows_before == 2_000, f"Expected 2000 rows, got {rows_before}"

    config = make_config()
    engine = HiveExternalEngine(spark, config)

    # Analyze
    info = engine.analyze(db, table)
    assert info.needs_compaction, "Table should need compaction"
    avg_kb = info.avg_file_size_bytes / 1024
    print(f"  analyze  → {info.file_count} files, avg {avg_kb:.1f} KB — needs_compaction={info.needs_compaction}")

    # Compact
    report = engine.compact(db, table)
    assert report.status == CompactionStatus.COMPLETED, f"Compact failed: {report}"
    print(f"  compact  → {report.files_after} files after (was {report.files_before})")

    # Row count preserved
    rows_after = spark.table(full).count()
    assert rows_after == rows_before, f"Row count changed: {rows_before} → {rows_after}"

    # Re-analyze: should no longer need compaction
    info2 = engine.analyze(db, table)
    assert not info2.needs_compaction, "Table should NOT need compaction after compact"
    print(f"  re-analyze → needs_compaction={info2.needs_compaction} ✓")

    # Cleanup
    engine.cleanup(db, table)
    print("  cleanup  → done")


# ── Scenario 2: single-partition table (date) ─────────────────────────────────


def test_single_partition(spark: SparkSession, warehouse: str) -> None:
    """Compact a table partitioned by date."""
    db = "inttest_sp"
    table = "events"
    full = f"{db}.{table}"
    loc = f"{warehouse}/{db}.db/{table}"

    setup_db(spark, db)
    spark.sql(f"DROP TABLE IF EXISTS `{db}`.`{table}`")
    spark.sql(f"""
        CREATE EXTERNAL TABLE `{db}`.`{table}` (
            event_id   BIGINT,
            user_id    BIGINT,
            event_type STRING
        )
        PARTITIONED BY (date STRING)
        STORED AS PARQUET
        LOCATION '{loc}'
        TBLPROPERTIES ('external.table.purge' = 'false')
    """)

    dates = ["2024-01-01", "2024-01-02", "2024-01-03"]
    for d in dates:
        write_small_files(spark, f"{loc}/date={d}", n_rows=1_000, n_files=20)
    spark.sql(f"MSCK REPAIR TABLE `{db}`.`{table}`")

    rows_before = spark.table(full).count()
    assert rows_before == 3_000

    config = make_config()
    engine = HiveExternalEngine(spark, config)

    info = engine.analyze(db, table)
    assert info.needs_compaction
    n_parts = len(info.partitions)
    compaction_flag = info.needs_compaction
    print(f"  analyze  → {info.file_count} files across {n_parts} partitions — needs_compaction={compaction_flag}")

    report = engine.compact(db, table)
    assert report.status == CompactionStatus.COMPLETED, f"Compact failed: {report}"
    print(f"  compact  → {report.files_after} files after (was {report.files_before})")

    rows_after = spark.table(full).count()
    assert rows_after == rows_before, f"Row count changed: {rows_before} → {rows_after}"

    info2 = engine.analyze(db, table)
    assert not info2.needs_compaction
    print(f"  re-analyze → needs_compaction={info2.needs_compaction} ✓")

    engine.cleanup(db, table)
    print("  cleanup  → done")


# ── Scenario 3: two-partition table (date + ref) ─────────────────────────────


def test_two_partitions(spark: SparkSession, warehouse: str) -> None:
    """Compact a table partitioned by (date, ref)."""
    db = "inttest_2p"
    table = "events_2p"
    full = f"{db}.{table}"
    loc = f"{warehouse}/{db}.db/{table}"

    setup_db(spark, db)
    spark.sql(f"DROP TABLE IF EXISTS `{db}`.`{table}`")
    spark.sql(f"""
        CREATE EXTERNAL TABLE `{db}`.`{table}` (
            event_id   BIGINT,
            user_id    BIGINT,
            event_type STRING
        )
        PARTITIONED BY (date STRING, ref STRING)
        STORED AS PARQUET
        LOCATION '{loc}'
        TBLPROPERTIES ('external.table.purge' = 'false')
    """)

    combos = [
        ("2024-01-01", "A"),
        ("2024-01-01", "B"),
        ("2024-01-02", "A"),
        ("2024-01-02", "B"),
    ]
    for d, r in combos:
        write_small_files(spark, f"{loc}/date={d}/ref={r}", n_rows=500, n_files=15)
    spark.sql(f"MSCK REPAIR TABLE `{db}`.`{table}`")

    rows_before = spark.table(full).count()
    assert rows_before == 2_000

    config = make_config()
    engine = HiveExternalEngine(spark, config)

    info = engine.analyze(db, table)
    assert info.needs_compaction
    n_parts = len(info.partitions)
    compaction_flag = info.needs_compaction
    print(f"  analyze  → {info.file_count} files across {n_parts} partitions — needs_compaction={compaction_flag}")

    report = engine.compact(db, table)
    assert report.status == CompactionStatus.COMPLETED, f"Compact failed: {report}"
    print(f"  compact  → {report.files_after} files after (was {report.files_before})")

    rows_after = spark.table(full).count()
    assert rows_after == rows_before, f"Row count changed: {rows_before} → {rows_after}"

    info2 = engine.analyze(db, table)
    assert not info2.needs_compaction
    print(f"  re-analyze → needs_compaction={info2.needs_compaction} ✓")

    engine.cleanup(db, table)
    print("  cleanup  → done")


# ── Scenario 4: rollback ──────────────────────────────────────────────────────


def test_rollback(spark: SparkSession, warehouse: str) -> None:
    """Compact then rollback: table must be restored to exact pre-compaction state."""
    db = "inttest_rb"
    table = "events_rb"
    full = f"{db}.{table}"
    loc = f"{warehouse}/{db}.db/{table}"

    setup_db(spark, db)
    spark.sql(f"DROP TABLE IF EXISTS `{db}`.`{table}`")
    spark.sql(f"""
        CREATE EXTERNAL TABLE `{db}`.`{table}` (
            event_id   BIGINT,
            user_id    BIGINT,
            event_type STRING
        )
        STORED AS PARQUET
        LOCATION '{loc}'
        TBLPROPERTIES ('external.table.purge' = 'false')
    """)

    write_small_files(spark, loc, n_rows=1_000, n_files=30)
    rows_before = spark.table(full).count()

    config = make_config()
    engine = HiveExternalEngine(spark, config)

    report = engine.compact(db, table)
    assert report.status == CompactionStatus.COMPLETED
    print(f"  compact  → {report.files_after} files after (was {report.files_before})")

    engine.rollback(db, table)
    print("  rollback → done")

    rows_after_rollback = spark.table(full).count()
    assert rows_after_rollback == rows_before, (
        f"Row count after rollback: {rows_after_rollback} (expected {rows_before})"
    )
    print(f"  rows preserved after rollback: {rows_after_rollback} ✓")


# ── runner ────────────────────────────────────────────────────────────────────

SCENARIOS = [
    ("Scenario 1 — Non-partitioned table", test_nonpartitioned),
    ("Scenario 2 — Single-partition (date)", test_single_partition),
    ("Scenario 3 — Two partitions (date+ref)", test_two_partitions),
    ("Scenario 4 — Rollback", test_rollback),
]


def main() -> None:
    """Run all integration scenarios and print a summary."""
    tmpdir = tempfile.mkdtemp(prefix="lakekeeper_inttest_")
    warehouse = f"{tmpdir}/warehouse"
    metastore = f"{tmpdir}/metastore_db"
    Path(warehouse).mkdir()

    print(f"\n{'═' * 60}")
    print("  LAKEKEEPER — Local Integration Tests")
    print("  PySpark 3.3.2 · Python 3.9 · Derby Hive Metastore")
    print(f"  Warehouse: {warehouse}")
    print(f"{'═' * 60}\n")

    spark = make_spark(warehouse, metastore)
    spark.sparkContext.setLogLevel("ERROR")

    results: list[tuple[str, str]] = []

    for name, fn in SCENARIOS:
        print(f"{_sep()}")
        print(f"  {name}")
        print(_sep())
        try:
            fn(spark, warehouse)
            results.append((name, "PASS ✓"))
            print("  → PASS\n")
        except Exception:
            results.append((name, "FAIL ✗"))
            traceback.print_exc()
            print("  → FAIL\n")

    spark.stop()
    shutil.rmtree(tmpdir, ignore_errors=True)

    print(f"\n{'═' * 60}")
    print("  SUMMARY")
    print("═" * 60)
    all_pass = True
    for name, status in results:
        print(f"  {status}  {name}")
        if "FAIL" in status:
            all_pass = False
    print(f"{'═' * 60}\n")

    if not all_pass:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
