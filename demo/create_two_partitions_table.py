r"""Create a fragmented two-partition Hive external table for lakekeeper demo.

Generates a table partitioned by two columns — (date, ref) — with many small
files per partition, simulating a production pipeline that outputs one file
per Spark task without coalescing.

The 'ref' column represents a reference category (e.g. data source, region,
business unit) that is commonly used as a second partition key alongside date.

Prerequisites:
    - The target database must already exist (created by an admin).
    - You must have CREATE TABLE and write privileges on that database.

Usage on a Kerberized cluster:
    spark-submit \
        --master yarn --deploy-mode cluster \
        --principal user@REALM.COM \
        --keytab /etc/security/keytabs/user.keytab \
        demo/create_two_partitions_table.py \
        --database mydb

Optional arguments:
    --database          Existing Hive database (required)
    --table             Table name (default: lakekeeper_events_2p)
    --location          HDFS path for the table (default: derived from DESCRIBE DATABASE)
    --files-per-part    Number of small files per (date, ref) partition (default: 100)
    --rows-per-part     Number of rows per (date, ref) partition (default: 50000)
    --days              Number of date values (default: 3)
    --refs              Reference values, comma-separated (default: A,B,C)

See demo/RUNBOOK.md for the complete end-to-end walkthrough.
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import date, timedelta

from pyspark.sql import SparkSession
from pyspark.sql import functions as F  # noqa: N812
from pyspark.sql.types import LongType

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    p = argparse.ArgumentParser(description="Create a fragmented two-partition Hive external table for lakekeeper demo")
    p.add_argument(
        "--database",
        required=True,
        help="Existing Hive database where the demo table will be created",
    )
    p.add_argument(
        "--table",
        default="lakekeeper_events_2p",
        help="Demo table name (default: lakekeeper_events_2p)",
    )
    p.add_argument(
        "--location",
        default=None,
        help="HDFS path for the external table (default: derived from DESCRIBE DATABASE)",
    )
    p.add_argument(
        "--files-per-part",
        type=int,
        default=100,
        help="Number of small files per (date, ref) partition (default: 100)",
    )
    p.add_argument(
        "--rows-per-part",
        type=int,
        default=50_000,
        help="Number of rows per (date, ref) partition (default: 50 000)",
    )
    p.add_argument(
        "--days",
        type=int,
        default=3,
        help="Number of date values to generate (default: 3)",
    )
    p.add_argument(
        "--refs",
        default="A,B,C",
        help="Comma-separated ref values (default: A,B,C)",
    )
    return p.parse_args()


def _get_db_location(spark: SparkSession, database: str) -> str | None:
    """Return the HDFS location of an existing Hive database."""
    try:
        rows = spark.sql(f"DESCRIBE DATABASE `{database}`").collect()
        for row in rows:
            key = (row[0] or "").strip().lower().rstrip(":")
            if key == "location":
                value = (row[1] or "").strip()
                if value:
                    return value
    except Exception:  # noqa: BLE001
        logger.debug("Could not read location for database %s", database)
    return None


def _check_database_exists(spark: SparkSession, database: str) -> None:
    """Abort with a clear message if the database does not exist."""
    try:
        matches = [row[0] for row in spark.sql(f"SHOW DATABASES LIKE '{database}'").collect()]
    except Exception as exc:  # noqa: BLE001
        logger.error("Failed to list databases: %s", exc)
        sys.exit(1)

    if database not in matches:
        logger.error(
            "Database '%s' does not exist or you lack access to it. "
            "Ask your cluster admin to create it, then re-run with --database %s.",
            database,
            database,
        )
        sys.exit(1)


def build_event_df(spark: SparkSession, n_rows: int, n_files: int):
    """Generate n_rows synthetic events, split across n_files Parquet files."""
    return (
        spark.range(n_rows)
        .withColumnRenamed("id", "event_id")
        .withColumn("_r1", F.rand())
        .withColumn("_r2", F.rand())
        .withColumn("user_id", (F.col("_r1") * 50_000).cast(LongType()))
        .withColumn(
            "event_type",
            F.when(F.col("_r2") < 0.50, "view").when(F.col("_r2") < 0.80, "click").otherwise("purchase"),
        )
        .withColumn(
            "amount",
            F.when(
                F.col("event_type") == "purchase",
                F.round(F.rand() * 499 + 1, 2),
            ).otherwise(None),
        )
        .drop("_r1", "_r2")
        .repartition(n_files)  # deliberately fragmented
    )


def main() -> None:
    """Entry point: create the two-partition demo table with many small files."""
    args = parse_args()
    full_table = f"{args.database}.{args.table}"
    ref_values = [r.strip() for r in args.refs.split(",") if r.strip()]

    spark = SparkSession.builder.appName(f"lakekeeper-demo-{args.table}").enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    _check_database_exists(spark, args.database)

    if args.location:
        table_location = args.location
    else:
        db_location = _get_db_location(spark, args.database)
        if not db_location:
            logger.error(
                "Could not auto-detect the HDFS location for database '%s'. "
                "Specify it explicitly with --location hdfs:///path/to/table.",
                args.database,
            )
            sys.exit(1)
        table_location = f"{db_location}/{args.table}"

    logger.info("Table location: %s", table_location)

    # Create (or recreate) the demo table — partitioned by (date, ref)
    spark.sql(f"DROP TABLE IF EXISTS `{args.database}`.`{args.table}`")
    spark.sql(f"""
        CREATE EXTERNAL TABLE `{args.database}`.`{args.table}` (
            event_id    BIGINT   COMMENT 'Unique event identifier',
            user_id     BIGINT   COMMENT 'User identifier',
            event_type  STRING   COMMENT 'view | click | purchase',
            amount      DOUBLE   COMMENT 'Transaction amount (purchase only)'
        )
        PARTITIONED BY (
            date  STRING  COMMENT 'Event date YYYY-MM-DD',
            ref   STRING  COMMENT 'Reference category (e.g. data source, region)'
        )
        STORED AS PARQUET
        LOCATION '{table_location}'
        TBLPROPERTIES ('external.table.purge' = 'true')
    """)
    logger.info("External table created: %s", full_table)

    dates = [(date.today() - timedelta(days=args.days - 1 - i)).isoformat() for i in range(args.days)]

    total_files = 0
    total_partitions = 0

    for d in dates:
        for ref in ref_values:
            partition_path = f"{table_location}/date={d}/ref={ref}"
            df = build_event_df(spark, args.rows_per_part, args.files_per_part)
            df.write.mode("overwrite").parquet(partition_path)
            total_files += args.files_per_part
            total_partitions += 1
            logger.info(
                "Written partition date=%s / ref=%s  →  %d files  (%d rows)",
                d,
                ref,
                args.files_per_part,
                args.rows_per_part,
            )

    spark.sql(f"MSCK REPAIR TABLE `{args.database}`.`{args.table}`")
    logger.info("Partitions registered in Hive Metastore.")

    actual_rows = spark.table(full_table).count()
    spark.stop()

    sep = "=" * 60
    print(f"\n{sep}")
    print("  DEMO TABLE READY — lakekeeper (two partitions: date + ref)")
    print(sep)
    print(f"  Table      : {full_table}")
    print(f"  Location   : {table_location}")
    print(f"  Partitions : {total_partitions}  ({len(dates)} dates × {len(ref_values)} refs)")
    print(f"  Files      : {total_files}  ({args.files_per_part} per partition)")
    print(f"  Rows       : {actual_rows:,}")
    print()
    print("  Next steps:")
    print(f"    lakekeeper analyze --table {full_table}")
    print(f"    lakekeeper compact --table {full_table}")
    print(f"    lakekeeper analyze --table {full_table}   # verify result")
    print(f"    lakekeeper cleanup --table {full_table}   # free up disk")
    print(sep + "\n")


if __name__ == "__main__":
    main()
