r"""Create a fragmented non-partitioned Hive external table for lakekeeper demo.

Generates a flat table (no partitions) with many small Parquet files,
simulating an ingestion pipeline that never coalesces its output.

Prerequisites:
    - The target database must already exist (created by an admin).
    - You must have CREATE TABLE and write privileges on that database.

Usage on a Kerberized cluster:
    spark-submit \
        --master yarn --deploy-mode cluster \
        --principal user@REALM.COM \
        --keytab /etc/security/keytabs/user.keytab \
        demo/create_nonpartitioned_table.py \
        --database mydb

Optional arguments:
    --database      Existing Hive database (required)
    --table         Table name (default: lakekeeper_flat)
    --location      HDFS path for the table (default: derived from DESCRIBE DATABASE)
    --files         Total number of small files to create (default: 500)
    --rows          Total number of rows (default: 300000)

See demo/RUNBOOK.md for the complete end-to-end walkthrough.
"""

from __future__ import annotations

import argparse
import logging
import sys

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
    p = argparse.ArgumentParser(
        description="Create a fragmented non-partitioned Hive external table for lakekeeper demo"
    )
    p.add_argument(
        "--database",
        required=True,
        help="Existing Hive database where the demo table will be created",
    )
    p.add_argument(
        "--table",
        default="lakekeeper_flat",
        help="Demo table name (default: lakekeeper_flat)",
    )
    p.add_argument(
        "--location",
        default=None,
        help="HDFS path for the external table (default: derived from DESCRIBE DATABASE)",
    )
    p.add_argument(
        "--files",
        type=int,
        default=500,
        help="Number of small files to create (default: 500)",
    )
    p.add_argument(
        "--rows",
        type=int,
        default=300_000,
        help="Total number of rows (default: 300 000)",
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


def main() -> None:
    """Entry point: create the non-partitioned demo table with many small files."""
    args = parse_args()
    full_table = f"{args.database}.{args.table}"

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

    # Create (or recreate) the demo table
    spark.sql(f"DROP TABLE IF EXISTS `{args.database}`.`{args.table}`")
    spark.sql(f"""
        CREATE EXTERNAL TABLE `{args.database}`.`{args.table}` (
            event_id    BIGINT   COMMENT 'Unique event identifier',
            user_id     BIGINT   COMMENT 'User identifier',
            event_type  STRING   COMMENT 'view | click | purchase',
            amount      DOUBLE   COMMENT 'Transaction amount (purchase only)',
            event_ts    BIGINT   COMMENT 'Unix timestamp'
        )
        STORED AS PARQUET
        LOCATION '{table_location}'
        TBLPROPERTIES ('external.table.purge' = 'true')
    """)
    logger.info("External table created: %s", full_table)

    # Write all rows split across many small files (no partition column)
    df = (
        spark.range(args.rows)
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
        .withColumn("event_ts", (F.unix_timestamp() - (F.rand() * 86_400 * 30).cast(LongType())))
        .drop("_r1", "_r2")
        .repartition(args.files)  # deliberately fragmented
    )

    df.write.mode("overwrite").parquet(table_location)
    logger.info("Written %d rows → %d small files at %s", args.rows, args.files, table_location)

    actual_rows = spark.table(full_table).count()
    spark.stop()

    sep = "=" * 60
    print(f"\n{sep}")
    print("  DEMO TABLE READY — lakekeeper (non-partitioned)")
    print(sep)
    print(f"  Table      : {full_table}")
    print(f"  Location   : {table_location}")
    print("  Partitions : none")
    print(f"  Files      : {args.files}")
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
