r"""Create a fragmented Hive external table for lakekeeper demo/testing.

Generates a partitioned table (date) with many small files per partition,
simulating a real incremental pipeline that has accumulated small-file debt.

Prerequisites:
    - The target database must already exist (created by an admin).
    - You must have CREATE TABLE and write privileges on that database.

Usage on a Kerberized cluster:
    spark-submit \
        --master yarn --deploy-mode cluster \
        --principal user@REALM.COM \
        --keytab /etc/security/keytabs/user.keytab \
        demo/create_single_partition_table.py \
        --database mydb

Optional arguments:
    --database          Existing Hive database (required, no default)
    --table             Table name (default: lakekeeper_events)
    --location          HDFS path for the table (default: derived from DESCRIBE DATABASE)
    --files-per-part    Number of small files per date partition (default: 200)
    --rows-per-part     Number of rows per date partition (default: 100000)
    --days              Number of date partitions to create (default: 3)

After the script completes, run:
    lakekeeper analyze --table <database>.lakekeeper_events
    lakekeeper compact --table <database>.lakekeeper_events
    lakekeeper analyze --table <database>.lakekeeper_events   # verify result
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
    p = argparse.ArgumentParser(description="Create a fragmented Hive external table for lakekeeper demo")
    p.add_argument(
        "--database",
        required=True,
        help="Existing Hive database where the demo table will be created",
    )
    p.add_argument(
        "--table",
        default="lakekeeper_events",
        help="Demo table name (default: lakekeeper_events)",
    )
    p.add_argument(
        "--location",
        default=None,
        help=("HDFS path for the external table. Default: <db_location>/<table> (derived from DESCRIBE DATABASE)."),
    )
    p.add_argument(
        "--files-per-part",
        type=int,
        default=200,
        help="Number of small files per date partition (default: 200)",
    )
    p.add_argument(
        "--rows-per-part",
        type=int,
        default=100_000,
        help="Number of rows per date partition (default: 100 000)",
    )
    p.add_argument(
        "--days",
        type=int,
        default=3,
        help="Number of date partitions (default: 3)",
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
    """Entry point: create the demo table and populate it with fragmented data."""
    args = parse_args()
    full_table = f"{args.database}.{args.table}"

    spark = SparkSession.builder.appName(f"lakekeeper-demo-{args.table}").enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # Verify the database exists before doing anything
    _check_database_exists(spark, args.database)

    # Resolve table HDFS location
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
            amount      DOUBLE   COMMENT 'Transaction amount (purchase only)'
        )
        PARTITIONED BY (date STRING COMMENT 'Event date YYYY-MM-DD')
        STORED AS PARQUET
        LOCATION '{table_location}'
        TBLPROPERTIES ('external.table.purge' = 'true')
    """)
    logger.info("External table created: %s", full_table)

    # Generate one date partition at a time with many small files
    dates = [(date.today() - timedelta(days=args.days - 1 - i)).isoformat() for i in range(args.days)]

    total_files = 0

    for d in dates:
        partition_path = f"{table_location}/date={d}"
        df = build_event_df(spark, args.rows_per_part, args.files_per_part)
        df.write.mode("overwrite").parquet(partition_path)
        total_files += args.files_per_part
        logger.info(
            "Written partition date=%s  →  %d files  (%d rows)",
            d,
            args.files_per_part,
            args.rows_per_part,
        )

    # Register all partitions in the Hive Metastore
    spark.sql(f"MSCK REPAIR TABLE `{args.database}`.`{args.table}`")
    logger.info("Partitions registered in Hive Metastore.")

    # Verify row count
    actual_rows = spark.table(full_table).count()

    spark.stop()

    # Summary
    sep = "=" * 60
    print(f"\n{sep}")
    print("  DEMO TABLE READY — lakekeeper")
    print(sep)
    print(f"  Table      : {full_table}")
    print(f"  Location   : {table_location}")
    print(f"  Partitions : {len(dates)}  (date)")
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
