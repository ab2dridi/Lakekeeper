"""SparkSession helper utilities."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

    from beekeeper.config import SparkSubmitConfig

logger = logging.getLogger(__name__)


def build_spark_submit_command(spark_cfg: SparkSubmitConfig, beekeeper_args: list[str]) -> list[str]:
    """Build a spark-submit command list from a SparkSubmitConfig.

    Args:
        spark_cfg: Spark submit configuration.
        beekeeper_args: Beekeeper CLI arguments to append after the script path.

    Returns:
        Full command list suitable for subprocess.run().
    """
    cmd = ["spark-submit", "--master", spark_cfg.master, "--deploy-mode", spark_cfg.deploy_mode]

    if spark_cfg.principal:
        cmd += ["--principal", spark_cfg.principal]
    if spark_cfg.keytab:
        cmd += ["--keytab", spark_cfg.keytab]
    if spark_cfg.queue:
        cmd += ["--conf", f"spark.yarn.queue={spark_cfg.queue}"]
    if spark_cfg.archives:
        cmd += ["--archives", spark_cfg.archives]
    if spark_cfg.python_env:
        cmd += ["--conf", f"spark.pyspark.python={spark_cfg.python_env}"]
    if spark_cfg.executor_memory:
        cmd += ["--executor-memory", spark_cfg.executor_memory]
    if spark_cfg.num_executors is not None:
        cmd += ["--num-executors", str(spark_cfg.num_executors)]
    if spark_cfg.executor_cores is not None:
        cmd += ["--executor-cores", str(spark_cfg.executor_cores)]
    if spark_cfg.driver_memory:
        cmd += ["--driver-memory", spark_cfg.driver_memory]
    for key, value in spark_cfg.extra_conf.items():
        cmd += ["--conf", f"{key}={value}"]

    cmd.append(spark_cfg.script_path)
    cmd += beekeeper_args
    return cmd


def get_or_create_spark_session(
    app_name: str = "beekeeper",
    master: str | None = None,
    enable_hive: bool = True,
) -> SparkSession:
    """Get or create a SparkSession configured for Hive access.

    Args:
        app_name: Spark application name.
        master: Spark master URL. If None, uses existing config.
        enable_hive: Whether to enable Hive support.

    Returns:
        Configured SparkSession.
    """
    from pyspark.sql import SparkSession

    builder = SparkSession.builder.appName(app_name)

    if master:
        builder = builder.master(master)

    if enable_hive:
        builder = builder.enableHiveSupport()

    spark = builder.getOrCreate()
    logger.info("SparkSession initialized: %s", spark.sparkContext.applicationId)
    return spark


def stop_spark_session(spark: SparkSession) -> None:
    """Stop a SparkSession gracefully.

    Args:
        spark: SparkSession to stop.
    """
    try:
        spark.stop()
        logger.info("SparkSession stopped.")
    except Exception:
        logger.exception("Error stopping SparkSession")
