"""Tests for beekeeper.utils.spark module."""

from __future__ import annotations

import sys
from unittest.mock import MagicMock, patch

from beekeeper.config import SparkSubmitConfig
from beekeeper.utils.spark import build_spark_submit_command, stop_spark_session


class TestBuildSparkSubmitCommand:
    def test_minimal_command(self):
        cfg = SparkSubmitConfig(enabled=True)
        cmd = build_spark_submit_command(cfg, ["compact", "--database", "mydb"])
        assert cmd[:5] == ["spark-submit", "--master", "yarn", "--deploy-mode", "client"]
        assert cmd[-3:] == ["compact", "--database", "mydb"]
        assert "run_beekeeper.py" in cmd

    def test_kerberos_params(self):
        cfg = SparkSubmitConfig(
            enabled=True,
            principal="user@REALM.COM",
            keytab="/etc/keytabs/user.keytab",
        )
        cmd = build_spark_submit_command(cfg, [])
        assert "--principal" in cmd
        assert "user@REALM.COM" in cmd
        assert "--keytab" in cmd
        assert "/etc/keytabs/user.keytab" in cmd

    def test_yarn_queue(self):
        cfg = SparkSubmitConfig(enabled=True, queue="data-engineering")
        cmd = build_spark_submit_command(cfg, [])
        assert "--conf" in cmd
        assert "spark.yarn.queue=data-engineering" in cmd

    def test_archives_and_python_env(self):
        cfg = SparkSubmitConfig(
            enabled=True,
            archives="/opt/env.tar.gz#env",
            python_env="./env/bin/python",
        )
        cmd = build_spark_submit_command(cfg, [])
        assert "--archives" in cmd
        assert "/opt/env.tar.gz#env" in cmd
        assert "spark.pyspark.python=./env/bin/python" in cmd

    def test_executor_resources(self):
        cfg = SparkSubmitConfig(
            enabled=True,
            executor_memory="4g",
            num_executors=10,
            executor_cores=2,
            driver_memory="2g",
        )
        cmd = build_spark_submit_command(cfg, [])
        assert "--executor-memory" in cmd
        assert "4g" in cmd
        assert "--num-executors" in cmd
        assert "10" in cmd
        assert "--executor-cores" in cmd
        assert "2" in cmd
        assert "--driver-memory" in cmd
        assert "2g" in cmd

    def test_extra_conf(self):
        cfg = SparkSubmitConfig(
            enabled=True,
            extra_conf={"spark.yarn.kerberos.relogin.period": "1h", "spark.dynamicAllocation.enabled": "true"},
        )
        cmd = build_spark_submit_command(cfg, [])
        assert "spark.yarn.kerberos.relogin.period=1h" in cmd
        assert "spark.dynamicAllocation.enabled=true" in cmd

    def test_custom_script_path(self):
        cfg = SparkSubmitConfig(enabled=True, script_path="/opt/beekeeper/run_beekeeper.py")
        cmd = build_spark_submit_command(cfg, ["analyze", "--table", "mydb.mytable"])
        assert "/opt/beekeeper/run_beekeeper.py" in cmd
        assert cmd[-3:] == ["analyze", "--table", "mydb.mytable"]

    def test_optional_params_omitted_when_none(self):
        cfg = SparkSubmitConfig(enabled=True)
        cmd = build_spark_submit_command(cfg, [])
        assert "--principal" not in cmd
        assert "--keytab" not in cmd
        assert "--executor-memory" not in cmd
        assert "--num-executors" not in cmd


class TestGetOrCreateSparkSession:
    def _setup_mock_pyspark(self):
        """Create a mock pyspark module and register it in sys.modules."""
        mock_spark_session = MagicMock()
        mock_pyspark = MagicMock()
        mock_pyspark.sql.SparkSession = mock_spark_session
        return mock_pyspark, mock_spark_session

    def test_default_session(self):
        mock_pyspark, mock_spark_cls = self._setup_mock_pyspark()
        mock_builder = MagicMock()
        mock_spark_cls.builder = mock_builder
        mock_builder.appName.return_value = mock_builder
        mock_builder.enableHiveSupport.return_value = mock_builder
        mock_session = MagicMock()
        mock_session.sparkContext.applicationId = "test-app"
        mock_builder.getOrCreate.return_value = mock_session

        with patch.dict(sys.modules, {"pyspark": mock_pyspark, "pyspark.sql": mock_pyspark.sql}):
            from beekeeper.utils.spark import get_or_create_spark_session

            result = get_or_create_spark_session()

        mock_builder.appName.assert_called_with("beekeeper")
        mock_builder.enableHiveSupport.assert_called_once()
        assert result == mock_session

    def test_custom_app_name_and_master(self):
        mock_pyspark, mock_spark_cls = self._setup_mock_pyspark()
        mock_builder = MagicMock()
        mock_spark_cls.builder = mock_builder
        mock_builder.appName.return_value = mock_builder
        mock_builder.master.return_value = mock_builder
        mock_builder.enableHiveSupport.return_value = mock_builder
        mock_session = MagicMock()
        mock_session.sparkContext.applicationId = "test-app"
        mock_builder.getOrCreate.return_value = mock_session

        with patch.dict(sys.modules, {"pyspark": mock_pyspark, "pyspark.sql": mock_pyspark.sql}):
            from beekeeper.utils.spark import get_or_create_spark_session

            get_or_create_spark_session(app_name="my_app", master="local[4]")

        mock_builder.appName.assert_called_with("my_app")
        mock_builder.master.assert_called_with("local[4]")

    def test_without_hive(self):
        mock_pyspark, mock_spark_cls = self._setup_mock_pyspark()
        mock_builder = MagicMock()
        mock_spark_cls.builder = mock_builder
        mock_builder.appName.return_value = mock_builder
        mock_session = MagicMock()
        mock_session.sparkContext.applicationId = "test-app"
        mock_builder.getOrCreate.return_value = mock_session

        with patch.dict(sys.modules, {"pyspark": mock_pyspark, "pyspark.sql": mock_pyspark.sql}):
            from beekeeper.utils.spark import get_or_create_spark_session

            get_or_create_spark_session(enable_hive=False)

        mock_builder.enableHiveSupport.assert_not_called()


class TestStopSparkSession:
    def test_stop_success(self):
        mock_spark = MagicMock()
        stop_spark_session(mock_spark)
        mock_spark.stop.assert_called_once()

    def test_stop_with_exception(self):
        mock_spark = MagicMock()
        mock_spark.stop.side_effect = RuntimeError("stop failed")
        # Should not raise
        stop_spark_session(mock_spark)
