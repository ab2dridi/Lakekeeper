"""Tests for beekeeper.config module."""

from __future__ import annotations

import pytest

from beekeeper.config import BeekeeperConfig, SparkSubmitConfig


class TestBeekeeperConfig:
    def test_defaults(self):
        config = BeekeeperConfig()
        assert config.block_size_mb == 128
        assert config.compaction_ratio_threshold == 10.0
        assert config.backup_prefix == "__bkp"
        assert config.dry_run is False
        assert config.log_level == "INFO"
        assert config.config_file is None
        assert config.database is None
        assert config.table is None
        assert config.tables == []

    def test_block_size_bytes(self):
        config = BeekeeperConfig(block_size_mb=256)
        assert config.block_size_bytes == 256 * 1024 * 1024

    def test_compaction_threshold_bytes(self):
        config = BeekeeperConfig(block_size_mb=128, compaction_ratio_threshold=10.0)
        expected = int((128 * 1024 * 1024) / 10.0)
        assert config.compaction_threshold_bytes == expected

    def test_from_yaml(self, tmp_path):
        yaml_content = "block_size_mb: 256\ncompaction_ratio_threshold: 5.0\ndry_run: true\n"
        yaml_file = tmp_path / "config.yaml"
        yaml_file.write_text(yaml_content)

        config = BeekeeperConfig.from_yaml(yaml_file)
        assert config.block_size_mb == 256
        assert config.compaction_ratio_threshold == 5.0
        assert config.dry_run is True

    def test_from_yaml_missing_file(self):
        with pytest.raises(FileNotFoundError):
            BeekeeperConfig.from_yaml("/nonexistent/config.yaml")

    def test_from_yaml_empty_file(self, tmp_path):
        yaml_file = tmp_path / "empty.yaml"
        yaml_file.write_text("")
        config = BeekeeperConfig.from_yaml(yaml_file)
        assert config.block_size_mb == 128  # defaults

    def test_from_yaml_ignores_unknown_keys(self, tmp_path):
        yaml_content = "block_size_mb: 256\nunknown_key: value\n"
        yaml_file = tmp_path / "config.yaml"
        yaml_file.write_text(yaml_content)

        config = BeekeeperConfig.from_yaml(yaml_file)
        assert config.block_size_mb == 256
        assert not hasattr(config, "unknown_key")

    def test_merge_cli_overrides(self):
        config = BeekeeperConfig(block_size_mb=128)
        new_config = config.merge_cli_overrides(block_size_mb=256, dry_run=True)
        assert new_config.block_size_mb == 256
        assert new_config.dry_run is True
        # Original unchanged
        assert config.block_size_mb == 128

    def test_merge_cli_overrides_ignores_none(self):
        config = BeekeeperConfig(block_size_mb=128)
        new_config = config.merge_cli_overrides(block_size_mb=None, dry_run=None)
        assert new_config.block_size_mb == 128
        assert new_config.dry_run is False

    def test_setup_logging(self):
        config = BeekeeperConfig(log_level="DEBUG")
        config.setup_logging()  # Should not raise

    def test_spark_submit_default_disabled(self):
        config = BeekeeperConfig()
        assert config.spark_submit.enabled is False
        assert config.spark_submit.master == "yarn"
        assert config.spark_submit.deploy_mode == "client"

    def test_from_yaml_with_spark_submit(self, tmp_path):
        yaml_content = (
            "block_size_mb: 256\n"
            "spark_submit:\n"
            "  enabled: true\n"
            "  master: yarn\n"
            "  principal: user@REALM.COM\n"
            "  keytab: /etc/keytabs/user.keytab\n"
            "  queue: data-engineering\n"
            "  executor_memory: 4g\n"
            "  num_executors: 10\n"
            "  extra_conf:\n"
            "    spark.driver.memory: 2g\n"
        )
        yaml_file = tmp_path / "config.yaml"
        yaml_file.write_text(yaml_content)

        config = BeekeeperConfig.from_yaml(yaml_file)

        assert config.block_size_mb == 256
        assert config.spark_submit.enabled is True
        assert config.spark_submit.principal == "user@REALM.COM"
        assert config.spark_submit.keytab == "/etc/keytabs/user.keytab"
        assert config.spark_submit.queue == "data-engineering"
        assert config.spark_submit.executor_memory == "4g"
        assert config.spark_submit.num_executors == 10
        assert config.spark_submit.extra_conf == {"spark.driver.memory": "2g"}

    def test_merge_cli_overrides_preserves_spark_submit(self):
        config = BeekeeperConfig()
        config.spark_submit = SparkSubmitConfig(enabled=True, queue="my-queue")
        new_config = config.merge_cli_overrides(block_size_mb=256)
        assert new_config.spark_submit.enabled is True
        assert new_config.spark_submit.queue == "my-queue"


class TestSparkSubmitConfig:
    def test_defaults(self):
        cfg = SparkSubmitConfig()
        assert cfg.enabled is False
        assert cfg.master == "yarn"
        assert cfg.deploy_mode == "client"
        assert cfg.principal is None
        assert cfg.keytab is None
        assert cfg.queue is None
        assert cfg.script_path == "run_beekeeper.py"
        assert cfg.extra_conf == {}

    def test_from_dict_full(self):
        data = {
            "enabled": True,
            "master": "yarn",
            "deploy_mode": "cluster",
            "principal": "user@REALM",
            "keytab": "/etc/user.keytab",
            "queue": "prod",
            "archives": "/opt/env.tar.gz#env",
            "python_env": "./env/bin/python",
            "executor_memory": "8g",
            "num_executors": 20,
            "executor_cores": 4,
            "driver_memory": "4g",
            "script_path": "/opt/beekeeper/run_beekeeper.py",
            "extra_conf": {"spark.yarn.kerberos.relogin.period": "1h"},
        }
        cfg = SparkSubmitConfig.from_dict(data)
        assert cfg.enabled is True
        assert cfg.deploy_mode == "cluster"
        assert cfg.principal == "user@REALM"
        assert cfg.num_executors == 20
        assert cfg.extra_conf == {"spark.yarn.kerberos.relogin.period": "1h"}

    def test_from_dict_ignores_unknown_keys(self):
        cfg = SparkSubmitConfig.from_dict({"enabled": True, "unknown_key": "value"})
        assert cfg.enabled is True
        assert not hasattr(cfg, "unknown_key")
