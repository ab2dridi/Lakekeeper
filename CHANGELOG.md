# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [0.0.2] - 2026-02-24

### Fixed

- **Cluster mode infinite re-submission loop** — in `deploy_mode: cluster`, the YARN driver runs on a remote node and does not inherit edge-node environment variables. The `LAKEKEEPER_SUBMITTED` guard is now propagated explicitly via `spark.yarn.appMasterEnv.LAKEKEEPER_SUBMITTED=1` injected into the spark-submit command, preventing the driver from re-launching itself.
- **Backup table creation** — `CREATE EXTERNAL TABLE … LIKE` is not supported by SparkSQL. Replaced with `SHOW CREATE TABLE` (get original DDL) + regex name substitution + `ALTER TABLE SET TBLPROPERTIES ('external.table.purge'='false')`. This also correctly handles all storage properties (SerDe, row format, compression).

### Added

- **EXTERNAL table guard** — MANAGED tables are now detected at analysis time and skipped automatically with a clear message. Only `EXTERNAL` tables are compacted (rename-swap is unsafe on managed tables).
- **`extra_files` in spark_submit config** — new list field rendered as `--files file1,file2` in the spark-submit command, useful for distributing cluster config files (`hive-site.xml`, `hdfs-site.xml`) to executors.
- **`--config-file` on main group** — the option can now be placed before the subcommand name (`lakekeeper --config-file lakekeeper.yaml compact …`) as well as after it (backward compatible).

[0.0.2]: https://github.com/ab2dridi/Lakekeeper/releases/tag/v0.0.2

---

## [0.0.1] - 2026-02-24

### Added

- **Core compaction engine** — safe compaction of Hive external tables via HDFS rename swap strategy. The table's Metastore location never changes, preserving Apache Atlas lineage and catalog properties.
- **Per-partition compaction** — only partitions exceeding the small-file threshold are compacted; well-sized partitions are skipped.
- **Zero-copy backups** — `CREATE EXTERNAL TABLE LIKE` pointing to the original HDFS location, no data duplication.
- **Row count verification** — aborts and rolls back automatically if row counts do not match after compaction.
- **CLI** (`lakekeeper`) with four commands:
  - `analyze` — dry-run analysis, reports which tables and partitions need compaction.
  - `compact` — runs compaction on a table, a list of tables, or a full database.
  - `rollback` — restores a table to its exact pre-compaction state.
  - `cleanup` — removes backup tables and reclaims HDFS space; supports age filtering (`--older-than 7d`).
- **Automatic spark-submit launch** — when `spark_submit.enabled: true` is set in the YAML config, the CLI automatically builds and executes the full `spark-submit` command (Kerberos principal/keytab, YARN queue, conda archives, executor resources, extra `--conf` pairs). The `LAKEKEEPER_SUBMITTED` environment variable prevents infinite re-submission.
- **YAML configuration** — all parameters configurable via a YAML file; CLI flags override YAML values.
- **Python 3.9+ support** — compatible with Cloudera CDP 7.1.9, Hortonworks HDP, and vanilla Hadoop distributions.

[0.0.1]: https://github.com/ab2dridi/Lakekeeper/releases/tag/v0.0.1
