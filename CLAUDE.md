# Lakekeeper — Project Memory for Claude Code

## Project overview

**Lakekeeper** compacts Hive external tables on Kerberized on-premises Hadoop clusters
(primary target: Cloudera CDP 7.1.9 SP1 CHF13, PySpark 3.3.2, Python 3.9, Hive 3).

PyPI package name: `lakekeeper` (the repo dir is still named `BeeKeeper`).
GitHub: https://github.com/ab2dridi/Lakekeeper

## Key architectural decisions

### What Lakekeeper does
1. **Analyze** — `DESCRIBE FORMATTED` + HDFS file counts to detect small-file tables
2. **Backup** — `SHOW CREATE TABLE` DDL clone pointing to the same HDFS path (zero-copy)
3. **Compact** — Spark coalesce to a temp sibling dir, row-count verification, atomic HDFS rename
4. **Rollback** — HDFS rename back from `__old_TS/` to original path, drop backup table
5. **Cleanup** — Delete `__old_TS/` dirs and drop backup tables from Metastore

### EXTERNAL-only constraint
Only `EXTERNAL` tables are supported. Managed tables are rejected at analysis time
(`SkipTableError`). Reason: the HDFS rename swap is unsafe on managed tables where the
Metastore controls the data lifecycle.

### Zero-copy backup strategy
`CREATE EXTERNAL TABLE … LIKE …` is **not supported** by SparkSQL.
Use `SHOW CREATE TABLE <original>` → regex-substitute the table name → execute DDL →
`ALTER TABLE SET TBLPROPERTIES ('external.table.purge'='false')`.

### external.table.purge behaviour
- CDP clusters default to `external.table.purge = true` on real tables.
- Demo scripts explicitly create with `false` to preserve data during testing.
- Backup tables are **always** set to `false` via `ALTER TABLE SET TBLPROPERTIES`
  regardless of the original table's setting.
- Lakekeeper never modifies the original table's `external.table.purge`.

### deploy_mode: cluster guard
In `--deploy-mode cluster`, the YARN driver runs on a remote node and does not inherit
edge-node env vars. `LAKEKEEPER_SUBMITTED=1` is propagated via
`spark.yarn.appMasterEnv.LAKEKEEPER_SUBMITTED=1` injected into `extra_conf` at
submit time (see `cli.py::_maybe_submit`).

### --config-file placement
`--config-file / -c` lives on the **main group** (before the subcommand):
```
lakekeeper --config-file lakekeeper.yaml compact --table mydb.events
```
It is also accepted on each subcommand for backward compatibility.

## Cluster environment constraints

| Item | Value |
|---|---|
| Hadoop distribution | Cloudera CDP 7.1.9 SP1 CHF13 |
| PySpark | 3.3.2 |
| Python | 3.9 |
| Hive | 3.x (HMS) |
| Deploy mode | always `--deploy-mode cluster` |
| Auth | Kerberos (principal + keytab) |
| User rights | No CREATE DATABASE; only writes to existing databases |

## Common spark_submit YAML patterns

```yaml
spark_submit:
  enabled: true
  master: yarn
  deploy_mode: cluster
  principal: user@REALM.COM
  keytab: /etc/security/keytabs/user.keytab
  queue: my-queue
  archives: /hdfs/path/lakekeeper_env.tar.gz#lakekeeper_env
  python_env: ./lakekeeper_env/bin/python
  executor_memory: 4g
  num_executors: 10
  executor_cores: 2
  driver_memory: 2g
  script_path: /hdfs/path/run_lakekeeper.py
  extra_conf:
    spark.yarn.kerberos.relogin.period: 1h
    spark.yarn.security.tokens.hive.enabled: "false"
  extra_files:
    - /etc/hive/conf.cloudera.hive/hive-site.xml
    - /etc/hive/conf.cloudera.hive/hdfs-site.xml
```

## Development workflow

```bash
# Env: Python 3.12 (dev), test env lakekeeper_test (Python 3.9 + PySpark 3.3.2 + Java 17)
pip install -e ".[dev]"

# Lint
ruff check src/ tests/
ruff format --check src/ tests/

# Unit tests (fast, no Spark)
pytest tests/ --ignore=tests/integration -v

# Integration tests (needs lakekeeper_test conda env with Java 17)
conda run -n lakekeeper_test python tests/integration/test_local_cluster.py
```

## Package publishing

```bash
# Build
python -m build

# Publish to PyPI (need token)
python -m twine upload dist/lakekeeper-<version>*
```

Current published version: 0.0.1. Next planned: 0.0.2 (cluster-mode fix + backup fix).

## Files of note

| File | Purpose |
|---|---|
| `src/lakekeeper/cli.py` | Click CLI — entry point, spark-submit wrapper, SkipTableError handling |
| `src/lakekeeper/core/analyzer.py` | DESCRIBE FORMATTED parsing, EXTERNAL check, compaction threshold |
| `src/lakekeeper/core/backup.py` | SHOW CREATE TABLE DDL clone, partition backup |
| `src/lakekeeper/core/compactor.py` | Coalesce + rename swap + row verification |
| `src/lakekeeper/engine/hive_external.py` | Orchestrates analyzer/backup/compactor/cleanup |
| `src/lakekeeper/config.py` | LakekeeperConfig + SparkSubmitConfig dataclasses |
| `src/lakekeeper/models.py` | TableInfo, PartitionInfo, BackupInfo, SkipTableError |
| `src/lakekeeper/utils/spark.py` | spark-submit command builder, SparkSession helper |
| `src/lakekeeper/utils/hdfs.py` | HDFS file count/size via JVM (Hadoop FileSystem API) |
| `run_lakekeeper.py` | Entry-point script shipped to HDFS for spark-submit |
| `demo/RUNBOOK.md` | End-to-end operational guide |
| `demo/create_*.py` | Data generation scripts for demo tables |
| `tests/integration/test_local_cluster.py` | Local PySpark 3.3.2 + Derby HMS integration test |
