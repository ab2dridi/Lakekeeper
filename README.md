# Lakekeeper

> Safe compaction of Hive external tables on on-premises Kerberized Hadoop clusters.

[![PyPI version](https://img.shields.io/pypi/v/lakekeeper.svg)](https://pypi.org/project/lakekeeper/)
[![Python](https://img.shields.io/pypi/pyversions/lakekeeper.svg)](https://pypi.org/project/lakekeeper/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

---

## The problem

On Hadoop clusters using Hive external tables with PySpark, data pipelines
accumulate thousands of small files over time (e.g. 65,000 files for 3 GB of
data). This pattern degrades read performance, overloads the HDFS NameNode,
and slows down all downstream queries.

The root cause is that Spark writes one file per partition task by default,
and incremental pipelines append rather than rewrite. Common tools like
`INSERT OVERWRITE` or `saveAsTable` solve the small-file problem but destroy
metadata cataloging properties (Apache Atlas lineage, table location in the
Hive Metastore), making them unsuitable for production use on managed clusters.

Lakekeeper solves this **without touching the table's Metastore location**.

---

## Solution

Lakekeeper compacts Hive external tables safely:

- **No `saveAsTable`** — the table's Metastore location never changes, preserving lineage and catalog properties (Apache Atlas and compatible systems)
- **Zero-copy backups** — DDL clone (`SHOW CREATE TABLE`) pointing to the original location, no data duplication
- **External tables only** — MANAGED and Iceberg tables are detected and skipped automatically
- **Per-partition compaction** — only compacts partitions that exceed the small-file threshold, untouched partitions are skipped
- **Dynamic target file count** — computed from actual data size and configured HDFS block size
- **Row count verification** — aborts and rolls back automatically if counts do not match after compaction
- **Compression codec preservation** — detects the source table's codec from `TBLPROPERTIES` (`parquet.compression` / `orc.compress`) and passes it explicitly to the writer; Spark's session default is never silently applied
- **Sort order preservation** — optionally re-sorts data before coalescing to restore predicate-pruning efficiency; priority: CLI `--sort-columns` > YAML per-table config > DDL `SORTED BY` auto-detection
- **Skewed distribution detection** — uses `min(avg, median)` as the effective file size; catches tables where a few large files inflate the average while dozens of tiny files go undetected

---

## Requirements

- Python >= 3.9
- Apache Spark (PySpark) accessible on the cluster
- Hive Metastore with external table support
- HDFS as the underlying storage

Tested on Cloudera CDP 7.1.9. Compatible with any on-premises Hadoop distribution
(Hortonworks HDP, Apache Ambari, vanilla Hadoop) that exposes a standard Hive
Metastore and HDFS filesystem.

---

## Installation

```bash
pip install lakekeeper
```

For development:

```bash
git clone https://github.com/ab2dridi/Lakekeeper.git
cd BeeKeeper
pip install ".[dev]"
```

---

## End-to-end usage

### Scenario 1 — Local cluster (no Kerberos)

Suitable for development environments or clusters without Kerberos authentication.

```bash
# 1. Install
pip install lakekeeper

# 2. Analyze — see which tables need compaction (no writes, safe to run anytime)
lakekeeper analyze --database mydb

# 3. Compact a specific table
lakekeeper compact --table mydb.events

# 4. If something went wrong, rollback to the original state
lakekeeper rollback --table mydb.events

# 5. Once you're confident, remove the backup to free up disk space
lakekeeper cleanup --table mydb.events
```

### Scenario 2 — On-premises Kerberized cluster (YAML config)

On a Kerberized cluster, configure `spark_submit` in a YAML file. The
`lakekeeper` CLI automatically builds and executes the `spark-submit` command —
no need to write it manually.

**Step 1 — Create the Python environment to ship to the cluster**

```bash
conda create -n lakekeeper_env python=3.9 -y
conda activate lakekeeper_env
pip install lakekeeper
conda-pack -o lakekeeper_env.tar.gz
```

**Step 2 — Generate a starter config file**

```bash
# Generate a commented template (then edit the values for your cluster)
lakekeeper generate-config --output lakekeeper.yaml
```

Or copy [`lakekeeper.example.yaml`](lakekeeper.example.yaml) from the repository root.

**Step 2b — Edit the config file**

```yaml
# lakekeeper.yaml
block_size_mb: 128
compaction_ratio_threshold: 10.0
log_level: INFO

spark_submit:
  enabled: true
  master: yarn
  deploy_mode: client
  principal: myuser@MY.REALM.COM
  keytab: /etc/security/keytabs/myuser.keytab
  queue: data-engineering
  archives: /opt/lakekeeper_env.tar.gz#lakekeeper_env
  python_env: ./lakekeeper_env/bin/python
  executor_memory: 4g
  num_executors: 10
  executor_cores: 2
  driver_memory: 2g
  script_path: /opt/lakekeeper/run_lakekeeper.py
  extra_conf:
    spark.yarn.kerberos.relogin.period: 1h
```

**Step 3 — Run**

```bash
# Analyze (dry-run, no writes)
lakekeeper --config-file lakekeeper.yaml analyze --database mydb

# Compact a single table
lakekeeper --config-file lakekeeper.yaml compact --table mydb.events

# Compact multiple tables
lakekeeper --config-file lakekeeper.yaml compact --tables mydb.events,mydb.users

# Compact an entire database
lakekeeper --config-file lakekeeper.yaml compact --database mydb

# Rollback if needed
lakekeeper --config-file lakekeeper.yaml rollback --table mydb.events

# Cleanup backups older than 7 days
lakekeeper --config-file lakekeeper.yaml cleanup --database mydb --older-than 7d
```

Under the hood, Lakekeeper builds and executes:

```
spark-submit --master yarn --deploy-mode client \
  --principal myuser@MY.REALM.COM \
  --keytab /etc/security/keytabs/myuser.keytab \
  --conf spark.yarn.queue=data-engineering \
  --archives /opt/lakekeeper_env.tar.gz#lakekeeper_env \
  --conf spark.pyspark.python=./lakekeeper_env/bin/python \
  --executor-memory 4g --num-executors 10 \
  /opt/lakekeeper/run_lakekeeper.py compact --table mydb.events
```

### Scenario 3 — spark-submit manually

For one-off runs or when Lakekeeper is not installed on the edge node.

```bash
spark-submit \
  --master yarn \
  --deploy-mode client \
  --principal myuser@MY.REALM.COM \
  --keytab /etc/security/keytabs/myuser.keytab \
  --conf spark.yarn.queue=my-queue \
  --archives lakekeeper_env.tar.gz#lakekeeper_env \
  --conf spark.pyspark.python=./lakekeeper_env/bin/python \
  run_lakekeeper.py compact --database mydb --block-size 128
```

---

## CLI reference

```
lakekeeper [OPTIONS] COMMAND [ARGS]...

Options:
  -c, --config-file PATH  YAML configuration file.
  --version               Show version and exit.
  --help                  Show help and exit.

Commands:
  analyze          Analyze tables and report compaction needs (dry-run, no writes).
  compact          Compact Hive external tables.
  rollback         Rollback a table to its pre-compaction state.
  cleanup          Remove backup tables and reclaim HDFS space.
  generate-config  Generate a commented lakekeeper.yaml configuration template.
```

> **`--config-file` placement:** Pass it before the subcommand name:
> `lakekeeper --config-file lakekeeper.yaml compact --table mydb.events`
> It can also be placed after the subcommand for backward compatibility.

### analyze

```bash
lakekeeper analyze --database mydb
lakekeeper analyze --table mydb.events
lakekeeper analyze --tables mydb.events,mydb.users
lakekeeper analyze --table mydb.events --block-size 256 --ratio-threshold 5
```

### compact

```bash
lakekeeper compact --database mydb
lakekeeper compact --table mydb.events
lakekeeper compact --tables mydb.events,mydb.users
lakekeeper compact --database mydb --block-size 256 --ratio-threshold 5
lakekeeper compact --database mydb --dry-run                          # analyze only, no writes
lakekeeper compact --table mydb.events --sort-columns date,user_id   # re-sort before coalescing
lakekeeper compact --table mydb.events --analyze-stats               # refresh Metastore stats after
lakekeeper compact --table mydb.events --no-analyze-stats            # disable stats (overrides YAML)
```

### rollback

```bash
lakekeeper rollback --table mydb.events
```

### cleanup

```bash
lakekeeper cleanup --table mydb.events              # remove all backups for a table
lakekeeper cleanup --database mydb --older-than 7d  # remove backups older than 7 days
```

---

## Configuration reference

### Lakekeeper parameters

| Parameter | Default | CLI flag | Description |
|---|---|---|---|
| `block_size_mb` | `128` | `--block-size` | Target HDFS block size in MB |
| `compaction_ratio_threshold` | `10.0` | `--ratio-threshold` | Compact if `min(avg, median)` file size < block_size / ratio |
| `backup_prefix` | `__bkp` | — | Prefix for backup table names |
| `dry_run` | `false` | `--dry-run` | Analyze only, no writes |
| `log_level` | `INFO` | `--log-level` | `DEBUG`, `INFO`, `WARNING`, `ERROR` |
| `sort_columns` | `{}` | `--sort-columns` | Per-table sort columns (see [Sort order](#sort-order-preservation)) |
| `analyze_after_compaction` | `false` | `--analyze-stats` / `--no-analyze-stats` | Run `ANALYZE TABLE COMPUTE STATISTICS` after each successful compaction |

### spark_submit parameters

| Parameter | Default | Description |
|---|---|---|
| `enabled` | `false` | Enable automatic spark-submit launch |
| `submit_command` | `spark-submit` | Submit binary name (use `spark3-submit` on clusters where `spark-submit` points to Spark 2) |
| `master` | `yarn` | Spark master URL |
| `deploy_mode` | `client` | `client` or `cluster` |
| `principal` | — | Kerberos principal (e.g. `user@REALM.COM`) |
| `keytab` | — | Path to the Kerberos keytab file |
| `queue` | — | YARN queue name (`spark.yarn.queue`) |
| `archives` | — | `--archives` for the conda-packed Python env |
| `python_env` | — | Python path inside the archive (`spark.pyspark.python`) |
| `executor_memory` | — | `--executor-memory` (e.g. `4g`) |
| `num_executors` | — | `--num-executors` |
| `executor_cores` | — | `--executor-cores` |
| `driver_memory` | — | `--driver-memory` |
| `script_path` | `run_lakekeeper.py` | Path to the entry-point script passed to spark-submit |
| `extra_conf` | `{}` | Additional `--conf key=value` pairs |
| `extra_files` | `[]` | Files distributed to the driver via `--files` (e.g. `hive-site.xml`) |
| `py_files` | `[]` | Python dependencies distributed via `--py-files` (e.g. a wheel or zip) |

---

## How it works

### Compaction strategy — HDFS rename swap

Lakekeeper uses HDFS directory renames rather than `ALTER TABLE SET LOCATION`
to swap data. The table's Metastore location never changes — only the contents
of the HDFS directory are replaced in place. Lineage and cataloging properties
(Apache Atlas and compatible systems) are fully preserved.

#### Non-partitioned table

Given a table `mydb.events` at `hdfs:///warehouse/mydb/events/`:

```
Step 1 — Backup
  Metastore: mydb.__bkp_events_20240301_020000  →  hdfs:///warehouse/mydb/events/
             (external.table.purge=false)
  HDFS:      events/   (original files, untouched)

Step 2 — Write compacted data to a temp sibling directory
  HDFS:      events/                          ← original, still live
             events__compact_tmp_1709257200/  ← Spark writes here

Step 3 — Verify row count
  Counts differ → delete events__compact_tmp_1709257200/ and abort.
  Original data at events/ is never touched.

Step 4 — Atomic HDFS rename swap
  rename  events/                        →  events__old_1709257200/
  rename  events__compact_tmp_1709257200/ →  events/

Final state:
  events/                       ← compacted files (table still points here)
  events__old_1709257200/       ← original files (kept for rollback)
  __bkp_events_20240301_020000  ← backup table in Metastore
```

#### Partitioned table

The same rename swap is applied **partition by partition**, only for partitions
that exceed the compaction threshold:

```
Before:
  events/year=2024/month=01/   10 000 files, 1 GB  ← needs compaction
  events/year=2024/month=02/   3 files, 300 MB     ← skipped

After:
  events/year=2024/month=01/              ← 8 compacted files
  events/year=2024/month=01__old_TS/      ← original (kept for rollback)
  events/year=2024/month=02/              ← untouched
```

Readers of already-compacted partitions see the new files immediately while
readers of not-yet-processed partitions still see the original data. All reads
remain consistent throughout the operation.

### Rollback

```bash
lakekeeper rollback --table mydb.events
```

1. Finds the most recent backup table (`__bkp_events_*`)
2. Reads its Metastore location → `events__old_TS/` (the original data)
3. Deletes `events/` (the compacted data)
4. Renames `events__old_TS/` back to `events/`
5. Drops the backup table

The table is restored to exactly its pre-compaction state.

### Cleanup

```bash
lakekeeper cleanup --table mydb.events
```

1. Finds all `__bkp_events_*` backup tables
2. For each: deletes the `__old_*` HDFS directory it points to, then drops the backup table

**Cleanup is irreversible.** Once run, rollback is no longer possible for the cleaned backups.

---

## Sort order preservation

`coalesce()` does not preserve sort order. If the original table's files were
written sorted (e.g. by `date` or `user_id`) for predicate-pruning efficiency,
Lakekeeper can re-apply the sort before coalescing.

Three ways to configure sort columns, in descending priority:

### 1 — CLI (one table, ad-hoc)

```bash
lakekeeper compact --table mydb.events --sort-columns date,user_id
```

### 2 — YAML (per-table, persistent across runs)

```yaml
sort_columns:
  mydb.events: [date, user_id]
  mydb.logs:   [year, month, day]
```

### 3 — DDL auto-detect (zero config)

If the table was created with `CLUSTERED BY … SORTED BY …`, Lakekeeper reads
the `Sort Columns` field from `DESCRIBE FORMATTED` and sorts automatically —
no explicit configuration needed.

> **Note:** Sorting triggers a Spark shuffle before coalescing, which increases
> execution time and memory usage. Use it only for tables where sort order
> materially improves downstream query performance.

---

## Important considerations

### ⚠ Run during a maintenance window

Lakekeeper reads the table twice (once to count rows, once to write). Any rows
written by an active pipeline **between those two reads** will not appear in
the compacted output and will be lost after the rename swap.

**Always run Lakekeeper while source pipelines are stopped**, or schedule it
in a maintenance window.

### ⚠ 2× disk space required

During compaction, both the original and compacted data exist on HDFS simultaneously:
- `events/` — original files (until the rename swap)
- `events__compact_tmp_TS/` — compacted files being written

Ensure the HDFS parent directory quota allows **at least 2× the table size** before starting.

### ⚠ Do not delete `__old_*` directories manually

After a successful compaction, `events__old_TS/` is the rollback safety net.
Deleting it manually makes rollback impossible. Use `lakekeeper cleanup` instead.

### ⚠ Do not drop backup tables manually

Backup tables are created with `TBLPROPERTIES ('external.table.purge'='false')`
to prevent the Hive Metastore setting `external.table.purge=true` from deleting
the underlying HDFS data on `DROP TABLE`. Dropping a backup table manually
removes the Metastore pointer to `events__old_TS/` and prevents rollback.

> **Cloudera CDP note:** CDP clusters commonly set `external.table.purge=true`
> globally. The `purge=false` property on backup tables overrides this default.

### ⚠ Leftover staging directories block the next run

If a previous compaction crashed, it may have left a `events__compact_tmp_TS/`
or `events__old_TS/` directory behind. Lakekeeper **refuses to start** if either
path already exists. Resolve manually before retrying:

1. Inspect the leftover directory contents.
2. If it contains valid compacted data, check whether the rename swap completed and restore accordingly.
3. If it is stale or incomplete, delete it: `hdfs dfs -rm -r <path>`.

---

## Development

```bash
git clone https://github.com/ab2dridi/Lakekeeper.git
cd BeeKeeper
pip install ".[dev]"

# Lint
ruff check src/ tests/
ruff format --check src/ tests/

# Tests with coverage
pytest tests/ -v --cov=lakekeeper --cov-report=term-missing
```

---

## License

MIT — see [LICENSE](LICENSE) for details.
