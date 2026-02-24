# Beekeeper

Safe compaction tool for Hive external tables on Cloudera CDP.

## Problem

On Cloudera CDP 7.1.9, Hive external tables created via PySpark accumulate thousands of small files (e.g., 65,000 files for 3 GB). This degrades read performance, overloads the HDFS NameNode, and slows queries.

## Solution

Beekeeper compacts these tables **safely**:
- No `saveAsTable` — preserves Atlas cataloging properties
- Zero-copy backups — no data duplication
- Automatic partition detection — only compacts partitions that need it
- Dynamic target file count — based on data size and HDFS block size
- Row count verification — automatic rollback on mismatch

## Installation

```bash
pip install .

# With dev dependencies
pip install ".[dev]"
```

## Usage

### Analyze tables (dry-run)

```bash
# Analyze all external tables in a database
beekeeper analyze --database mydb

# Analyze a specific table
beekeeper analyze --table mydb.mytable
```

### Compact tables

```bash
# Compact all external tables in a database
beekeeper compact --database mydb

# Compact a specific table
beekeeper compact --table mydb.mytable

# Compact multiple tables
beekeeper compact --tables mydb.t1,mydb.t2,mydb.t3

# With custom parameters
beekeeper compact --database mydb --block-size 256 --ratio-threshold 5

# Dry-run (analyze only)
beekeeper compact --database mydb --dry-run
```

### Rollback

```bash
beekeeper rollback --table mydb.mytable
```

### Cleanup backups

```bash
# Clean all backups for a table
beekeeper cleanup --table mydb.mytable

# Clean backups older than 7 days
beekeeper cleanup --database mydb --older-than 7d
```

### With spark-submit (manual)

Useful for one-off runs or when Beekeeper is not installed on the edge node.

```bash
# 1. Create conda environment
conda create -n beekeeper_env python=3.9 -y
conda activate beekeeper_env
pip install ./beekeeper
conda-pack -o beekeeper_env.tar.gz

# 2. Submit
spark-submit \
  --master yarn \
  --deploy-mode client \
  --principal myuser@MY.REALM.COM \
  --keytab /etc/security/keytabs/myuser.keytab \
  --conf spark.yarn.queue=my-queue \
  --archives beekeeper_env.tar.gz#beekeeper_env \
  --conf spark.pyspark.python=./beekeeper_env/bin/python \
  run_beekeeper.py compact --database mydb --block-size 128
```

### With the CLI on a Kerberized cluster (recommended)

On a Kerberized Cloudera CDP cluster, configure `spark_submit` in your YAML file.
The `beekeeper` CLI will automatically build and execute the `spark-submit` command.

```bash
beekeeper --config-file config.yaml compact --database mydb
# → spark-submit --master yarn --deploy-mode client --principal ... run_beekeeper.py compact --database mydb
```

**How it works:**

1. `beekeeper compact ...` reads the YAML config and detects `spark_submit.enabled: true`
2. It builds the full `spark-submit` command and executes it as a subprocess
3. `run_beekeeper.py` is called by spark-submit, which runs the same CLI code inside the cluster
4. The env variable `BEEKEEPER_SUBMITTED=1` prevents an infinite loop

## Configuration

### YAML config file

```yaml
block_size_mb: 128
compaction_ratio_threshold: 10.0
backup_prefix: "__bkp"
dry_run: false
log_level: INFO
```

### Full YAML with spark-submit (Kerberized cluster)

```yaml
block_size_mb: 128
compaction_ratio_threshold: 10.0
dry_run: false
log_level: INFO

spark_submit:
  enabled: true
  master: yarn
  deploy_mode: client
  principal: myuser@MY.REALM.COM
  keytab: /etc/security/keytabs/myuser.keytab
  queue: data-engineering
  archives: /opt/beekeeper_env.tar.gz#beekeeper_env
  python_env: ./beekeeper_env/bin/python
  executor_memory: 4g
  num_executors: 10
  executor_cores: 2
  driver_memory: 2g
  script_path: /opt/beekeeper/run_beekeeper.py
  extra_conf:
    spark.yarn.kerberos.relogin.period: 1h
    spark.dynamicAllocation.enabled: "false"
```

```bash
beekeeper compact --database mydb --config-file config.yaml
```

### Parameters

#### Beekeeper parameters

| Parameter | Default | Description |
|---|---|---|
| `block_size_mb` | 128 | Target HDFS block size in MB |
| `compaction_ratio_threshold` | 10.0 | Compact if avg file < block_size / ratio |
| `backup_prefix` | `__bkp` | Prefix for backup tables |
| `dry_run` | False | Analyze without compacting |
| `log_level` | INFO | Log level |

#### spark_submit parameters

| Parameter | Default | Description |
|---|---|---|
| `enabled` | `false` | Enable spark-submit mode |
| `master` | `yarn` | Spark master URL |
| `deploy_mode` | `client` | `client` or `cluster` |
| `principal` | — | Kerberos principal (e.g. `user@REALM.COM`) |
| `keytab` | — | Path to the Kerberos keytab file |
| `queue` | — | YARN queue (`spark.yarn.queue`) |
| `archives` | — | `--archives` argument for the conda env |
| `python_env` | — | Path to python inside the archive (`spark.pyspark.python`) |
| `executor_memory` | — | `--executor-memory` (e.g. `4g`) |
| `num_executors` | — | `--num-executors` |
| `executor_cores` | — | `--executor-cores` |
| `driver_memory` | — | `--driver-memory` |
| `script_path` | `run_beekeeper.py` | Path to the entry-point script passed to spark-submit |
| `extra_conf` | `{}` | Additional `--conf key=value` pairs |

## How it works

### Compaction strategy — HDFS rename swap

Beekeeper uses HDFS directory renames rather than `ALTER TABLE SET LOCATION` to swap data. This means **the table's Metastore location never changes** — only the contents of the HDFS directory are replaced. Atlas lineage and cataloging properties are fully preserved.

#### Non-partitioned table — step by step

Given a table `mydb.events` located at `hdfs:///warehouse/mydb/events/`:

```
Step 1 — Backup
  Metastore: mydb.__bkp_events_20240301_020000  →  hdfs:///warehouse/mydb/events/
             (TBLPROPERTIES external.table.purge=false)
  HDFS:      events/   (original files, untouched)

Step 2 — Write compacted data to a temp sibling directory
  HDFS:      events/                       ← original, still live
             events__compact_tmp_1709257200/  ← Spark writes here

Step 3 — Verify row count
  If counts differ: delete events__compact_tmp_1709257200/ and abort.
  The original data at events/ is never touched.

Step 4 — Atomic HDFS rename swap
  rename  events/                        →  events__old_1709257200/
  (update backup table Metastore         →  hdfs:///warehouse/mydb/events__old_1709257200/)
  rename  events__compact_tmp_1709257200/ →  events/

Final HDFS state:
  events/                     ← compacted data (table still points here, unchanged)
  events__old_1709257200/     ← original data (kept for rollback)
  __bkp_events_20240301_020000 table in Metastore
```

The table's location (`events/`) is now populated with compacted files. No ALTER TABLE was issued on the main table.

#### Partitioned table

The same swap is applied **partition by partition**, only for partitions that exceed the compaction threshold. Partitions that are already well-sized are skipped.

```
Before:
  events/year=2024/month=01/   10 000 files, 1 GB  ← needs compaction
  events/year=2024/month=02/   3 files, 300 MB     ← skipped

After:
  events/year=2024/month=01/              ← 8 compacted files
  events/year=2024/month=01__old_TS/      ← original (kept for rollback)
  events/year=2024/month=02/              ← untouched
```

During compaction of a partitioned table, **readers of already-compacted partitions see the new compact files** while readers of not-yet-processed partitions still see the original files. All data remains consistent throughout.

### Rollback

```bash
beekeeper rollback --table mydb.events
```

What happens:
1. Finds the most recent backup table (`__bkp_events_*`)
2. Reads the backup table's location — this is `events__old_TS/` (the original data)
3. **Deletes** `events/` (the compacted data)
4. Renames `events__old_TS/` back to `events/`
5. Drops the backup table

After rollback the table is in exactly its pre-compaction state.

### Cleanup

```bash
beekeeper cleanup --table mydb.events
```

What happens:
1. Finds all `__bkp_events_*` tables
2. For each backup: deletes the `__old_*` HDFS directory it points to, then drops the backup table

**Cleanup is irreversible.** Once run, rollback is no longer possible for the cleaned backups.

---

## Important considerations

### ⚠ Concurrent writes — run during a maintenance window

Beekeeper reads the table twice (once to count rows, once to write). Any rows written by an active pipeline **between those two reads** will not appear in the compacted output and will be lost after the rename swap.

**Always run Beekeeper while the source pipelines are stopped**, or schedule it in a maintenance window. Beekeeper will detect a row count mismatch if the gap is large enough to change the count, but a small write (fewer rows than rounding differences) may go undetected.

### ⚠ Disk quota — 2× space required

During compaction, both the original data and the compacted data exist on HDFS simultaneously:
- `events/` — original files (until the rename swap)
- `events__compact_tmp_TS/` — compacted files being written

Make sure the HDFS parent directory quota allows **at least 2× the size of the table** before starting.

### ⚠ Do not delete `__old_*` directories manually

After a successful compaction, `events__old_TS/` holds the original data and is the safety net for rollback. Deleting it manually makes rollback impossible. Use `beekeeper cleanup` to remove it once you are confident the compaction is correct.

### ⚠ Do not drop backup tables manually

Backup tables are created with `TBLPROPERTIES ('external.table.purge'='false')` specifically to prevent a Cloudera CDP cluster-wide setting (`external.table.purge=true`) from deleting their underlying HDFS data on `DROP TABLE`. Dropping a backup table manually via Hive/Beeline is safe because of this setting, but doing so removes the Metastore pointer to `events__old_TS/`, preventing Beekeeper from performing a rollback.

### ⚠ Leftover staging directories block the next run

If a previous compaction crashed between steps, it may have left a `events__compact_tmp_TS/` or `events__old_TS/` directory behind. Beekeeper **refuses to start** if either staging path already exists (to avoid silently overwriting data). You must resolve the situation manually before retrying:

1. Inspect what is in the leftover directory.
2. If it contains good compacted data, check whether the rename swap completed and restore accordingly.
3. If it is stale/incomplete, delete it with `hdfs dfs -rm -r <path>`.

---

## Development

```bash
# Install dev dependencies
pip install ".[dev]"

# Lint
ruff check src/ tests/
ruff format --check src/ tests/

# Test with coverage
pytest tests/ -v --cov=beekeeper --cov-report=term-missing
```
