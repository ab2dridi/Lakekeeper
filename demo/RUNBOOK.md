# Lakekeeper — End-to-End Demo Runbook

This runbook walks through the full compaction cycle on a real Kerberized
Hadoop cluster: data generation → analysis → compaction → verification →
cleanup (and rollback if needed).

---

## Prerequisites

| Requirement | Notes |
|---|---|
| Hadoop cluster with Kerberos | Tested on Cloudera CDP 7.1.9, HDP, vanilla Hadoop |
| Hive Metastore | External table support required |
| HDFS | Write access to the target database location |
| Python environment | `pip install lakekeeper` |
| Valid Kerberos keytab | `kinit` or keytab file |
| Existing Hive database | Ask your admin to create one if needed |

---

## Step 1 — Write the config file

Generate a fully-commented template, then edit the values:

```bash
lakekeeper generate-config --output lakekeeper.yaml
```

Or write `lakekeeper.yaml` manually on the edge node:

```yaml
# lakekeeper.yaml
block_size_mb: 128
compaction_ratio_threshold: 10.0
log_level: INFO

# Run ANALYZE TABLE COMPUTE STATISTICS after each successful compaction
# (refreshes rowCount / numFiles in the Metastore). Default: false.
# Can also be toggled at runtime: --analyze-stats / --no-analyze-stats
analyze_after_compaction: false

# Re-sort specific tables before coalescing to preserve predicate-pruning
# efficiency. Sorting triggers a Spark shuffle — only enable when needed.
# Can also be set per-run: --sort-columns col1,col2 (single table only)
# sort_columns:
#   <database>.lakekeeper_events: [date]

spark_submit:
  enabled: true
  # Use spark3-submit on clusters where spark-submit points to Spark 2.
  submit_command: spark-submit
  master: yarn
  deploy_mode: cluster
  principal: youruser@YOUR.REALM.COM
  keytab: /etc/security/keytabs/youruser.keytab
  queue: your-queue
  archives: /hdfs/path/to/lakekeeper_env.tar.gz#lakekeeper_env
  python_env: ./lakekeeper_env/bin/python
  executor_memory: 4g
  num_executors: 10
  executor_cores: 2
  driver_memory: 2g
  script_path: /hdfs/path/to/run_lakekeeper.py
  extra_conf:
    spark.yarn.kerberos.relogin.period: 1h
    spark.yarn.security.tokens.hive.enabled: "false"   # adjust to cluster policy
  extra_files:
    - /etc/hive/conf.cloudera.hive/hive-site.xml
    - /etc/hive/conf.cloudera.hive/hdfs-site.xml
  # Python wheels or zips distributed to executors via --py-files.
  # py_files:
  #   - /opt/mypackage.whl
```

> **Note — deploy_mode cluster:** In cluster mode the driver runs on a YARN
> node, not on the edge node. Lakekeeper automatically passes
> `spark.yarn.appMasterEnv.LAKEKEEPER_SUBMITTED=1` to the YARN AM so the
> re-submission guard works correctly.

---

## Step 2 — Build the Python environment (once)

```bash
conda create -n lakekeeper_env python=3.9 -y
conda activate lakekeeper_env
pip install lakekeeper
conda-pack -o lakekeeper_env.tar.gz

# Upload to HDFS
hdfs dfs -put lakekeeper_env.tar.gz /hdfs/path/to/lakekeeper_env.tar.gz
```

Also upload `run_lakekeeper.py` (from the repo root):

```bash
hdfs dfs -put run_lakekeeper.py /hdfs/path/to/run_lakekeeper.py
```

---

## Step 3 — Create the demo tables

Three demo scripts are available. Run the ones you want to test.

### Scenario A — Non-partitioned table

Creates `<database>.lakekeeper_flat` with **500 small files** (no partitions).

```bash
spark-submit \
  --master yarn --deploy-mode cluster \
  --principal youruser@YOUR.REALM.COM \
  --keytab /etc/security/keytabs/youruser.keytab \
  demo/create_nonpartitioned_table.py \
  --database <database>
```

Expected output:
```
  Table      : <database>.lakekeeper_flat
  Partitions : none
  Files      : 500
  Rows       : 300,000
```

---

### Scenario B — Single-partition table (date)

Creates `<database>.lakekeeper_events` with **3 date partitions × 200 files = 600 files**.

```bash
spark-submit \
  --master yarn --deploy-mode cluster \
  --principal youruser@YOUR.REALM.COM \
  --keytab /etc/security/keytabs/youruser.keytab \
  demo/create_single_partition_table.py \
  --database <database>
```

Expected output:
```
  Table      : <database>.lakekeeper_events
  Partitions : 3  (date)
  Files      : 600  (200 per partition)
  Rows       : 300,000
```

---

### Scenario C — Two-partition table (date + ref)

Creates `<database>.lakekeeper_events_2p` partitioned by `date` **and** `ref`
(reference category). Default: **3 dates × 3 refs × 100 files = 900 files**.

```bash
spark-submit \
  --master yarn --deploy-mode cluster \
  --principal youruser@YOUR.REALM.COM \
  --keytab /etc/security/keytabs/youruser.keytab \
  demo/create_two_partitions_table.py \
  --database <database>
```

To use custom ref values:
```bash
  demo/create_two_partitions_table.py --database <database> --refs FR,DE,US,GB
```

Expected output:
```
  Table      : <database>.lakekeeper_events_2p
  Partitions : 9  (3 dates × 3 refs)
  Files      : 900  (100 per partition)
  Rows       : 450,000
```

---

## Step 4 — Analyze (dry-run, no writes)

```bash
# Single table
lakekeeper --config-file lakekeeper.yaml analyze --table <database>.lakekeeper_flat
lakekeeper --config-file lakekeeper.yaml analyze --table <database>.lakekeeper_events
lakekeeper --config-file lakekeeper.yaml analyze --table <database>.lakekeeper_events_2p

# Entire database at once
lakekeeper --config-file lakekeeper.yaml analyze --database <database>
```

Sample output:
```
Table: <database>.lakekeeper_flat
  Status   : NEEDS COMPACTION
  Files    : 500
  Size     : 45.2 MB
  Avg file : 92 KB  (threshold: 12.8 MB)
```

---

## Step 5 — Compact

```bash
lakekeeper --config-file lakekeeper.yaml compact --table <database>.lakekeeper_flat
lakekeeper --config-file lakekeeper.yaml compact --table <database>.lakekeeper_events
lakekeeper --config-file lakekeeper.yaml compact --table <database>.lakekeeper_events_2p

# Or compact all demo tables at once
lakekeeper --config-file lakekeeper.yaml compact \
  --tables <database>.lakekeeper_flat,<database>.lakekeeper_events,<database>.lakekeeper_events_2p

# Re-sort before coalescing (preserves column sort order for predicate pruning)
lakekeeper --config-file lakekeeper.yaml compact \
  --table <database>.lakekeeper_events --sort-columns date

# Refresh Metastore statistics after compaction (updates rowCount / numFiles)
lakekeeper --config-file lakekeeper.yaml compact \
  --table <database>.lakekeeper_flat --analyze-stats
```

What happens internally:
1. Creates a zero-copy backup table (`__bkp_<table>_<timestamp>`)
2. Writes compacted data to a temp HDFS directory (`<table>__compact_tmp_<ts>`)
   - Compression codec is read from `TBLPROPERTIES` (`parquet.compression` /
     `orc.compress`) and passed explicitly to the writer — the session default
     is never silently applied.
   - If `sort_columns` is configured, data is sorted before `coalesce()`.
3. Verifies row count — aborts automatically if counts differ
4. Atomic HDFS rename swap: original → `<table>__old_<ts>`, compacted → `<table>`
5. Updates backup table pointer to `__old_<ts>` (rollback safety net)
6. If `analyze_after_compaction: true` (or `--analyze-stats`): runs
   `ANALYZE TABLE … COMPUTE STATISTICS` per compacted partition then
   at the table level to refresh Metastore stats.

---

## Step 6 — Verify

```bash
lakekeeper --config-file lakekeeper.yaml analyze --database <database>
```

Expected: all three tables show `OK` (no compaction needed), with file counts
reduced from hundreds to a handful.

---

## Step 7 — Cleanup

Once you are confident the compaction is correct, remove the backup data to
free HDFS space:

```bash
lakekeeper --config-file lakekeeper.yaml cleanup --table <database>.lakekeeper_flat
lakekeeper --config-file lakekeeper.yaml cleanup --table <database>.lakekeeper_events
lakekeeper --config-file lakekeeper.yaml cleanup --table <database>.lakekeeper_events_2p

# Or remove all backups older than 7 days across the database
lakekeeper --config-file lakekeeper.yaml cleanup --database <database> --older-than 7d
```

> **Cleanup is irreversible.** After cleanup, rollback is no longer possible
> for the removed backups.

---

## Rollback (if something went wrong)

If compaction produced an unexpected result, restore the table to its
exact pre-compaction state:

```bash
lakekeeper --config-file lakekeeper.yaml rollback --table <database>.lakekeeper_events
```

Rollback steps:
1. Finds the most recent backup table
2. Renames `<table>__old_<ts>/` back to `<table>/`
3. Drops the backup table

The table is restored to exactly the state it was in before `compact` ran.

---

## Leftover staging directories

If a previous run crashed, lakekeeper may have left temp directories:
- `<table>__compact_tmp_<ts>/`
- `<table>__old_<ts>/`

Lakekeeper refuses to start if either path exists. Inspect and clean manually:

```bash
hdfs dfs -ls hdfs:///path/to/db/<table>__*
hdfs dfs -rm -r hdfs:///path/to/db/<table>__compact_tmp_<ts>
```
