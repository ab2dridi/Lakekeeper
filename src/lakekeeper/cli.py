"""Click CLI for Lakekeeper."""

from __future__ import annotations

import os
import subprocess
import sys

import click

from lakekeeper import __version__
from lakekeeper.config import LakekeeperConfig
from lakekeeper.core.reporter import print_analysis_report, print_compaction_report
from lakekeeper.models import CompactionStatus, SkipTableError

_SUBMITTED_ENV = "LAKEKEEPER_SUBMITTED"


def _build_config(ctx: click.Context) -> LakekeeperConfig:
    """Build config from YAML file and CLI overrides."""
    params = ctx.params
    # --config-file may appear before the subcommand (group level, stored in
    # ctx.obj) or after it (subcommand level, in ctx.params).
    # Subcommand-level takes precedence.
    config_file = params.get("config_file") or (ctx.obj or {}).get("config_file")

    if config_file:
        config = LakekeeperConfig.from_yaml(config_file)
    else:
        config = LakekeeperConfig()

    return config.merge_cli_overrides(**params)


def _extract_config_file_for_cluster(
    args: list[str], deploy_mode: str
) -> tuple[list[str], list[str]]:
    """Rewrite a local --config-file path so it works on a remote YARN driver.

    In ``--deploy-mode cluster`` the YARN driver runs on a remote node and
    cannot access local edge-node files.  When ``--config-file`` points to a
    local file this function:

    1. Adds the file path to the returned *extra_files* list so it gets
       distributed via ``spark-submit --files`` to the driver container's
       working directory.
    2. Rewrites the argument to just the basename, which is where YARN places
       distributed files.

    HDFS paths (``hdfs://…``) are left untouched — the driver can read them
    directly.  In ``client`` deploy mode the driver runs locally so no
    rewriting is needed.
    """
    if deploy_mode != "cluster":
        return args, []

    import pathlib

    args = list(args)
    extra_files: list[str] = []

    i = 0
    while i < len(args):
        arg = args[i]
        # "--config-file PATH" or "-c PATH"
        if arg in ("--config-file", "-c") and i + 1 < len(args):
            path = args[i + 1]
            if not path.startswith("hdfs://") and os.path.isfile(path):
                args[i + 1] = pathlib.Path(path).name
                extra_files.append(path)
            break
        # "--config-file=PATH"
        if arg.startswith("--config-file="):
            path = arg.split("=", 1)[1]
            if not path.startswith("hdfs://") and os.path.isfile(path):
                args[i] = f"--config-file={pathlib.Path(path).name}"
                extra_files.append(path)
            break
        i += 1

    return args, extra_files


def _maybe_submit(config: LakekeeperConfig) -> None:
    """Launch via spark-submit if configured and not already running inside a submission."""
    if not config.spark_submit.enabled:
        return
    if os.environ.get(_SUBMITTED_ENV):
        return
    import dataclasses

    from lakekeeper.utils.spark import build_spark_submit_command

    # In --deploy-mode cluster the driver runs in a YARN container on a remote
    # node, so the env var set on the edge node is not inherited.  Pass it
    # explicitly as a YARN application-master environment variable so the guard
    # works regardless of deploy mode.
    submit_cfg = dataclasses.replace(
        config.spark_submit,
        extra_conf={
            **config.spark_submit.extra_conf,
            f"spark.yarn.appMasterEnv.{_SUBMITTED_ENV}": "1",
        },
    )

    # In --deploy-mode cluster, the YARN driver cannot read local edge-node
    # files.  Ship the config file via --files and rewrite the CLI arg to use
    # just the basename so the remote driver finds it in its working directory.
    lakekeeper_args, extra_cfg_files = _extract_config_file_for_cluster(
        sys.argv[1:], submit_cfg.deploy_mode
    )
    if extra_cfg_files:
        submit_cfg = dataclasses.replace(
            submit_cfg,
            extra_files=list(submit_cfg.extra_files or []) + extra_cfg_files,
        )

    cmd = build_spark_submit_command(submit_cfg, lakekeeper_args)
    click.echo(f"Launching via spark-submit: {' '.join(cmd)}")
    env = os.environ.copy()
    env[_SUBMITTED_ENV] = "1"
    result = subprocess.run(cmd, env=env)  # noqa: S603
    sys.exit(result.returncode)


def _get_engine(config: LakekeeperConfig):  # noqa: ANN202
    """Create a HiveExternalEngine with SparkSession."""
    from lakekeeper.engine.hive_external import HiveExternalEngine
    from lakekeeper.utils.spark import get_or_create_spark_session

    spark = get_or_create_spark_session()
    return HiveExternalEngine(spark, config)


def _resolve_tables(config: LakekeeperConfig, engine) -> list[tuple[str, str]]:  # noqa: ANN001
    """Resolve which tables to process.

    Returns:
        List of (database, table_name) tuples.
    """
    tables = []

    if config.table:
        parts = config.table.split(".")
        if len(parts) != 2:  # noqa: PLR2004
            click.echo(f"Error: table must be in format 'database.table', got '{config.table}'", err=True)
            sys.exit(1)
        tables.append((parts[0], parts[1]))

    elif config.tables:
        for t in config.tables:
            parts = t.strip().split(".")
            if len(parts) != 2:  # noqa: PLR2004
                click.echo(f"Error: table must be in format 'database.table', got '{t}'", err=True)
                sys.exit(1)
            tables.append((parts[0], parts[1]))

    elif config.database:
        table_names = engine.list_tables(config.database)
        tables = [(config.database, t) for t in table_names]

    else:
        click.echo("Error: must specify --database, --table, or --tables", err=True)
        sys.exit(1)

    return tables


@click.group()
@click.version_option(version=__version__, prog_name="lakekeeper")
@click.option("--config-file", "-c", default=None, metavar="PATH", help="YAML configuration file.")
@click.pass_context
def main(ctx: click.Context, config_file: str | None) -> None:
    """Lakekeeper - Safe compaction of Hive external tables."""
    ctx.ensure_object(dict)
    ctx.obj["config_file"] = config_file


@main.command()
@click.option("--database", "-d", help="Database to analyze.")
@click.option("--table", "-t", help="Specific table (format: db.table).")
@click.option("--tables", help="Comma-separated list of tables (format: db.t1,db.t2).")
@click.option("--block-size", "block_size_mb", type=int, help="Target block size in MB.")
@click.option("--ratio-threshold", "compaction_ratio_threshold", type=float, help="Compaction ratio threshold.")
@click.option("--config-file", "-c", help="YAML configuration file.")
@click.option("--log-level", help="Log level (DEBUG, INFO, WARNING, ERROR).")
@click.pass_context
def analyze(ctx: click.Context, **kwargs: str | None) -> None:
    """Analyze tables and report compaction needs (dry-run)."""
    config = _build_config(ctx)
    if kwargs.get("tables"):
        config = config.merge_cli_overrides(tables=kwargs["tables"].split(","))
    _maybe_submit(config)
    config.setup_logging()

    engine = _get_engine(config)
    tables = _resolve_tables(config, engine)

    click.echo(f"Analyzing {len(tables)} table(s)...\n")
    for database, table_name in tables:
        try:
            table_info = engine.analyze(database, table_name)
        except SkipTableError as e:
            click.echo(f"  Skipping {database}.{table_name}: {e}")
            continue
        print_analysis_report(table_info)


@main.command()
@click.option("--database", "-d", help="Database to compact.")
@click.option("--table", "-t", help="Specific table (format: db.table).")
@click.option("--tables", help="Comma-separated list of tables (format: db.t1,db.t2).")
@click.option("--block-size", "block_size_mb", type=int, help="Target block size in MB.")
@click.option("--ratio-threshold", "compaction_ratio_threshold", type=float, help="Compaction ratio threshold.")
@click.option("--dry-run", is_flag=True, help="Analyze only, do not compact.")
@click.option("--config-file", "-c", help="YAML configuration file.")
@click.option("--log-level", help="Log level (DEBUG, INFO, WARNING, ERROR).")
@click.option(
    "--sort-columns",
    "sort_columns_str",
    default=None,
    help="Sort columns before coalescing (comma-separated, e.g. 'date,user_id'). Only applies when --table is used.",
)
@click.pass_context
def compact(ctx: click.Context, sort_columns_str: str | None = None, **kwargs: str | None) -> None:
    """Compact Hive external tables."""
    config = _build_config(ctx)
    if kwargs.get("tables"):
        config = config.merge_cli_overrides(tables=kwargs["tables"].split(","))
    # CLI --sort-columns overrides YAML sort_columns for the specified table
    if sort_columns_str and config.table:
        sort_cols = [c.strip() for c in sort_columns_str.split(",") if c.strip()]
        if sort_cols:
            config.sort_columns[config.table] = sort_cols
    _maybe_submit(config)
    config.setup_logging()

    engine = _get_engine(config)
    tables = _resolve_tables(config, engine)

    click.echo(f"Processing {len(tables)} table(s)...\n")
    any_failed = False
    for database, table_name in tables:
        try:
            table_info = engine.analyze(database, table_name)
        except SkipTableError as e:
            click.echo(f"  Skipping {database}.{table_name}: {e}")
            continue
        print_analysis_report(table_info)

        if not table_info.needs_compaction:
            click.echo(f"  Skipping {table_info.full_name} - no compaction needed.\n")
            continue

        if config.dry_run:
            click.echo(f"  [DRY RUN] Would compact {table_info.full_name}\n")
            continue

        click.echo(f"  Creating backup for {table_info.full_name}...")
        try:
            backup_info = engine.create_backup(table_info)
        except Exception as e:
            click.echo(f"  Error creating backup for {table_info.full_name}: {e}", err=True)
            any_failed = True
            continue
        click.echo(f"  Backup created: {backup_info.backup_table}")

        click.echo(f"  Compacting {table_info.full_name}...")
        report = engine.compact(table_info, backup_info)
        print_compaction_report(report)

        if report.status == CompactionStatus.FAILED:
            any_failed = True

    if any_failed:
        sys.exit(1)


@main.command()
@click.option("--table", "-t", required=True, help="Table to rollback (format: db.table).")
@click.option("--config-file", "-c", help="YAML configuration file.")
@click.option("--log-level", help="Log level (DEBUG, INFO, WARNING, ERROR).")
@click.pass_context
def rollback(ctx: click.Context, **kwargs: str | None) -> None:
    """Rollback a table to its pre-compaction state."""
    config = _build_config(ctx)
    _maybe_submit(config)
    config.setup_logging()

    table = config.table
    if not table or "." not in table:
        click.echo("Error: --table must be in format 'database.table'", err=True)
        sys.exit(1)

    database, table_name = table.split(".", 1)
    engine = _get_engine(config)

    click.echo(f"Rolling back {table}...")
    used_backup = engine.rollback(database, table_name)
    click.echo(f"Rollback complete for {table} (backup used: {used_backup.backup_table}).")


@main.command()
@click.option("--database", "-d", help="Database to cleanup.")
@click.option("--table", "-t", help="Specific table (format: db.table).")
@click.option("--older-than", help="Only clean backups older than duration (e.g., 7d).")
@click.option("--config-file", "-c", help="YAML configuration file.")
@click.option("--log-level", help="Log level (DEBUG, INFO, WARNING, ERROR).")
@click.pass_context
def cleanup(ctx: click.Context, **kwargs: str | None) -> None:
    """Clean up backup tables and old compacted data."""
    config = _build_config(ctx)
    _maybe_submit(config)
    config.setup_logging()

    older_than = kwargs.get("older_than")
    older_than_days = _parse_duration(older_than) if older_than else None

    engine = _get_engine(config)

    if config.table:
        parts = config.table.split(".")
        if len(parts) != 2:  # noqa: PLR2004
            click.echo("Error: --table must be in format 'database.table'", err=True)
            sys.exit(1)
        database, table_name = parts
        cleaned = engine.cleanup(database, table_name, older_than_days)
        click.echo(f"Cleaned {cleaned} backup(s) for {config.table}.")

    elif config.database:
        table_names = engine.list_tables(config.database)
        total_cleaned = 0
        for table_name in table_names:
            cleaned = engine.cleanup(config.database, table_name, older_than_days)
            total_cleaned += cleaned
        total_cleaned += engine.cleanup_orphan_backups(config.database)
        click.echo(f"Cleaned {total_cleaned} backup(s) in database {config.database}.")

    else:
        click.echo("Error: must specify --database or --table", err=True)
        sys.exit(1)


def _parse_duration(duration_str: str) -> int:
    """Parse a duration string like '7d' into days.

    Args:
        duration_str: Duration string (e.g., '7d', '30d').

    Returns:
        Number of days.

    Raises:
        click.BadParameter: If format is invalid.
    """
    duration_str = duration_str.strip().lower()
    if duration_str.endswith("d"):
        try:
            return int(duration_str[:-1])
        except ValueError:
            pass
    msg = f"Invalid duration format: '{duration_str}'. Use format like '7d'."
    raise click.BadParameter(msg)
