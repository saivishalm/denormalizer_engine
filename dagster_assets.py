"""
Dagster Software-Defined Assets — BU Denormalization Pipeline
=============================================================
Orchestrates two independent denormalization pipelines:
  • JSON pipeline : 25live_raw  → 25live_raw_denormalized
  • XML  pipeline : mycv_raw    → mycv_raw_denormalized

Reuses all existing pipeline components without modification:
  TrinoClient, ConfigLoader, DenormalizationEngine
"""

import os
import logging
import asyncio
from pathlib import Path
from typing import Any, Callable

from dagster import (
    AssetExecutionContext,
    AssetKey,
    ConfigurableResource,
    Definitions,
    EnvVar,
    Failure,
    MaterializeResult,
    MetadataValue,
    ScheduleDefinition,
    SourceAsset,
    asset,
    define_asset_job,
)

from src.core.config_loader import ConfigLoader
from src.core.denormalization_engine import DenormalizationEngine
from src.db.trino_client import TrinoClient

# ---------------------------------------------------------------------------
# Dagster resource — wraps TrinoClient + ConfigLoader + DenormalizationEngine
# ---------------------------------------------------------------------------

class TrinoEngineResource(ConfigurableResource):
    """
    Dagster resource that holds Trino credentials and builds a fully-wired
    (TrinoClient, ConfigLoader, DenormalizationEngine) triple on demand.

    Credentials are injected at runtime via Dagster EnvVar bindings so that
    secrets are never embedded in code.
    """

    host: str
    user: str
    password: str

    def build_clients(
        self, target_schema: str, schema_location: str
    ) -> tuple[TrinoClient, ConfigLoader, DenormalizationEngine]:
        """
        Create and connect a TrinoClient, load the YAML config, and
        initialise DenormalizationEngine for the given target schema.
        """
        if not self.host or not self.user or not self.password:
            raise Failure(
                description=(
                    "Trino credentials are missing. "
                    "Set TRINO_HOST, TRINO_USER, TRINO_PASSWORD environment variables."
                )
            )

        trino = TrinoClient(
            host=self.host,
            port=443,
            user=self.user,
            password=self.password,
            catalog="iceberg",
        )
        trino.connect()

        config_dir = Path(__file__).parent / "config"
        config_loader = ConfigLoader(str(config_dir))
        config_loader.load_table_config()

        engine = DenormalizationEngine(
            trino_client=trino,
            catalog="iceberg",
            target_schema=target_schema,
            schema_location=schema_location,
        )

        return trino, config_loader, engine


# ---------------------------------------------------------------------------
# External source assets  (data produced outside this pipeline)
# ---------------------------------------------------------------------------

live25_raw_source = SourceAsset(
    key=AssetKey("25live_raw"),
    description=(
        "Raw 25Live event data ingested from the 25Live API. "
        "Stored as JSON payloads in the Iceberg schema 25live_raw."
    ),
    group_name="json_pipeline",
)

mycv_raw_source = SourceAsset(
    key=AssetKey("mycv_raw"),
    description=(
        "Raw Symplectic Elements XML data ingested from the MyCV API. "
        "Stored as raw_xml payloads in the Iceberg schema mycv_raw."
    ),
    group_name="xml_pipeline",
)


def _run_one_table(
    log: logging.Logger,
    trino_engine: TrinoEngineResource,
    table_name: str,
    table_config: dict,
    default_target_schema: str,
    default_schema_location: str,
    source_schema_fallback: str,
) -> dict[str, Any]:
    """
    Execute denormalization for one source table and return engine results.
    """
    _, config_loader, engine = trino_engine.build_clients(
        target_schema=default_target_schema,
        schema_location=default_schema_location,
    )

    results = engine.run_table(
        source_schema=table_config.get("source_schema", source_schema_fallback),
        source_table=table_name,
        table_config=table_config,
        config_loader=config_loader,
        target_schema=table_config.get("target_schema", default_target_schema),
    )

    log.info(
        "[ASSET] %s -> %s: inserted=%d processed=%d columns_added=%d",
        table_name,
        results.get("table", table_name),
        results.get("rows_inserted", 0),
        results.get("rows_processed", 0),
        results.get("columns_added", 0),
    )
    return results


def _build_parent_asset(
    table_name: str,
    table_config: dict,
    target_pipeline_key: AssetKey,
    group_name: str,
    default_target_schema: str,
    default_schema_location: str,
    source_schema_fallback: str,
) -> Callable:
    target_schema = table_config.get("target_schema", default_target_schema)
    target_table = table_config.get("target_table", table_name)
    asset_key = AssetKey([target_schema, target_table])

    @asset(
        key=asset_key,
        deps=[target_pipeline_key],
        group_name=group_name,
        description=(
            f"Parent denormalized table for source '{table_name}'. "
            f"Writes to {target_schema}.{target_table}."
        ),
    )
    def _parent_asset(
        context: AssetExecutionContext,
        trino_engine: TrinoEngineResource,
    ) -> MaterializeResult:
        results = _run_one_table(
            log=context.log,
            trino_engine=trino_engine,
            table_name=table_name,
            table_config=table_config,
            default_target_schema=default_target_schema,
            default_schema_location=default_schema_location,
            source_schema_fallback=source_schema_fallback,
        )

        return MaterializeResult(
            metadata={
                "source_table": MetadataValue.text(table_name),
                "target_table": MetadataValue.text(target_table),
                "rows_processed": MetadataValue.int(results.get("rows_processed", 0)),
                "rows_inserted": MetadataValue.int(results.get("rows_inserted", 0)),
                "columns_added": MetadataValue.int(results.get("columns_added", 0)),
                "child_tables_materialized": MetadataValue.int(len(results.get("child_results", {}))),
                "errors": MetadataValue.int(len(results.get("errors", []))),
            }
        )

    return _parent_asset


def _build_child_asset(
    table_name: str,
    table_config: dict,
    child_name: str,
    group_name: str,
    default_target_schema: str,
    default_schema_location: str,
    source_schema_fallback: str,
) -> Callable:
    target_schema = table_config.get("target_schema", default_target_schema)
    target_table = table_config.get("target_table", table_name)
    parent_asset_key = AssetKey([target_schema, target_table])
    child_asset_key = AssetKey([target_schema, child_name])

    @asset(
        key=child_asset_key,
        deps=[parent_asset_key],
        group_name=group_name,
        description=(
            f"Child denormalized table '{child_name}' derived from parent "
            f"{target_schema}.{target_table}."
        ),
    )
    def _child_asset(
        context: AssetExecutionContext,
        trino_engine: TrinoEngineResource,
    ) -> MaterializeResult:
        results = _run_one_table(
            log=context.log,
            trino_engine=trino_engine,
            table_name=table_name,
            table_config=table_config,
            default_target_schema=default_target_schema,
            default_schema_location=default_schema_location,
            source_schema_fallback=source_schema_fallback,
        )
        child_results = results.get("child_results", {}).get(child_name, {})

        return MaterializeResult(
            metadata={
                "source_table": MetadataValue.text(table_name),
                "parent_table": MetadataValue.text(target_table),
                "child_table": MetadataValue.text(child_name),
                "rows_inserted": MetadataValue.int(child_results.get("rows_inserted", 0)),
                "errors": MetadataValue.int(len(child_results.get("errors", []))),
            }
        )

    return _child_asset


def _build_table_assets() -> list[Callable]:
    """
    Build per-table parent/child assets from static YAML config so lineage is:
    source -> parent table -> child table(s).
    """
    config_dir = Path(__file__).parent / "config"
    config_loader = ConfigLoader(str(config_dir))
    config_data = config_loader.load_table_config()
    all_tables: dict = config_data.get("tables", config_data)

    # Load pipeline configuration from YAML
    pipelines_config = config_data.get("pipelines", {})
    json_pipeline = pipelines_config.get("json", {})
    xml_pipeline = pipelines_config.get("xml", {})

    assets: list[Callable] = []

    for table_name, table_config in all_tables.items():
        fmt = table_config.get("format", "json")
        child_tables = table_config.get("child_tables", {})

        if fmt in ("symplectic", "xml"):
            target_pipeline_key = AssetKey("mycv_raw_denormalized")
            group_name = "xml_table_assets"
            default_target_schema = xml_pipeline.get("target_schema", "mycv_raw_denormalized")
            default_schema_location = xml_pipeline.get("schema_location", "s3a://buaws-datalake-nonprod-s3/raw/mycv_raw_denormalized")
            source_schema_fallback = xml_pipeline.get("source_schema", "mycv_raw")
        else:
            target_pipeline_key = AssetKey("25live_raw_denormalized")
            group_name = "json_table_assets"
            default_target_schema = json_pipeline.get("target_schema", "25live_raw_denormalized")
            default_schema_location = json_pipeline.get("schema_location", "s3a://buaws-datalake-nonprod-s3/raw/25live_raw_denormalized")
            source_schema_fallback = json_pipeline.get("source_schema", "25live_raw")

        assets.append(
            _build_parent_asset(
                table_name=table_name,
                table_config=table_config,
                target_pipeline_key=target_pipeline_key,
                group_name=group_name,
                default_target_schema=default_target_schema,
                default_schema_location=default_schema_location,
                source_schema_fallback=source_schema_fallback,
            )
        )

        for child_name in child_tables.keys():
            assets.append(
                _build_child_asset(
                    table_name=table_name,
                    table_config=table_config,
                    child_name=child_name,
                    group_name=group_name,
                    default_target_schema=default_target_schema,
                    default_schema_location=default_schema_location,
                    source_schema_fallback=source_schema_fallback,
                )
            )

    return assets


TABLE_LINEAGE_ASSETS = _build_table_assets()


# ---------------------------------------------------------------------------
# Internal helper — async table execution (bounded concurrency)
# ---------------------------------------------------------------------------

async def _run_tables_async(
    engine: DenormalizationEngine,
    config_loader: ConfigLoader,
    config_tables: dict,
    log: logging.Logger,
) -> tuple[int, list[str]]:
    """
    Process all tables with asyncio (≤3 concurrent workers).
    Returns (success_count, failed_table_names).
    Blocking table work is executed via asyncio.to_thread since the Trino
    client is synchronous.
    """
    table_names = list(config_tables.keys())
    success_count = 0
    failed_tables: list[str] = []
    semaphore = asyncio.Semaphore(3)

    async def _process_one(table_name: str) -> tuple[str, dict | None, Exception | None]:
        table_config = config_tables[table_name]
        try:
            async with semaphore:
                result = await asyncio.to_thread(
                    engine.run_table,
                    source_schema=table_config.get("source_schema", "25live_raw"),
                    source_table=table_name,
                    table_config=table_config,
                    config_loader=config_loader,
                    target_schema=table_config.get("target_schema"),
                )
            return table_name, result, None
        except Exception as exc:  # noqa: BLE001
            return table_name, None, exc

    tasks = [asyncio.create_task(_process_one(table_name)) for table_name in table_names]

    for idx, task in enumerate(asyncio.as_completed(tasks), 1):
        table_name, results, err = await task
        if err is None:
            log.info(
                "[%d/%d] [OK] %s: %d/%d rows, %d new columns",
                idx, len(table_names), table_name,
                results["rows_inserted"], results["rows_processed"],
                results["columns_added"],
            )
            for child_name, child_info in results.get("child_results", {}).items():
                log.info(
                    "     └─ %s: %d rows", child_name, child_info.get("rows_inserted", 0)
                )
            success_count += 1
        else:
            log.error("[%d/%d] [FAILED] %s: %s", idx, len(table_names), table_name, err)
            failed_tables.append(table_name)

    return success_count, failed_tables


def _run_tables(
    engine: DenormalizationEngine,
    config_loader: ConfigLoader,
    config_tables: dict,
    log: logging.Logger,
) -> tuple[int, list[str]]:
    """Sync wrapper for Dagster assets that runs async orchestration."""
    return asyncio.run(_run_tables_async(engine, config_loader, config_tables, log))


# ---------------------------------------------------------------------------
# Asset — JSON pipeline  (25live_raw → 25live_raw_denormalized)
# ---------------------------------------------------------------------------

@asset(
    key=AssetKey("25live_raw_denormalized"),
    deps=[live25_raw_source],
    group_name="json_pipeline",
    description=(
        "Denormalized 25Live data. "
        "Flattens JSON payloads from 25live_raw into typed Iceberg tables "
        "in the schema 25live_raw_denormalized. "
        "Schema is inferred dynamically; new columns are added automatically (drift-tolerant)."
    ),
)
def live25_raw_denormalized(
    context: AssetExecutionContext,
    trino_engine: TrinoEngineResource,
) -> None:
    log = context.log
    log.info("=" * 70)
    log.info("JSON PIPELINE  25live_raw → 25live_raw_denormalized")
    log.info("=" * 70)

    # Load pipeline configuration from YAML
    config_dir = Path(__file__).parent / "config"
    config_loader_temp = ConfigLoader(str(config_dir))
    config_data = config_loader_temp.load_table_config()
    pipelines_config = config_data.get("pipelines", {})
    json_pipeline = pipelines_config.get("json", {})

    target_schema = json_pipeline.get("target_schema", "25live_raw_denormalized")
    schema_location = json_pipeline.get("schema_location", "s3a://buaws-datalake-nonprod-s3/raw/25live_raw_denormalized")

    trino, config_loader, engine = trino_engine.build_clients(
        target_schema=target_schema,
        schema_location=schema_location,
    )
    log.info("[OK] Connected to Trino at %s", trino.host)

    config_data = config_loader.load_table_config()
    all_tables: dict = config_data.get("tables", config_data)

    # JSON tables only — exclude symplectic/xml tables
    config_tables = {
        k: v
        for k, v in all_tables.items()
        if v.get("format", "json") not in ("symplectic", "xml")
    }

    # Auto-discover new JSON tables from source_schemas (if configured).
    log.info("[DISCOVER] Scanning JSON source schemas for new tables...")
    try:
        discovered = config_loader.get_source_schema_tables(trino, include_formats={"json"})
        if discovered:
            new_only = {k: v for k, v in discovered.items() if k not in config_tables}
            if new_only:
                log.info(
                    "[DISCOVER] %d new JSON table(s) found: %s",
                    len(new_only), ", ".join(new_only),
                )
            else:
                log.info("[DISCOVER] No new JSON tables found beyond current config.")
            # Merge — manual YAML config takes precedence over auto-discovered defaults
            config_tables = {**discovered, **config_tables}
        else:
            log.info("[DISCOVER] No additional JSON tables discovered.")
    except Exception as exc:  # noqa: BLE001
        # Discovery failure is non-fatal — continue with YAML-configured tables
        log.warning("[DISCOVER] JSON auto-discovery failed (continuing with config): %s", exc)

    if not config_tables:
        log.warning("[SKIP] No JSON tables found in config. Nothing to process.")
        return

    log.info("[JSON] %d table(s) to process: %s", len(config_tables), ", ".join(config_tables))

    success_count, failed_tables = _run_tables(engine, config_loader, config_tables, log)

    log.info("=" * 70)
    log.info("[JSON SUMMARY] %d/%d tables completed successfully", success_count, len(config_tables))
    if failed_tables:
        log.error("[JSON FAILURES] %s", ", ".join(failed_tables))
        log.info("=" * 70)
        raise Failure(
            description=(
                f"JSON pipeline: {len(failed_tables)} table(s) failed — "
                f"{', '.join(failed_tables)}"
            )
        )
    log.info("[DONE] All JSON tables denormalized successfully.")
    log.info("=" * 70)


# ---------------------------------------------------------------------------
# Asset — XML pipeline  (mycv_raw → mycv_raw_denormalized)
# ---------------------------------------------------------------------------

@asset(
    deps=[mycv_raw_source],
    group_name="xml_pipeline",
    description=(
        "Denormalized MyCV (Symplectic Elements) data. "
        "Parses Symplectic API XML from mycv_raw and writes parent + child tables "
        "into the schema mycv_raw_denormalized. "
        "Auto-discovers any new tables added to mycv_raw since the last config update."
    ),
)
def mycv_raw_denormalized(
    context: AssetExecutionContext,
    trino_engine: TrinoEngineResource,
) -> None:
    log = context.log
    log.info("=" * 70)
    log.info("XML PIPELINE  mycv_raw → mycv_raw_denormalized")
    log.info("=" * 70)

    # Load pipeline configuration from YAML
    config_dir = Path(__file__).parent / "config"
    config_loader_temp = ConfigLoader(str(config_dir))
    config_data = config_loader_temp.load_table_config()
    pipelines_config = config_data.get("pipelines", {})
    xml_pipeline = pipelines_config.get("xml", {})

    target_schema = xml_pipeline.get("target_schema", "mycv_raw_denormalized")
    schema_location = xml_pipeline.get("schema_location", "s3a://buaws-datalake-nonprod-s3/raw/mycv_raw_denormalized")

    trino, config_loader, engine = trino_engine.build_clients(
        target_schema=target_schema,
        schema_location=schema_location,
    )
    log.info("[OK] Connected to Trino at %s", trino.host)

    config_data = config_loader.load_table_config()
    all_tables: dict = config_data.get("tables", config_data)

    # Start with every symplectic table declared in the YAML
    config_tables = {
        k: v for k, v in all_tables.items() if v.get("format") == "symplectic"
    }

    # Auto-discover tables added to mycv_raw that are not yet in the YAML
    log.info("[DISCOVER] Scanning mycv_raw for new tables...")
    try:
        discovered = config_loader.get_source_schema_tables(trino, include_formats={"symplectic"})
        if discovered:
            new_only = {k: v for k, v in discovered.items() if k not in config_tables}
            if new_only:
                log.info(
                    "[DISCOVER] %d new table(s) found: %s",
                    len(new_only), ", ".join(new_only),
                )
            else:
                log.info("[DISCOVER] No new tables found beyond current config.")
            # Merge — manual YAML config takes precedence over auto-discovered defaults
            config_tables = {**discovered, **config_tables}
        else:
            log.info("[DISCOVER] No additional tables discovered.")
    except Exception as exc:  # noqa: BLE001
        # Discovery failure is non-fatal — continue with YAML-configured tables
        log.warning("[DISCOVER] Auto-discovery failed (continuing with config): %s", exc)

    if not config_tables:
        log.warning("[SKIP] No XML/symplectic tables found. Nothing to process.")
        return

    log.info("[XML] %d table(s) to process: %s", len(config_tables), ", ".join(config_tables))

    success_count, failed_tables = _run_tables(engine, config_loader, config_tables, log)

    log.info("=" * 70)
    log.info("[XML SUMMARY] %d/%d tables completed successfully", success_count, len(config_tables))
    if failed_tables:
        log.error("[XML FAILURES] %s", ", ".join(failed_tables))
        log.info("=" * 70)
        raise Failure(
            description=(
                f"XML pipeline: {len(failed_tables)} table(s) failed — "
                f"{', '.join(failed_tables)}"
            )
        )
    log.info("[DONE] All XML tables denormalized successfully.")
    log.info("=" * 70)


# ---------------------------------------------------------------------------
# Job — runs both pipelines in a single Dagster run
# ---------------------------------------------------------------------------

denormalization_job = define_asset_job(
    name="denormalization_job",
    selection=[live25_raw_denormalized, mycv_raw_denormalized],
    description=(
        "Materializes both the JSON (25Live) and XML (MyCV) denormalization assets. "
        "Runs both pipelines end-to-end: schema inference, table creation, "
        "drift handling, and data refresh."
    ),
)


# ---------------------------------------------------------------------------
# Schedule — hourly execution
# ---------------------------------------------------------------------------

denormalization_daily_schedule = ScheduleDefinition(
    name="denormalization_daily_schedule",
    cron_schedule="0 9 * * *",      # Every day at 9:00 AM
    job=denormalization_job,
    description=(
        "Triggers a full denormalization of both the 25Live JSON source and the "
        "MyCV XML source every day at 9:00 AM. Both pipelines run within the same Dagster run."
    ),
)


# ---------------------------------------------------------------------------
# Definitions — single entry point registered with Dagster
# ---------------------------------------------------------------------------

defs = Definitions(
    assets=[
        # External source assets (lineage: upstream of the computed assets)
        live25_raw_source,
        mycv_raw_source,
        # Computed target assets
        live25_raw_denormalized,
        mycv_raw_denormalized,
        # Per-table parent/child assets for detailed lineage + independent materialization
        *TABLE_LINEAGE_ASSETS,
    ],
    jobs=[denormalization_job],
    schedules=[denormalization_daily_schedule],
    resources={
        # Credentials resolved at runtime from environment variables —
        # never stored in code or config files.
        "trino_engine": TrinoEngineResource(
            host=EnvVar("TRINO_HOST"),
            user=EnvVar("TRINO_USER"),
            password=EnvVar("TRINO_PASSWORD"),
        ),
    },
)
