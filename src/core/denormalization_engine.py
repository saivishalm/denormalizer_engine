"""
Generic Denormalization Engine
Handles any table following the 25Live JSON pattern:
  { "<root_key>": { "engine": ..., "<entity_key>": { ...scalars... }, "pubdate": ... } }

Features:
  - Schema inference from JSON payloads
  - Schema drift tolerance (ALTER TABLE ADD COLUMN, never drops)
  - Iceberg sorted_by on creation
  - Child table extraction (e.g. event_history from events)
  - DELETE + re-INSERT on each run (full refresh)
"""

import json
import logging
from typing import Any
from concurrent.futures import ThreadPoolExecutor, as_completed

from src.db.trino_client import TrinoClient
from src.parsers.xml_parser import XMLParser
from src.parsers.symplectic_parser import SymplecticParser

logger = logging.getLogger(__name__)

# Python type → Iceberg/Trino SQL type
_PYTHON_TO_SQL: dict[type, str] = {
    int:        "INTEGER",
    float:      "DOUBLE",
    bool:       "BOOLEAN",
    str:        "VARCHAR",
    type(None): "VARCHAR",
}


class DenormalizationEngine:
    """
    Generic engine that denormalizes a raw JSON table into Iceberg tables.

    Usage:
        engine = DenormalizationEngine(trino_client, catalog="iceberg",
                                       target_schema="25live_raw_denormalized",
                                       schema_location="s3a://bucket/path")
        results = engine.run_table(
            source_schema="25live_raw",
            source_table="events",
            table_config={...}   # from table_config.yaml
        )
    """

    def __init__(
        self,
        trino_client: TrinoClient,
        catalog: str,
        target_schema: str,
        schema_location: str,
        performance_config: dict = None,
    ):
        self.trino = trino_client
        self.catalog = catalog
        self.target_schema = target_schema
        self.schema_location = schema_location.rstrip("/")
        self.target_schema_q = (
            f'"{target_schema}"' if target_schema[0].isdigit() else target_schema
        )

        # Load performance settings with defaults
        perf = performance_config or {}
        self.parse_workers_max = perf.get("parse_workers_max", 8)
        self.parse_workers_min = perf.get("parse_workers_min", 1)
        self.parse_rows_per_worker = perf.get("parse_rows_per_worker", 50)
        self.child_table_workers = perf.get("child_table_workers", 4)
        self.batch_size_min = perf.get("batch_size_min", 50)
        self.batch_size_max = perf.get("batch_size_max", 2000)
        self.batch_target_bytes = perf.get("batch_target_bytes", 1500000)
        self.batch_avg_chars_per_value = perf.get("batch_avg_chars_per_value", 40)

    # ─────────────────────────────────────────────────────────────────────────
    # Public API
    # ─────────────────────────────────────────────────────────────────────────

    def run_table(
        self,
        source_schema: str,
        source_table: str,
        table_config: dict,
        config_loader=None,
        target_schema: str = None,
    ) -> dict:
        """
        Full denormalization for one source table.
        
        Args:
            source_schema: Source schema name
            source_table: Source table name
            table_config: Table configuration dict (from YAML)
            config_loader: Optional ConfigLoader for auto-discovery
            target_schema: Optional override target schema (default: self.target_schema)
        
        If config_loader is provided, auto-discovers child tables from raw JSON.

        Returns:
            dict with rows_processed, rows_inserted, columns_added,
                  child_results, errors
        """
        # Allow table_config to override target_schema (e.g. mycv_raw_denormalized)
        if target_schema is None:
            target_schema = table_config.get("target_schema", self.target_schema)

        target_schema_q = (
            f'"{target_schema}"' if target_schema[0].isdigit() else target_schema
        )

        # Ensure target schema exists (idempotent — needed for new schemas like mycv_raw_denormalized)
        self._ensure_target_schema(target_schema, target_schema_q, table_config.get("schema_location"))
        
        # Auto-discover child tables if config_loader provided
        # Allow table_config to override source_schema (e.g. XML tables in mycv_raw)
        source_schema = table_config.get("source_schema", source_schema)

        fmt = table_config.get("format", "json")   # "json" | "xml" | "symplectic"

        if config_loader:
            # Skip JSON auto-discovery for symplectic/XML tables — child tables
            # are already discovered during schema discovery at startup.
            if fmt not in ("symplectic", "xml"):
                table_config = config_loader.auto_discover_child_tables(
                    self.trino, source_schema, source_table, table_config
                )

        root_key      = table_config["root_key"]
        entity_key    = table_config["entity_key"]
        target_table  = table_config.get("target_table", source_table)
        sort_by       = table_config.get("sort_by")
        child_configs = table_config.get("child_tables", {})

        results = {
            "table": target_table,
            "rows_processed": 0,
            "rows_inserted": 0,
            "columns_added": 0,
            "child_results": {},
            "errors": [],
        }

        source_schema_q = (
            f'"{source_schema}"' if source_schema[0].isdigit() else source_schema
        )

        # 1. Fetch all raw rows
        # Use data_column from table config if specified (e.g. 'raw_xml' for mycv_raw),
        # otherwise fall back to the standard 'data' column used by JSON schemas.
        data_col   = table_config.get("data_column", "data")
        # Always quote the source table name to handle reserved keywords (e.g. "group", "user")
        source_table_q = f'"{source_table}"'
        test_limit = table_config.get("test_limit")
        limit_clause = f" LIMIT {test_limit}" if test_limit else ""
        logger.info(f"Fetching {source_schema}.{source_table}{f' (test: {test_limit} rows)' if test_limit else ''}")
        raw_rows = self.trino.execute_query(
            f"SELECT id, {data_col} FROM {self.catalog}.{source_schema_q}.{source_table_q}{limit_clause}"
        ).fetchall()
        results["rows_processed"] = len(raw_rows)
        logger.info(f"  {len(raw_rows)} rows fetched")

        # 2. Parse payload in parallel — XML parsing is CPU-bound; threading
        #    releases the GIL for ElementTree calls and saturates available cores.
        flat_rows: list[dict]              = []
        child_rows: dict[str, list[dict]] = {name: [] for name in child_configs}

        parse_workers = min(
            self.parse_workers_max,
            max(self.parse_workers_min, len(raw_rows) // self.parse_rows_per_worker + 1)
        )
        logger.info(f"  Parsing {len(raw_rows)} rows with {parse_workers} worker thread(s)")

        with ThreadPoolExecutor(max_workers=parse_workers) as parse_pool:
            parse_futures = {
                parse_pool.submit(
                    self._parse_single_row,
                    raw_id, data_str, fmt, root_key, entity_key, child_configs
                ): raw_id
                for raw_id, data_str in raw_rows
            }
            for future in as_completed(parse_futures):
                raw_id = parse_futures[future]
                try:
                    flat_row, child_dict = future.result()
                    flat_rows.append(flat_row)
                    for child_name, child_list in child_dict.items():
                        child_rows[child_name].extend(child_list)
                except Exception as e:
                    msg = f"Parse error raw_id={raw_id}: {e}"
                    logger.error(msg)
                    results["errors"].append(msg)

        logger.info(f"  Parsed {len(flat_rows)} parent rows, "
                    f"{sum(len(v) for v in child_rows.values())} total child rows")

        # 3. Infer schema from all flat rows
        inferred = self._infer_schema(flat_rows)

        # 4. Create table or drift-check, then refresh data
        base_location = table_config.get("schema_location", self.schema_location).rstrip("/")
        location = f"{base_location}/{target_table}"
        cols_added, actual_schema = self._ensure_table(target_table, inferred, sort_by, location, target_schema_q)
        results["columns_added"] = cols_added

        inserted = self._refresh_rows(target_table, flat_rows, actual_schema, results["errors"], target_schema_q)
        results["rows_inserted"] = inserted

        # 5. Child tables (process in parallel)
        if child_configs:
            with ThreadPoolExecutor(max_workers=min(self.child_table_workers, len(child_configs))) as executor:
                futures = {}
                for child_name, child_cfg in child_configs.items():
                    future = executor.submit(
                        self._process_child_table,
                        child_name,
                        child_cfg,
                        child_rows,
                        target_schema_q,
                        base_location,
                        results["errors"]
                    )
                    futures[future] = child_name
                
                for future in as_completed(futures):
                    child_name = futures[future]
                    try:
                        child_result = future.result()
                        results["child_results"][child_name] = child_result
                        # Propagate any batch-insert errors from the child table run
                        results["errors"].extend(child_result.get("errors", []))
                        logger.debug(f"  Child {child_name}: {child_result['rows_inserted']} rows inserted")
                    except Exception as e:
                        logger.error(f"  Child {child_name} failed: {e}")
                        results["errors"].append(f"Child table {child_name}: {e}")

        logger.info(
            f"{target_table}: {results['rows_inserted']}/{results['rows_processed']} rows, "
            f"{results['columns_added']} new columns"
        )
        return results

    def _process_child_table(self, child_name: str, child_cfg: dict, child_rows: dict, target_schema_q: str, base_location: str, errors: list) -> dict:
        """Process a single child table (called in parallel)."""
        parent_fk = child_cfg.get("parent_fk", "parent_id")

        if child_name not in child_rows or not child_rows[child_name]:
            # No data in this run — ensure the table exists with at least a minimal schema
            # so all configured child tables are always present in the target schema.
            existing = self._get_existing_columns(child_name, target_schema_q)
            if not existing:
                minimal_schema = {parent_fk: "VARCHAR"}
                child_location = f"{base_location}/{child_name}"
                child_sort = child_cfg.get("sort_by")
                try:
                    self._ensure_table(child_name, minimal_schema, child_sort, child_location, target_schema_q)
                    logger.info(f"  Created empty table {child_name} (no data in current run)")
                except Exception as e:
                    logger.warning(f"  Could not create empty table {child_name}: {e}")
            return {"rows_inserted": 0, "errors": []}
        
        child_inferred = self._infer_schema(child_rows[child_name])
        child_location = f"{base_location}/{child_name}"
        child_sort     = child_cfg.get("sort_by")
        child_errors: list[str] = []

        _, child_actual = self._ensure_table(child_name, child_inferred, child_sort, child_location, target_schema_q)
        child_inserted = self._refresh_rows(
            child_name, child_rows[child_name], child_actual, child_errors, target_schema_q
        )
        
        return {
            "rows_inserted": child_inserted,
            "errors": child_errors,
        }

    # ─────────────────────────────────────────────────────────────────────────
    # Payload parsing (JSON + XML)
    # ─────────────────────────────────────────────────────────────────────────

    def _parse_single_row(
        self,
        raw_id: Any,
        data_str: Any,
        fmt: str,
        root_key: str,
        entity_key: str,
        child_configs: dict,
    ) -> tuple[dict, dict[str, list]]:
        """
        Parse one raw row → (flat_parent_dict, {child_name: [rows]}).
        Designed to be called from a thread pool — all methods it uses are
        stateless / thread-safe.
        """
        payload  = self._parse_payload(data_str, fmt, root_key, entity_key)
        root_obj = payload.get(root_key, {})
        entity   = root_obj.get(entity_key, {})

        # Parent row
        flat_row: dict[str, Any] = {
            "raw_id":  str(raw_id),
            "engine":  root_obj.get("engine"),
            "pubdate": root_obj.get("pubdate"),
        }
        for k, v in entity.items():
            if not isinstance(v, list):
                flat_row[k] = self._extract_scalar(v)

        # Child rows
        child_dict: dict[str, list] = {}
        for child_name, child_cfg in child_configs.items():
            child_entity_key = child_cfg["entity_key"]
            parent_fk        = child_cfg["parent_fk"]
            fk_value         = entity.get(parent_fk) or raw_id
            child_obj        = entity.get(child_entity_key)
            rows: list[dict]  = []

            if isinstance(child_obj, dict) and child_obj:
                child_row = {parent_fk: fk_value}
                for k, v in child_obj.items():
                    child_row[k] = self._extract_scalar(v)
                rows.append(child_row)
            elif isinstance(child_obj, list):
                for item in child_obj:
                    if isinstance(item, dict):
                        child_row = {parent_fk: fk_value}
                        for k, v in item.items():
                            child_row[k] = self._extract_scalar(v)
                        rows.append(child_row)

            if rows:
                child_dict[child_name] = rows

        return flat_row, child_dict

    def _parse_payload(self, data_str: Any, fmt: str, root_key: str = "", entity_key: str = "") -> dict:
        """
        Parse a raw data string into a nested dict.
        Dispatches to JSON, generic XML, or Symplectic API XML parser based on fmt.
        """
        if fmt == "symplectic":
            if isinstance(data_str, str):
                return SymplecticParser.parse_payload(data_str, root_key, entity_key)
            return data_str if isinstance(data_str, dict) else {}
        if fmt == "xml":
            if isinstance(data_str, str):
                return XMLParser.parse_payload(data_str)
            return data_str if isinstance(data_str, dict) else {}
        # Default: JSON
        if isinstance(data_str, str):
            return json.loads(data_str)
        return data_str if isinstance(data_str, dict) else {}

    def _ensure_target_schema(
        self,
        target_schema: str,
        target_schema_q: str,
        schema_location: str | None,
    ) -> None:
        """
        Create the target schema in Trino if it does not already exist.
        Idempotent — safe to call on every run.
        Uses schema_location from table_config if provided, otherwise no location clause.
        """
        try:
            if schema_location:
                ddl = (
                    f"CREATE SCHEMA IF NOT EXISTS {self.catalog}.{target_schema_q} "
                    f"WITH (location = '{schema_location.rstrip('/')}')"
                )
            else:
                ddl = f"CREATE SCHEMA IF NOT EXISTS {self.catalog}.{target_schema_q}"
            self.trino.execute_query(ddl)
        except Exception as e:
            logger.debug(f"  _ensure_target_schema skipped for {target_schema}: {e}")

    # ─────────────────────────────────────────────────────────────────────────
    # Schema inference
    # ─────────────────────────────────────────────────────────────────────────

    def _extract_scalar(self, value: Any) -> Any:
        """
        Extract scalar value from potentially wrapped objects.
        Handles cases like {"nil": true} → None, or just returns scalar values.
        """
        # If it's already a scalar, return as-is
        if not isinstance(value, (dict, list)):
            return value
        
        # Handle nil wrapper: {"nil": true} → None
        if isinstance(value, dict):
            if len(value) == 1 and "nil" in value and value.get("nil") is True:
                return None
            # If it's a dict but not a nil wrapper, treat as NULL (can't represent nested objects as scalars)
            return None
        
        # Lists can't be scalar values
        return None

    def _infer_schema(self, rows: list[dict]) -> dict[str, str]:
        """
        Infer SQL column types from a list of flat row dicts.
        - First non-None value wins the type
        - Type conflicts → widen to VARCHAR
        """
        schema: dict[str, str] = {}
        for row in rows:
            for col, val in row.items():
                if val is None or val == "":
                    schema.setdefault(col, "VARCHAR")
                    continue
                sql_type = _PYTHON_TO_SQL.get(type(val), "VARCHAR")
                if col not in schema:
                    schema[col] = sql_type
                elif schema[col] != sql_type and schema[col] != "VARCHAR":
                    schema[col] = "VARCHAR"   # type conflict → widen
        return schema

    # ─────────────────────────────────────────────────────────────────────────
    # Table management (create / drift)
    # ─────────────────────────────────────────────────────────────────────────

    def _get_existing_columns(self, table: str, target_schema_q: str) -> dict[str, str]:
        """DESCRIBE the target table; returns {} if table does not exist."""
        table_q = f'"{table}"'
        try:
            r = self.trino.execute_query(
                f"DESCRIBE {self.catalog}.{target_schema_q}.{table_q}"
            )
            return {row[0]: row[1].upper() for row in r.fetchall()}
        except Exception:
            return {}

    def _ensure_table(
        self,
        table: str,
        schema: dict[str, str],
        sort_by: str | None,
        location: str,
        target_schema_q: str,
    ) -> tuple[int, dict[str, str]]:
        """
        Create table if it doesn't exist, else add any new columns (schema drift).
        Returns (columns_added, actual_table_schema).
        actual_table_schema reflects the real column types from DESCRIBE so that
        INSERT literals are cast to the correct types (e.g. DATE, not VARCHAR).
        """
        existing = self._get_existing_columns(table, target_schema_q)

        # Always quote the table name to handle reserved keywords (e.g. "group", "user")
        table_q = f'"{table}"'

        if not existing:
            # CREATE TABLE
            col_defs  = ",\n".join(f"    {col}  {typ}" for col, typ in schema.items())
            with_parts = [
                "format         = 'PARQUET'",
                "format_version = 2",
                f"location       = '{location}'",
            ]
            if sort_by:
                with_parts.append(f"sorted_by      = ARRAY['{sort_by}']")

            ddl = (
                f"CREATE TABLE IF NOT EXISTS "
                f"{self.catalog}.{target_schema_q}.{table_q} (\n"
                f"{col_defs}\n)\nWITH (\n    "
                + ",\n    ".join(with_parts)
                + "\n)"
            )
            try:
                self.trino.execute_query(ddl)
                logger.info(f"  Created table {table} with {len(schema)} columns")
            except Exception as e:
                # sorted_by might not be supported — retry without it
                if sort_by and "sorted_by" in str(e).lower():
                    logger.warning(f"  sorted_by not supported, retrying without: {e}")
                    with_parts_no_sort = [p for p in with_parts if "sorted_by" not in p]
                    ddl_no_sort = (
                        f"CREATE TABLE IF NOT EXISTS "
                        f"{self.catalog}.{target_schema_q}.{table_q} (\n"
                        f"{col_defs}\n)\nWITH (\n    "
                        + ",\n    ".join(with_parts_no_sort)
                        + "\n)"
                    )
                    self.trino.execute_query(ddl_no_sort)
                    logger.info(f"  Created table {table} (without sorted_by)")
                else:
                    raise
            # fresh create: inferred schema IS the table schema
            return len(schema), dict(schema)

        # Table exists — handle schema drift only (sorted_by is set at CREATE time
        # and persists in Iceberg metadata; no need to ALTER on every run).
        added = 0
        for col, col_type in schema.items():
            if col not in existing:
                alter = (
                    f"ALTER TABLE {self.catalog}.{target_schema_q}.{table_q} "
                    f"ADD COLUMN {col} {col_type}"
                )
                self.trino.execute_query(alter)
                logger.warning(f"  Schema drift: added column {col} {col_type} to {table}")
                existing[col] = col_type
                added += 1
        # return full actual schema (existing columns + any newly added ones)
        return added, existing

    def _ensure_sorted_by(
        self,
        table: str,
        sort_col: str,
        target_schema_q: str,
    ) -> None:
        """
        Ensure the Iceberg table has sorted_by set on sort_col.
        Uses ALTER TABLE SET PROPERTIES which is idempotent — safe to call
        on every run whether or not the property is already present.
        Silently skips if the cluster does not support the property.
        """
        table_q = f'"{table}"'
        try:
            self.trino.execute_query(
                f"ALTER TABLE {self.catalog}.{target_schema_q}.{table_q} "
                f"SET PROPERTIES sorted_by = ARRAY['{sort_col}']"
            )
            logger.info(f"  Index: sorted_by='{sort_col}' confirmed on {table}")
        except Exception as e:
            logger.debug(f"  sorted_by not set on {table} (skipping): {e}")

    # ─────────────────────────────────────────────────────────────────────────
    # Data refresh (delete existing + insert fresh)
    # ─────────────────────────────────────────────────────────────────────────

    def _refresh_rows(
        self,
        table: str,
        rows: list[dict],
        schema: dict[str, str],
        errors: list[str],
        target_schema_q: str,
    ) -> int:
        """
        Delete all existing rows then insert fresh data.
        Returns count of successfully inserted rows.
        """
        if not rows:
            return 0

        # Always quote the table name to handle reserved keywords (e.g. "group", "user")
        table_q = f'"{table}"'

        # Delete existing data (TRUNCATE is faster than DELETE)
        try:
            self.trino.execute_query(
                f"TRUNCATE TABLE {self.catalog}.{target_schema_q}.{table_q}"
            )
        except Exception:
            # If TRUNCATE fails, try DELETE (TRUNCATE might not be supported)
            try:
                self.trino.execute_query(
                    f"DELETE FROM {self.catalog}.{target_schema_q}.{table_q}"
                )
            except Exception as e:
                logger.debug(f"  DELETE failed for {table} (may be empty): {e}")

        # Insert fresh rows — batch them to avoid Trino's 1MB query text limit.
        # Wide XML tables (many columns) need a smaller batch size to stay under limit.
        columns    = list(schema.keys())
        col_str    = ", ".join(columns)
        inserted   = 0
        # Estimate chars per value on average (col name in header + value literal).
        # Target batch size to stay safely under Trino's 1 MB query-text limit
        # when accounting for INSERT header + column names overhead.
        num_cols   = max(len(columns), 1)
        batch_size = max(
            self.batch_size_min,
            min(self.batch_size_max, self.batch_target_bytes // (num_cols * self.batch_avg_chars_per_value))
        )

        for batch_start in range(0, len(rows), batch_size):
            batch_end = min(batch_start + batch_size, len(rows))
            batch = rows[batch_start:batch_end]

            try:
                # Build multi-row VALUES clause
                value_rows = []
                for row in batch:
                    val_str = ", ".join(
                        self._to_sql_literal(row.get(col), schema[col]) for col in columns
                    )
                    value_rows.append(f"({val_str})")

                values_clause = ", ".join(value_rows)

                self.trino.execute_query(
                    f"INSERT INTO {self.catalog}.{target_schema_q}.{table_q} "
                    f"({col_str}) VALUES {values_clause}"
                )
                inserted += len(batch)

                if (batch_end % (batch_size * 5) == 0):
                    logger.debug(f"  Inserted {inserted}/{len(rows)} rows into {table}")

            except Exception as e:
                msg = f"Batch insert error in {table} (rows {batch_start}-{batch_end}): {e}"
                logger.error(msg)
                errors.append(msg)

        return inserted

    # ─────────────────────────────────────────────────────────────────────────
    # SQL literal formatting
    # ─────────────────────────────────────────────────────────────────────────

    def _to_sql_literal(self, value: Any, col_type: str) -> str:
        """
        Format a Python value as a Trino SQL literal, always using explicit CAST
        so that the query column types match the table column types exactly.
        NULL  → CAST(NULL AS <col_type>)
        DATE  → DATE 'YYYY-MM-DD'
        VARCHAR → CAST('...' AS VARCHAR)
        numbers → bare numeric literals (Trino infers these correctly)
        """
        # Normalise the base type token (strip precision from e.g. VARCHAR(255))
        base = col_type.upper().split("(")[0].strip()

        # NULL / empty string
        if value is None or (isinstance(value, str) and str(value).strip() == ""):
            return f"CAST(NULL AS {col_type})"

        if base in ("INTEGER", "INT", "BIGINT", "SMALLINT"):
            try:
                return str(int(float(str(value))))
            except (ValueError, TypeError):
                return f"CAST(NULL AS {col_type})"

        if base == "DOUBLE":
            try:
                return str(float(str(value)))
            except (ValueError, TypeError):
                return f"CAST(NULL AS {col_type})"

        if base == "BOOLEAN":
            return "true" if str(value).strip().lower() in ("true", "1", "yes") else "false"

        if base == "DATE":
            escaped = str(value).replace("'", "''")
            return f"DATE '{escaped}'"

        if base in ("TIMESTAMP", "TIMESTAMPTZ"):
            escaped = str(value).replace("'", "''")
            return f"CAST('{escaped}' AS {col_type})"

        # Default: VARCHAR — wrap in CAST to avoid varchar(N) vs varchar mismatch
        escaped = str(value).replace("'", "''")
        return f"CAST('{escaped}' AS VARCHAR)"
