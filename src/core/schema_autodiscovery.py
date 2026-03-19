"""
Schema Auto-Discovery
Automatically detects child tables from raw JSON payloads and Symplectic XML schemas.
"""

import json
import logging
import xml.etree.ElementTree as ET
from typing import Any, Dict, List, Tuple
from src.db.trino_client import TrinoClient

logger = logging.getLogger(__name__)


class SchemaAutodiscovery:
    """
    Inspects raw JSON tables and auto-discovers:
    - Arrays (become child tables with 1:many relationship)
    - Nested objects (can become child tables or be flattened)
    """

    def __init__(self, trino_client: TrinoClient):
        self.trino = trino_client

    def discover_child_tables(
        self,
        source_schema: str,
        source_table: str,
        root_key: str,
        entity_key: str,
        business_key: str,
        data_column: str = "data",
    ) -> Dict[str, dict]:
        """
        Fetch multiple sample rows and identify nested structures.
        (Sample 100 rows to ensure we catch all possible nested fields)

        Returns:
            dict with child_table_name -> config
            Example:
            {
                "organization_addresses": {
                    "entity_key": "address",
                    "parent_fk": "organization_id",
                    "sort_by": "organization_id"
                }
            }
        """
        try:
            # Quote schema name if it starts with a digit
            source_schema_q = (
                f'"{source_schema}"' if source_schema[0].isdigit() else source_schema
            )
            # Always quote table name to handle reserved keywords (e.g. "group", "user")
            source_table_q = f'"{source_table}"'

            # Fetch multiple sample rows (100) to catch all nested structures
            result = self.trino.execute_query(
                f"SELECT {data_column} FROM {source_schema_q}.{source_table_q} LIMIT 100"
            ).fetchall()

            if not result:
                logger.warning(f"  {source_table}: No rows to analyze")
                return {}

            # Collect all nested fields across all sample rows
            all_nested_keys = {}  # key -> (count, type, sample_value)
            total_rows_with_entity = 0
            
            for row in result:
                # row is a tuple like (json_string,)
                data_str = row[0]
                payload = json.loads(data_str) if isinstance(data_str, str) else data_str

                if root_key not in payload:
                    continue

                root_obj = payload[root_key]
                if entity_key not in root_obj:
                    continue

                entity = root_obj[entity_key]
                if not isinstance(entity, dict):
                    continue

                total_rows_with_entity += 1

                # Track all nested (array/object) fields found across all samples
                for key, val in entity.items():
                    if isinstance(val, (list, dict)) and not self._is_special_object(key, val):
                        if key not in all_nested_keys:
                            all_nested_keys[key] = [0, type(val).__name__, val]
                        all_nested_keys[key][0] += 1  # increment count

            # Build child tables only for fields present in ≥50% of rows
            child_tables = {}
            threshold = 0.5  # 50% minimum presence
            
            for key, (count, val_type, val) in all_nested_keys.items():
                presence_ratio = count / total_rows_with_entity if total_rows_with_entity > 0 else 0
                
                # Only create child table if present in ≥50% of rows
                if presence_ratio < threshold:
                    # Field is too sparse to warrant a child table; it will be dropped
                    # as NULL in the parent row since _extract_scalar returns None for dicts/lists.
                    logger.info(f"    ℹ Sparse field '{key}' ({presence_ratio:.0%}) - skipping (insufficient presence for child table)")
                    continue
                
                # Skip if field name already contains entity_key (avoid double-naming)
                if key.startswith(entity_key + "_"):
                    child_name = key
                else:
                    child_name = f"{entity_key}_{key}"
                
                # Arrays → create child table (1:many)
                if val_type == "list" and len(val) > 0:
                    child_tables[child_name] = {
                        "entity_key": key,
                        "parent_fk": business_key,
                        "sort_by": business_key,
                    }
                    logger.info(f"    ✓ Array field '{key}' ({presence_ratio:.0%}) → child table '{child_name}'")

                # Objects (1:1) → create child table
                elif val_type == "dict" and len(val) > 0:
                    child_tables[child_name] = {
                        "entity_key": key,
                        "parent_fk": business_key,
                        "sort_by": business_key,
                    }
                    logger.info(f"    ✓ Object field '{key}' ({presence_ratio:.0%}) → child table '{child_name}'")

            return child_tables

        except Exception as e:
            logger.error(f"  {source_table}: Discovery failed: {e}")
            return {}

    @staticmethod
    def _is_special_object(key: str, obj: dict) -> bool:
        """
        Detect if object is a wrapper (like {"nil": true}, {"type_id": ..., "status": "est"})
        vs an actual nested entity.
        """
        # Skip if it's just a nil wrapper or error object
        if len(obj) <= 2 and ("nil" in obj or "error" in obj):
            return True
        # Skip specific known wrapper fields
        if key in ["engine", "pubdate"]:
            return True
        return False

    # ── Symplectic XML schema discovery ──────────────────────────────────────

    _API_NS = "http://www.symplectic.co.uk/publications/api"

    # Candidate names for the raw payload column, checked in priority order.
    _PAYLOAD_COLUMN_CANDIDATES = ["data", "payload", "body", "xml", "content", "raw", "record", "value"]

    def _find_payload_column(self, schema_q: str, table_q: str) -> str:
        """
        Introspect a table's columns and return the name of the payload column
        (the VARCHAR column that holds the raw XML/JSON string).

        Checks _PAYLOAD_COLUMN_CANDIDATES first; falls back to the first
        VARCHAR column; returns None if nothing suitable is found.
        """
        try:
            rows = self.trino.execute_query(
                f"SHOW COLUMNS FROM {schema_q}.{table_q}"
            ).fetchall()
            # SHOW COLUMNS returns (column_name, type, extra, comment)
            col_names = [row[0].lower() for row in rows]
            for candidate in self._PAYLOAD_COLUMN_CANDIDATES:
                if candidate in col_names:
                    return candidate
            # Fallback: first VARCHAR / CHAR column
            for row in rows:
                if "varchar" in row[1].lower() or "char" in row[1].lower():
                    return row[0]
            return col_names[0] if col_names else None
        except Exception as e:
            logger.error(f"_find_payload_column: SHOW COLUMNS failed for {schema_q}.{table_q}: {e}")
            return None

    def discover_symplectic_schema(
        self,
        source_schema: str,
        schema_defaults: Dict[str, Any],
    ) -> Dict[str, Dict[str, Any]]:
        """
        Auto-discover all tables in a Symplectic-format source schema.

        For each table found via SHOW TABLES:
          - Auto-detects the payload column name (or uses data_column from YAML)
          - Fetches one sample row and extracts `category` from the XML root
          - Parses with SymplecticParser to find list-valued entity fields
            (those become child tables automatically)
          - Assembles a full table config dict for DenormalizationEngine

        Args:
            source_schema:   Trino schema name to inspect (e.g. "mycv_raw")
            schema_defaults: dict from the source_schemas YAML block, e.g.
                             { format, target_schema, schema_location,
                               data_column (optional) }

        Returns:
            { table_name: { full table config } }
        """
        # Late import to avoid circular dependency at module load time
        from src.parsers.symplectic_parser import SymplecticParser

        source_schema_q = (
            f'"{source_schema}"' if source_schema[0].isdigit() else source_schema
        )

        # Payload column override from YAML (if not set, auto-detect per table)
        yaml_data_column = schema_defaults.get("data_column")

        # --- 1. List all tables in the schema ---
        try:
            rows = self.trino.execute_query(
                f"SHOW TABLES FROM {source_schema_q}"
            ).fetchall()
        except Exception as e:
            logger.error(f"discover_symplectic_schema: SHOW TABLES failed for {source_schema}: {e}")
            return {}

        table_names = [row[0] for row in rows]
        if not table_names:
            logger.warning(f"discover_symplectic_schema: no tables found in {source_schema}")
            return {}

        logger.info(f"[DISCOVER] {source_schema}: found {len(table_names)} tables: {', '.join(table_names)}")

        discovered: Dict[str, Dict[str, Any]] = {}

        target_schema   = schema_defaults.get("target_schema",   f"{source_schema}_denormalized")
        schema_location = schema_defaults.get("schema_location",  "")
        fmt             = schema_defaults.get("format",           "symplectic")

        for tbl in table_names:
            # Always quote table names — handles reserved keywords like "group"
            tbl_q = f'"{tbl}"'
            try:
                # Determine payload column (YAML override → auto-detect)
                data_col = yaml_data_column or self._find_payload_column(source_schema_q, tbl_q)
                if not data_col:
                    logger.warning(f"  {tbl}: cannot determine payload column — skipping")
                    continue

                # Sample up to 5 rows so sparse list-type fields (child tables) are
                # more likely to be discovered even if absent from the first row.
                sample_rows = self.trino.execute_query(
                    f"SELECT {data_col} FROM {source_schema_q}.{tbl_q} LIMIT 5"
                ).fetchall()

                if not sample_rows:
                    logger.warning(f"  {tbl}: no rows — skipping")
                    continue

                # Use the first non-empty row for category/entity_key detection;
                # accumulate child table list fields across ALL sample rows.
                xml_str = None
                for (row_val,) in sample_rows:
                    if row_val and str(row_val).strip():
                        xml_str = str(row_val).strip().lstrip("\ufeff")
                        break

                if not xml_str:
                    logger.warning(f"  {tbl}: empty column '{data_col}' — skipping")
                    continue

                logger.debug(f"  {tbl}: column='{data_col}', preview={xml_str[:120]!r}")

                if not xml_str.startswith("<"):
                    logger.error(
                        f"  {tbl}: column '{data_col}' does not contain XML "
                        f"(starts with: {xml_str[:80]!r}). "
                        f"Set data_column: <correct_column> in source_schemas config."
                    )
                    # Try every other VARCHAR column in case auto-detect picked the wrong one
                    rows = self.trino.execute_query(
                        f"SHOW COLUMNS FROM {source_schema_q}.{tbl_q}"
                    ).fetchall()
                    xml_str = None
                    for col_row in rows:
                        candidate = col_row[0]
                        if candidate.lower() == data_col.lower():
                            continue
                        if "varchar" not in col_row[1].lower() and "char" not in col_row[1].lower():
                            continue
                        val = self.trino.execute_query(
                            f"SELECT {candidate} FROM {source_schema_q}.{tbl_q} LIMIT 1"
                        ).fetchone()
                        if val and val[0] and str(val[0]).strip().startswith("<"):
                            xml_str = str(val[0]).strip().lstrip("\ufeff")
                            data_col = candidate
                            logger.info(f"  {tbl}: found XML in column '{candidate}'")
                            break
                    if not xml_str:
                        logger.error(f"  {tbl}: could not find an XML column — skipping")
                        continue

                # Extract category (entity_key) from the XML root element
                try:
                    root_el = ET.fromstring(xml_str)
                except ET.ParseError as xml_err:
                    logger.error(
                        f"  {tbl}: XML parse failed (column='{data_col}'): {xml_err}. "
                        f"Content preview: {xml_str[:200]!r}"
                    )
                    continue
                category = root_el.get("category", "").strip()
                if not category:
                    # Fall back: strip trailing 's' from table name
                    category = tbl.rstrip("s")

                # Normalize hyphens → underscores so entity_key is a valid SQL identifier
                # e.g. "teaching-activity" → "teaching_activity"
                entity_key   = category.replace("-", "_")
                business_key = f"{entity_key}_id"

                # Parse all sample rows and union child table discoveries —
                # this catches list-type fields that are absent in some rows.
                child_tables: Dict[str, Any] = {}
                for (row_val,) in sample_rows:
                    if not row_val:
                        continue
                    sample_xml = str(row_val).strip().lstrip("\ufeff")
                    if not sample_xml.startswith("<"):
                        continue
                    try:
                        parsed = SymplecticParser.parse_payload(sample_xml, root_key=tbl, entity_key=entity_key)
                    except ET.ParseError:
                        continue
                    sample_entity = parsed.get(tbl, {}).get(entity_key, {})
                    for field_name, field_val in sample_entity.items():
                        if isinstance(field_val, list) and len(field_val) > 0:
                            child_table_name = f"{entity_key}_{field_name}"
                            if child_table_name not in child_tables:
                                child_tables[child_table_name] = {
                                    "entity_key": field_name,
                                    "parent_fk":  business_key,
                                    "sort_by":    business_key,
                                }
                                logger.info(f"  {tbl}: list field '{field_name}' -> child table '{child_table_name}'")

                table_cfg: Dict[str, Any] = {
                    "format":          fmt,
                    "source_schema":   source_schema,
                    "target_schema":   target_schema,
                    "schema_location": schema_location,
                    "data_column":     data_col,   # carry forward so engine uses correct column
                    "root_key":        tbl,
                    "entity_key":      entity_key,
                    "target_table":    tbl,
                    "business_key":    business_key,
                    "sort_by":         business_key,
                }
                if child_tables:
                    table_cfg["child_tables"] = child_tables

                discovered[tbl] = table_cfg
                logger.info(f"  [OK] {tbl}: entity_key={entity_key}, {len(child_tables)} child table(s)")

            except Exception as e:
                logger.error(f"  {tbl}: discovery error — {e}")
                continue

        return discovered

    # ── Generic JSON schema discovery ───────────────────────────────────────

    def discover_json_schema(
        self,
        source_schema: str,
        schema_defaults: Dict[str, Any],
    ) -> Dict[str, Dict[str, Any]]:
        """
        Auto-discover all tables in a JSON-format source schema.

        For each table found via SHOW TABLES, build a baseline table config that
        can be denormalized by DenormalizationEngine. Child tables are discovered
        later per table from sampled payloads.
        """
        source_schema_q = (
            f'"{source_schema}"' if source_schema[0].isdigit() else source_schema
        )

        target_schema = schema_defaults.get("target_schema", f"{source_schema}_denormalized")
        schema_location = schema_defaults.get("schema_location", "")
        fmt = schema_defaults.get("format", "json")
        yaml_data_column = schema_defaults.get("data_column")

        try:
            rows = self.trino.execute_query(
                f"SHOW TABLES FROM {source_schema_q}"
            ).fetchall()
        except Exception as e:
            logger.error(f"discover_json_schema: SHOW TABLES failed for {source_schema}: {e}")
            return {}

        table_names = [row[0] for row in rows]
        if not table_names:
            logger.warning(f"discover_json_schema: no tables found in {source_schema}")
            return {}

        logger.info(f"[DISCOVER] {source_schema}: found {len(table_names)} tables: {', '.join(table_names)}")

        discovered: Dict[str, Dict[str, Any]] = {}

        for tbl in table_names:
            tbl_q = f'"{tbl}"'
            try:
                data_col = yaml_data_column or self._find_payload_column(source_schema_q, tbl_q) or "data"

                # Soft-validate payload shape from a sample row when available.
                sample = self.trino.execute_query(
                    f"SELECT {data_col} FROM {source_schema_q}.{tbl_q} LIMIT 1"
                ).fetchone()
                if sample and sample[0] is not None:
                    sample_str = str(sample[0]).strip().lstrip("\ufeff")
                    if sample_str and not sample_str.startswith("{"):
                        logger.warning(
                            f"  {tbl}: payload in column '{data_col}' does not look like JSON "
                            f"(starts with {sample_str[:20]!r}); discovery will continue"
                        )

                entity_key = tbl.rstrip("s") if tbl.endswith("s") and len(tbl) > 1 else tbl
                business_key = f"{entity_key}_id"

                table_cfg: Dict[str, Any] = {
                    "format": fmt,
                    "source_schema": source_schema,
                    "target_schema": target_schema,
                    "schema_location": schema_location,
                    "data_column": data_col,
                    "root_key": tbl,
                    "entity_key": entity_key,
                    "target_table": tbl,
                    "business_key": business_key,
                    "sort_by": business_key,
                }

                discovered[tbl] = table_cfg
                logger.info(f"  [OK] {tbl}: entity_key={entity_key}, data_column={data_col}")
            except Exception as e:
                logger.error(f"  {tbl}: discovery error — {e}")
                continue

        return discovered
