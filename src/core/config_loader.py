"""
Configuration Loader
Loads and manages YAML-based configuration for table mappings and schema definitions.
Also supports auto-discovery of child tables from raw JSON.
"""

import yaml
import logging
from pathlib import Path
from typing import Dict, Any
from src.core.schema_autodiscovery import SchemaAutodiscovery

logger = logging.getLogger(__name__)


class ConfigLoader:
    """
    Loads YAML configuration files for table metadata and denormalization rules.
    """
    
    def __init__(self, config_dir: str = None):
        """
        Initialize config loader.
        
        Args:
            config_dir: Path to configuration directory
        """
        self.config_dir = Path(config_dir) if config_dir else Path(__file__).parent.parent.parent / "config"
        self.table_config = {}
        self.schema_config = {}
    
    def load_table_config(self, filename: str = "table_config.yaml") -> Dict[str, Any]:
        """
        Load table configuration from YAML file.
        
        Args:
            filename: YAML filename in config directory
        
        Returns:
            Dictionary with table mappings
        """
        config_path = self.config_dir / filename
        
        try:
            with open(config_path, 'r') as f:
                self.table_config = yaml.safe_load(f) or {}
            logger.info(f"Loaded table config from {config_path}")
            return self.table_config
        except FileNotFoundError:
            logger.warning(f"Config file not found: {config_path}")
            return {}
        except Exception as e:
            logger.error(f"Failed to load config: {str(e)}")
            raise
    
    def load_schema_config(self, filename: str = "schema_definitions.yaml") -> Dict[str, Any]:
        """
        Load schema definitions from YAML file.
        
        Args:
            filename: YAML filename in config directory
        
        Returns:
            Dictionary with schema definitions
        """
        config_path = self.config_dir / filename
        
        try:
            with open(config_path, 'r') as f:
                self.schema_config = yaml.safe_load(f) or {}
            logger.info(f"Loaded schema config from {config_path}")
            return self.schema_config
        except FileNotFoundError:
            logger.warning(f"Schema config file not found: {config_path}")
            return {}
        except Exception as e:
            logger.error(f"Failed to load schema config: {str(e)}")
            raise
    
    def _tables(self) -> Dict[str, Any]:
        """Return the tables sub-dict from loaded config (handles top-level 'tables:' key)."""
        if "tables" in self.table_config:
            return self.table_config["tables"]
        return self.table_config

    def get_business_key(self, table: str) -> str:
        """
        Get business key for a table from configuration.
        
        Args:
            table: Table name
        
        Returns:
            Business key column name
        """
        return self._tables().get(table, {}).get("business_key", f"{table}_id")
    
    def get_table_config(self, table: str) -> Dict[str, Any]:
        """
        Get configuration for specific table.
        
        Args:
            table: Table name
        
        Returns:
            Table configuration dictionary
        """
        return self._tables().get(table, {})
    
    def load_environment_config(self, filename: str = "prod.yaml") -> Dict[str, Any]:
        """
        [DEPRECATED] Load environment-specific configuration.
        Environment is now configured via environment variables (TRINO_HOST, TRINO_USER, etc).
        """
        logger.warning("load_environment_config() is deprecated. Use environment variables instead.")
        return {}
    
    def get_environment_config(self) -> Dict[str, Any]:
        """
        [DEPRECATED] Get environment configuration.
        Environment is now configured via environment variables.
        """
        return {}
    def auto_discover_child_tables(
        self,
        trino_client,
        source_schema: str,
        table: str,
        table_config: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Auto-discover child tables from raw JSON, merge with manually configured ones.

        Args:
            trino_client: TrinoClient instance for querying raw data
            source_schema: Source schema name (e.g. "25live_raw")
            table: Table name
            table_config: Table configuration dict from YAML

        Returns:
            Enhanced table_config with discovered child tables merged in
        """
        root_key = table_config.get("root_key", table)
        entity_key = table_config.get("entity_key", table.rstrip("s"))  # Plural → singular
        business_key = table_config.get("business_key", f"{entity_key}_id")

        try:
            discovery = SchemaAutodiscovery(trino_client)
            
            logger.info(f"  Auto-discovering child tables for {table}...")
            data_column = table_config.get("data_column", "data")
            discovered = discovery.discover_child_tables(
                source_schema, table, root_key, entity_key, business_key, data_column
            )

            # Merge: manual config takes precedence, discovered fills gaps
            manual_children = table_config.get("child_tables", {})
            all_children = {**discovered, **manual_children}

            if discovered:
                logger.info(f"  Found {len(discovered)} auto-discovered child table(s)")
            if all_children:
                table_config["child_tables"] = all_children

            return table_config

        except Exception as e:
            logger.warning(f"  Auto-discovery failed for {table}: {e}")
            # Return original config if discovery fails
            return table_config

    def get_source_schema_tables(
        self,
        trino_client,
        include_formats: set[str] | None = None,
    ) -> Dict[str, Any]:
        """
        Auto-discover all tables from every schema listed under `source_schemas`
        in table_config.yaml, and return a merged table-config dict.

        Tables that are already explicitly configured in the `tables` section
        are NOT overridden — the manual config always takes precedence.

        Args:
            trino_client: Connected TrinoClient instance

        Returns:
            { table_name: config_dict } for all discovered tables that are NOT
            already present in the manually-configured `tables` section.
        """
        raw = self.table_config
        source_schemas = (
            raw.get("source_schemas", {}) if isinstance(raw, dict) else {}
        )
        if not source_schemas:
            return {}

        # Tables already configured manually — discovery must not override them
        manual_tables = set(self._tables().keys())

        discovery = SchemaAutodiscovery(trino_client)
        all_discovered: Dict[str, Any] = {}

        allowed_formats = {f.lower() for f in include_formats} if include_formats else None

        for schema_name, schema_defaults in source_schemas.items():
            fmt = (schema_defaults or {}).get("format", "json")

            if allowed_formats and fmt.lower() not in allowed_formats:
                logger.debug(
                    f"get_source_schema_tables: skipping schema '{schema_name}' "
                    f"with format '{fmt}' (not in include_formats={sorted(allowed_formats)})"
                )
                continue

            if fmt == "symplectic":
                found = discovery.discover_symplectic_schema(
                    source_schema=schema_name,
                    schema_defaults=schema_defaults or {},
                )
            elif fmt == "json":
                found = discovery.discover_json_schema(
                    source_schema=schema_name,
                    schema_defaults=schema_defaults or {},
                )
            else:
                logger.warning(
                    f"get_source_schema_tables: unsupported format '{fmt}' "
                    f"for schema '{schema_name}' — skipping"
                )
                continue

            # Only add tables not already in the manual config
            for tbl, cfg in found.items():
                if tbl not in manual_tables:
                    all_discovered[tbl] = cfg
                else:
                    logger.info(
                        f"  {tbl}: already in manual config — skipping auto-discovered version"
                    )

        return all_discovered