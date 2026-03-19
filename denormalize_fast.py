#!/usr/bin/env python3
"""
Ultra-Fast Denormalization Pipeline
- No auto-discovery (uses pre-configured schemas only)
- Parallel table processing (3 concurrent tables)
- Parallel child table processing (4 threads per table)
- Batch size: 2000 rows per query
- TRUNCATE instead of DELETE

Perfect for scheduled, recurring denormalization runs where schemas are stable.
"""

import os
import sys
import logging
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from src.db.trino_client import TrinoClient
from src.core.config_loader import ConfigLoader
from src.core.denormalization_engine import DenormalizationEngine

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Set to False to process both JSON and XML tables together.
XML_ONLY = True

# Limit rows fetched per XML/symplectic table during testing.
# Set to None to process all rows.
XML_TEST_LIMIT = 100


def process_table_fast(engine, config_loader, table_name, table_config_override=None):
    """Process a single table WITHOUT auto-discovery (called in parallel)."""
    try:
        table_config = table_config_override or config_loader.get_table_config(table_name)
        results = engine.run_table(
            source_schema=table_config.get('source_schema', '25live_raw'),
            source_table=table_name,
            table_config=table_config,
            config_loader=None,  # NO auto-discovery = faster!
            target_schema=table_config.get('target_schema')
        )
        return table_name, results, None
    except Exception as e:
        return table_name, None, str(e)


def main():
    """Main entry point."""
    try:
        # Load environment
        host = os.getenv('TRINO_HOST')
        user = os.getenv('TRINO_USER')
        password = os.getenv('TRINO_PASSWORD')
        
        if not all([host, user, password]):
            raise ValueError("Missing: TRINO_HOST, TRINO_USER, TRINO_PASSWORD")
        
        logger.info("[PIPELINE] Fast Denormalization Pipeline (No Auto-Discovery)")
        logger.info(f"[TRINO] {host}:443")
        
        # Initialize Trino
        trino = TrinoClient(host=host, port=443, user=user, password=password, catalog='iceberg')
        trino.connect()
        
        # Initialize config loader
        config_dir = Path(__file__).parent / "config"
        config_loader = ConfigLoader(str(config_dir))
        config_data = config_loader.load_table_config()
        
        if isinstance(config_data, dict) and 'tables' in config_data:
            config_tables = config_data['tables']
        else:
            config_tables = config_data

        # Auto-discover additional tables from source_schemas (e.g. mycv_raw)
        discovered_tables = config_loader.get_source_schema_tables(trino)
        if discovered_tables:
            logger.info(f"[DISCOVER] +{len(discovered_tables)} table(s): {', '.join(discovered_tables)}")
            config_tables = {**config_tables, **discovered_tables}

        # Filter to XML/Symplectic tables only when XML_ONLY mode is active
        if XML_ONLY:
            config_tables = {k: v for k, v in config_tables.items() if v.get('format') == 'symplectic'}
            logger.info(f"[XML_ONLY] Filtered to {len(config_tables)} symplectic table(s)")

        # Inject test row limit into each symplectic table config when testing
        if XML_TEST_LIMIT:
            config_tables = {
                k: {**v, 'test_limit': XML_TEST_LIMIT} if v.get('format') == 'symplectic' else v
                for k, v in config_tables.items()
            }
            logger.info(f"[XML_TEST] Limiting symplectic tables to {XML_TEST_LIMIT} rows each")

        table_names = list(config_tables.keys())
        logger.info(f"[CONFIG] Tables: {', '.join(table_names)} ({len(table_names)} total)\n")
        
        # Initialize engine
        engine = DenormalizationEngine(
            trino_client=trino,
            catalog="iceberg",
            target_schema="25live_raw_denormalized",
            schema_location="s3a://buaws-datalake-nonprod-s3/raw/25live_raw_denormalized"
        )
        
        logger.info("="*70)
        logger.info("PARALLEL PROCESSING (3 tables at a time)")
        logger.info("="*70 + "\n")
        
        success_count = 0
        failed_tables = []
        
        # Process tables in parallel (3 at a time)
        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = {
                executor.submit(
                    process_table_fast,
                    engine,
                    config_loader,
                    tbl,
                    config_tables.get(tbl),   # pass full config (covers discovered tables)
                ): tbl
                for tbl in table_names
            }
            
            for idx, future in enumerate(as_completed(futures), 1):
                table_name = futures[future]
                try:
                    _, results, error = future.result()
                    
                    if error:
                        logger.error(f"[{idx}] [FAILED] {table_name:20s} | ERROR: {error[:60]}")
                        failed_tables.append(table_name)
                    else:
                        child_count = len(results.get('child_results', {}))
                        logger.info(
                            f"[{idx}] [OK] {table_name:20s} | "
                            f"{results['rows_inserted']:6d}/{results['rows_processed']:6d} rows | "
                            f"{child_count} children"
                        )
                        success_count += 1
                
                except Exception as e:
                    logger.error(f"[{idx}] [FAILED] {table_name:20s} | EXCEPTION: {str(e)[:60]}")
                    failed_tables.append(table_name)
        
        # Summary
        logger.info("\n" + "="*70)
        logger.info(f"[COMPLETE] {success_count}/{len(table_names)} tables processed")
        if failed_tables:
            logger.warning(f"[WARNING] Failed: {', '.join(failed_tables)}")
        logger.info("="*70)
        
        return 0 if len(failed_tables) == 0 else 1
    
    except Exception as e:
        logger.error(f"[FATAL] {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
