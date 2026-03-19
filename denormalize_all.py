#!/usr/bin/env python3
"""
Simple Denormalization Pipeline
Denormalizes all configured tables (no auto-discovery).
Optimized with parallel processing for maximum speed.
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
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Set to False to process both JSON and XML tables together.
XML_ONLY = True

# Limit rows fetched per XML/symplectic table during testing.
# Set to None to process all rows.
XML_TEST_LIMIT = 100


def process_table(engine, config_loader, table_name, table_config_override=None):
    """Process a single table (called in parallel)."""
    try:
        table_config = table_config_override or config_loader.get_table_config(table_name)
        results = engine.run_table(
            source_schema=table_config.get('source_schema', '25live_raw'),
            source_table=table_name,
            table_config=table_config,
            config_loader=config_loader,
            target_schema=table_config.get('target_schema')
        )
        return table_name, results, None
    except Exception as e:
        return table_name, None, str(e)


def main():
    """Main entry point."""
    try:
        # Load environment variables
        host = os.getenv('TRINO_HOST')
        user = os.getenv('TRINO_USER')
        password = os.getenv('TRINO_PASSWORD')
        
        if not all([host, user, password]):
            raise ValueError("Missing required environment variables: TRINO_HOST, TRINO_USER, TRINO_PASSWORD")
        
        # Initialize Trino client
        trino = TrinoClient(
            host=host,
            port=443,
            user=user,
            password=password,
            catalog='iceberg'
        )
        trino.connect()
        logger.info(f"[OK] Connected to Trino at {host}")
        
        # Initialize config loader
        config_dir = Path(__file__).parent / "config"
        config_loader = ConfigLoader(str(config_dir))
        config_loader.load_table_config()
        logger.info(f"[OK] Loaded config from {config_dir}")
        
        # Initialize denormalization engine
        engine = DenormalizationEngine(
            trino_client=trino,
            catalog="iceberg",
            target_schema="25live_raw_denormalized",
            schema_location="s3a://buaws-datalake-nonprod-s3/raw/25live_raw_denormalized"
        )
        
        # Get configured tables
        config_data = config_loader.load_table_config()
        if isinstance(config_data, dict) and 'tables' in config_data:
            config_tables = config_data['tables']
        else:
            config_tables = config_data
        table_names = list(config_tables.keys())

        # Auto-discover additional tables from source_schemas (e.g. mycv_raw)
        logger.info("[DISCOVER] Scanning source_schemas for additional tables...")
        discovered_tables = config_loader.get_source_schema_tables(trino)
        if discovered_tables:
            logger.info(f"[DISCOVER] Found {len(discovered_tables)} table(s): {', '.join(discovered_tables)}")
            # Merge: manual config takes precedence (already guaranteed inside get_source_schema_tables)
            config_tables = {**config_tables, **discovered_tables}
            table_names = list(config_tables.keys())
        else:
            logger.info("[DISCOVER] No additional tables discovered.")

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
        
        # Process each table (in parallel)
        logger.info("\n" + "="*70)
        logger.info("DENORMALIZATION PIPELINE (Parallel Processing)")
        logger.info("="*70 + "\n")
        
        success_count = 0
        failed_tables = []
        
        # Use parallel processing: up to 3 tables at a time
        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = {
                executor.submit(
                    process_table,
                    engine,
                    config_loader,
                    table_name,
                    config_tables.get(table_name),   # pass full config (covers discovered tables)
                ): table_name
                for table_name in table_names
            }
            
            for idx, future in enumerate(as_completed(futures), 1):
                table_name = futures[future]
                try:
                    tbl, results, error = future.result()
                    
                    if error:
                        logger.error(f"[{idx}/{len(table_names)}] [FAILED] {table_name} FAILED: {error}")
                        failed_tables.append(table_name)
                    else:
                        logger.info(
                            f"[{idx}/{len(table_names)}] [OK] {table_name}: "
                            f"{results['rows_inserted']}/{results['rows_processed']} rows, "
                            f"{results['columns_added']} new columns"
                        )
                        
                        if results.get('child_results'):
                            for child_name, child_info in results['child_results'].items():
                                logger.info(f"     - {child_name}: {child_info.get('rows_inserted', 0)} rows")
                        
                        success_count += 1
                
                except Exception as e:
                    logger.error(f"[{idx}/{len(table_names)}] [FAILED] {table_name} ERROR: {e}")
                    failed_tables.append(table_name)
                continue
        
        # Summary
        logger.info("\n" + "="*70)
        logger.info(f"SUMMARY: {success_count}/{len(table_names)} tables processed successfully")
        if failed_tables:
            logger.warning(f"Failed tables: {', '.join(failed_tables)}")
        else:
            logger.info("[DONE] All tables denormalized successfully!")
        logger.info("="*70)
        
        return 0 if len(failed_tables) == 0 else 1
    
    except Exception as e:
        logger.error(f"[FATAL] FATAL ERROR: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
