# json_parser2 — Project Work Summary

## Overview

BU Denormalization Pipeline — two independent pipelines orchestrated via Dagster:

| Pipeline | Source | Target |
|---|---|---|
| JSON | `25live_raw` | `25live_raw_denormalized` |
| XML (Symplectic) | `mycv_raw` | `mycv_raw_denormalized` |

**Stack:** Python → Trino → Apache Iceberg on S3 (nonprod: `buaws-datalake-nonprod-s3`)

---

## Tables Configured

### JSON Pipeline (25live_raw)
| Table | Child Tables |
|---|---|
| events | event_history |
| organizations | — |
| reservations | — |
| resources | — |
| spaces | — |
| contacts | contact_r25user, contact_address |

### XML Pipeline (mycv_raw)
| Table | Child Tables |
|---|---|
| grant | grant_records, grant_addresses, grant_fields |
| group | group_properties, group_membership, group_records |
| professional_activity | activity_records |
| publication | publication_records, publication_authors, publication_keywords, publication_funding, publication_identifiers, publication_links, publication_subtypes, publication_journal |
| teaching_activity | teaching_activity_records |
| user | user_org_data, user_search_settings, user_addresses, user_identifiers |

---

## Code Review Findings

### Strengths
- Schema drift tolerance — `ALTER TABLE ADD COLUMN` only, never drops columns
- Secrets via Dagster `EnvVar` — never hardcoded in code
- Reserved SQL keyword quoting (`group`, `user`) handled correctly
- Dynamic batch sizing to stay under Trino's 1MB query-text limit
- Thread-based parallelism for parsing and child table processing
- Asyncio semaphore (max 3 concurrent tables) at pipeline level
- Auto-discovery of new tables from source schemas at runtime
- YAML manual config always takes precedence over auto-discovered tables
- `sorted_by` fallback if cluster does not support the property

### Issues Found

| Severity | Issue | Location |
|---|---|---|
| Medium | Child asset re-runs full parent denormalization (TRUNCATE + re-INSERT) when materialized independently | `dagster_assets.py:230` |
| Medium | TrinoClient connections never closed — no teardown in `TrinoEngineResource` | `trino_client.py` |
| Medium | Auto-discovery queries missing catalog prefix (`iceberg.`) — relies on Trino session default | `schema_autodiscovery.py:58` |
| Low | Hardcoded `"25live_raw"` fallback in `_run_tables_async` — wrong default for XML tables | `dagster_assets.py:344` |
| Low | Nonprod S3 paths hardcoded at module load in `_build_table_assets` | `dagster_assets.py:274` |
| Low | 50% presence threshold in auto-discovery may drop sparse but valid child tables | `schema_autodiscovery.py:103` |
| Low | `fetch_table_data` only quotes digit-prefixed names, not reserved keywords | `trino_client.py:91` |
| Cleanup | `_ensure_sorted_by` method defined but never called — dead code | `denormalization_engine.py:495` |
| Cleanup | `load_schema_config`, `load_environment_config`, `get_environment_config` — deprecated/unused | `config_loader.py:110` |

---

## Change Discussed — Dagster Asset Lineage Fix

### Problem
In the Dagster UI asset lineage graph, per-table assets (`reservations`, `resources`, `spaces`, etc.) were showing direct edges from the **source schema** (`25live_raw`), making them visually appear as source-side assets rather than target-side assets.

**Current (incorrect) lineage:**
```
25live_raw  →  25live_raw_denormalized
25live_raw  →  reservations
25live_raw  →  resources
25live_raw  →  spaces
```

### Fix Required
Per-table parent assets should depend on the **target pipeline asset** (`25live_raw_denormalized` / `mycv_raw_denormalized`), not the source.

**Correct lineage:**
```
25live_raw  →  25live_raw_denormalized  →  reservations
                                        →  resources
                                        →  spaces
                                        →  events  →  event_history
```

### Code Change Needed (`dagster_assets.py`)

**`_build_parent_asset`** — change `deps`:
```python
# Before
deps=[source_asset]          # points to 25live_raw

# After
deps=[target_pipeline_key]   # points to 25live_raw_denormalized
```

**`_build_child_asset`** — remove source dep, keep only parent:
```python
# Before
deps=[source_asset, parent_asset_key]

# After
deps=[parent_asset_key]
```

**`_build_table_assets`** — pass target key instead of source asset:
```python
# Before
source_asset=live25_raw_source  (or mycv_raw_source)

# After
target_pipeline_key=AssetKey("25live_raw_denormalized")  (or AssetKey("mycv_raw_denormalized"))
```

### Status
- Identified and discussed — **pending implementation**

---

## Schedule

Daily run at **9:00 AM** via `denormalization_daily_schedule` (cron: `0 9 * * *`).
Both JSON and XML pipelines run together in a single Dagster job: `denormalization_job`.
