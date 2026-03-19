# JSON Parser 2.0 - 25Live Denormalization Framework

**Production-grade Python framework for denormalizing nested JSON payloads from Trino/Iceberg.**

## Overview

This project denormalizes raw 25Live JSON data from `iceberg.25live_raw` into normalized Iceberg tables in `iceberg.25live_raw_denormalized`.

**Key Features:**
- ✅ Configuration-driven (YAML-based table mappings)
- ✅ Automatic schema inference and drift handling
- ✅ Parallel processing (3 concurrent main tables, 4 concurrent child tables)
- ✅ Optimized batch size (500 rows) for Trino query limits
- ✅ TRUNCATE-based table clearing (90% faster than DELETE)
- ✅ Auto-discovery of child tables from JSON arrays
- ✅ Dagster integration with lineage tracking
- ✅ **17-23x performance improvement** vs original implementation

**Execution Time:** All 6 tables in **~1 minute 46 seconds** (March 16, 2026 benchmark)

## Architecture

```
src/
├── core/
│   ├── denormalization_engine.py   # Main engine (generic + optimized)
│   ├── config_loader.py            # YAML configuration management
│   └── schema_autodiscovery.py     # Auto-discover child tables
├── db/
│   ├── trino_client.py             # Trino connectivity
│   └── sql_builder.py              # SQL generation
└── utils/
    └── logger.py                   # Logging setup

config/
├── table_config.yaml               # Table mappings (6 tables)

dagster_pipeline/
├── assets/                         # Denormalization assets
├── resources/                      # Trino/Config resources
├── shared/                         # Retry policies
└── definitions.py                  # Dagster entry point
```

## Configuration

**Table Configuration** (`config/table_config.yaml`):
```yaml
tables:
  events:
    root_key: events
    entity_key: event
    target_table: events
    business_key: event_id
    sort_by: event_id
```

**Environment Variables** (via `setup_env.ps1` or system):
```powershell
$env:DENORMALIZER_ENV = "prod"
$env:TRINO_HOST = "trino.de-eks-nonprod.bu.edu"
$env:TRINO_PORT = "443"           # HTTPS
$env:TRINO_USER = "dataeng_svmiriya"
$env:TRINO_PASSWORD = "[secret]"  # Use AWS Secrets Manager in prod
```

## Usage

### Option 1: Fast Parallel Script (Recommended for Scheduled Jobs) ⭐

```powershell
cd c:\Users\visha\Programs\json_parser2
. .\setup_env.ps1
python denormalize_fast.py
```

**Time:** ~1m46s for all 6 tables (Feb 2026 benchmark shows this is much faster)
**Features:** No auto-discovery (uses pre-configured tables only)  
**Perfect for:** Hourly/daily scheduled jobs with stable schemas

### Option 2: Full Discovery Script (Weekly Runs)

```powershell
python denormalize_all.py
```

**Time:** ~10-13 minutes for 6 tables  
**Features:** Parallel processing + auto-discovery for new tables  
**Perfect for:** Weekly full runs, when schemas might change

### Option 3: Dagster Orchestration

```bash
# Install
pip install -r requirements.txt

# Run UI (development)
dagster dev

# Execute job (production)
dagster job execute -j denormalize_25live
```

**Features:** Full workflow orchestration, lineage tracking, monitoring  
**Perfect for:** Production orchestration with audit trail

## Tables Processed (6 Total)

| Table | Status |
|-------|--------|
| events | ✅ Denormalized |
| organizations | ✅ Denormalized |
| reservations | ✅ Denormalized |
| resources | ✅ Denormalized |
| spaces | ✅ Denormalized |
| contacts | ✅ Denormalized (31,164 rows) |

## Optimizations (17-23x Speedup)

| Optimization | Before | After | Impact |
|---|---|---|---|
| Batch size | 100 rows | 500 rows | 5x fewer queries |
| Truncate method | DELETE | TRUNCATE | 80-90% faster |
| Child tables | Sequential | 4 parallel threads | 3-10x faster |
| Main tables | Sequential | 3 parallel | 3x faster |
| Auto-discovery | Always | Optional | 30-40% time saved |
| Logging | Every 1000 rows | Every 5000 rows | 5-10% faster |

**Total:** 30-40 minutes → **1 minute 46 seconds**

## Schema Drift Handling

Automatic schema detection and evolution:
- ✅ Detects new columns from JSON payloads
- ✅ Adds columns via `ALTER TABLE ADD COLUMN`
- ✅ Never drops existing columns
- ✅ Tracks column additions in results

**Example:**
```python
results = engine.run_table(...)
print(results['columns_added'])  # e.g., ['new_field_1', 'new_field_2']
```

## Dagster Lineage Tracking

Automatic data lineage with full traceability:

```
iceberg.25live_raw.events 
  ↓ (denormalize)
iceberg.25live_raw_denormalized.events
iceberg.25live_raw_denormalized.event_history (child table)

iceberg.25live_raw.organizations
  ↓ (denormalize)
iceberg.25live_raw_denormalized.organizations
```

**Tracked in Dagster UI:**
- Data dependencies between source and target tables
- Child table relationships
- Execution metadata (rows processed, inserted, errors)
- Retry history and failure tracking

## Installation

```bash
# Clone or navigate to project
cd json_parser2

# Install dependencies
pip install -r requirements.txt

# Set environment variables
. ./setup_env.ps1   # PowerShell
source ./setup_env.sh  # Bash/Linux

# Verify connection
python -c "
from src.db.trino_client import TrinoClient
import os
tc = TrinoClient(
    host=os.getenv('TRINO_HOST'),
    port=int(os.getenv('TRINO_PORT', 443)),
    user=os.getenv('TRINO_USER'),
    password=os.getenv('TRINO_PASSWORD'),
    catalog='iceberg'
)
tc.connect()
print('✅ Connected to Trino successfully')
"
```

## Directory Structure

```
json_parser2/
├── src/                            # Core code
│   ├── core/
│   │   ├── denormalization_engine.py    # Main engine
│   │   ├── config_loader.py             # Config management
│   │   └── schema_autodiscovery.py      # Child table discovery
│   ├── db/
│   │   ├── trino_client.py              # Trino client
│   │   └── sql_builder.py               # SQL generation
│   └── utils/
│       └── logger.py                    # Logging utilities
├── config/
│   └── table_config.yaml                # Table configurations
├── dagster_pipeline/                    # Dagster orchestration
│   ├── assets/                          # Denormalization assets
│   ├── resources/                       # Resources (Trino, Config)
│   ├── shared/                          # Shared policies & utils
│   └── definitions.py                   # Dagster entry point
├── denormalize_fast.py                  # ⭐ Fast parallel script
├── denormalize_all.py                   # Full parallel script
├── run_production.py                    # Legacy entry point
├── setup_env.ps1                        # PowerShell env setup
├── setup_env.sh                         # Bash env setup
├── requirements.txt                     # Python dependencies
├── dagster.yaml                         # Dagster config
├── conftest.py                          # Pytest config
├── README.md                            # This file
├── OPTIMIZATION_REPORT.md               # Detailed optimization metrics
└── PRODUCTION_DEPLOYMENT.md             # Deployment guide
```

## Monitoring & Logging

**Logs are printed to console** with format:
```
2026-03-16 20:10:18,069 - events: 38000/38000 rows, 5 new columns
2026-03-16 20:10:18,075 - [1] ✅ events | 38000/38000 rows | 1 children
```

**Optional file logging:**
```python
from src.utils.logger import setup_logging
logger = setup_logging(log_file="logs/denormalizer.log", level="INFO")
logger.info("Checkpoint")
```

## Production Deployment

**Recommended Setup:**

1. **Environment Variables** (AWS Secrets Manager):
   ```bash
   export TRINO_HOST=trino.company.com
   export TRINO_PORT=443
   export TRINO_USER=service-account
   export TRINO_PASSWORD=$(aws secretsmanager get-secret-value --secret-id trino/prod/password --query SecretString --output text)
   ```

2. **Scheduled Job** (Windows Task Scheduler):
   ```
   Program: C:\Users\visha\anaconda3\python.exe
   Arguments: C:\Users\visha\Programs\json_parser2\denormalize_fast.py
   Start in: C:\Users\visha\Programs\json_parser2
   ```

3. **Monitoring** (optional):
   - Monitor S3 location: `s3a://buaws-datalake-nonprod-s3/raw/25live_raw_denormalized`
   - Set alerts for job failures
   - Review Dagster UI for lineage and execution history

## Troubleshooting

| Issue | Solution |
|-------|----------|
| `MISSING: TRINO_HOST` | Run `setup_env.ps1` to set environment variables |
| `Query text too large` | Batch size is optimized (500 rows). Reduce in denormalization_engine.py if needed |
| `Connection timeout` | Verify port 443 is open: `Test-NetConnection -ComputerName trino.de-eks-nonprod.bu.edu -Port 443` |
| `Table not found` | Check `config/table_config.yaml` - table must be registered |

## Performance Tips

1. **Use `denormalize_fast.py` for scheduled jobs** (no discovery overhead)
2. **Batch size of 500 rows balances** query size vs network round-trips
3. **TRUNCATE operations use metadata-only clearing** for speed
4. **Parallel workers (3 main, 4 child tables)** can be tuned in source code if needed
5. **Monitor S3 storage costs** - schemas are stored at `s3a://buaws-datalake-nonprod-s3/raw/25live_raw_denormalized`

## Dependencies

- **Python 3.10+** (Anaconda 3.13)
- **Trino 0.20+** with Iceberg support
- **Apache Iceberg** (normalized table format)
- **S3** (schema storage)

See `requirements.txt` for complete list.

## Testing

```bash
# Run Dagster tests with pytest
pytest conftest.py -v

# Manual quick test
. ./setup_env.ps1
python -c "from src.core.config_loader import ConfigLoader; cfg = ConfigLoader(); cfg.load_table_config(); print(cfg.get_table_config('events'))"
```

---

**Last Updated:** March 16, 2026  
**Status:** ✅ Production-Ready  
**Performance:** 17-23x faster than baseline

## Future Enhancements

- [ ] XML parser and support
- [ ] Performance optimization for large datasets
- [ ] Streaming mode for continuous sync
- [ ] Data validation and quality checks
- [ ] Incremental sync capability
- [ ] Monitoring and alerting
- [ ] Skills-based architecture integration
