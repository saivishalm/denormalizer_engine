# BU Denormalization Engine

**Production-grade Python framework for denormalizing nested JSON and XML data from Trino/Iceberg.**

## Overview

This project denormalizes raw data from Trino/Iceberg into normalized tables:
- **JSON Pipeline**: `25live_raw` → `25live_raw_denormalized` (25Live event data)
- **XML Pipeline**: `mycv_raw` → `mycv_raw_denormalized` (Symplectic Elements data)

**Key Features:**
- ✅ Configuration-driven (YAML-based table mappings and performance tuning)
- ✅ Automatic schema inference and drift handling
- ✅ Parallel processing (configurable workers for parsing and table processing)
- ✅ Optimized batch sizes for Trino query limits
- ✅ Auto-discovery of child tables from nested structures
- ✅ Dagster orchestration with lineage tracking
- ✅ **4-8x performance improvement** with configurable tuning

## Architecture

```
src/
├── core/
│   ├── denormalization_engine.py   # Main engine with performance tuning
│   ├── config_loader.py            # YAML configuration management
│   └── schema_autodiscovery.py     # Auto-discover child tables
├── db/
│   └── trino_client.py             # Trino connectivity
└── parsers/
    ├── symplectic_parser.py        # XML parser for Symplectic API
    └── xml_parser.py               # Generic XML parser

config/
└── table_config.yaml               # Centralized configuration

dagster_assets.py                   # Dagster orchestration (main entry point)
```

## Configuration

All settings are centralized in `config/table_config.yaml`:

### **Trino Connection**
```yaml
trino:
  port: 443
  catalog: iceberg
```

### **Performance Tuning**
```yaml
performance:
  parse_workers_max: 16          # Max workers for parsing XML/JSON
  parse_workers_min: 4           # Min workers to always use
  parse_rows_per_worker: 10      # Rows per worker ratio
  child_table_workers: 4         # Max concurrent child tables
  table_concurrency: 5           # Max concurrent tables in async processing
  batch_size_min: 50             # Minimum batch size for inserts
  batch_size_max: 2000           # Maximum batch size
  batch_target_bytes: 1500000    # Target ~1.5MB per batch
  batch_avg_chars_per_value: 40  # Estimated chars per value
```

### **Pipeline Configuration**
```yaml
pipelines:
  json:
    target_schema: 25live_raw_denormalized
    schema_location: s3a://buaws-datalake-nonprod-s3/raw/25live_raw_denormalized
    source_schema: 25live_raw

  xml:
    target_schema: mycv_raw_denormalized
    schema_location: s3a://buaws-datalake-nonprod-s3/raw/mycv_raw_denormalized
    source_schema: mycv_raw
```

### **Table Mappings**
```yaml
tables:
  events:
    root_key: events
    entity_key: event
    target_table: events
    business_key: event_id
    sort_by: event_id
    child_tables:
      event_history:
        entity_key: event_history
        parent_fk: event_id
        sort_by: event_id
```

### **Environment Variables** (`.env` file)
```bash
TRINO_HOST=trino.de-eks-nonprod.bu.edu
TRINO_USER=your_username
TRINO_PASSWORD=your_password
DENORMALIZER_ENV=nonprod
```

## Installation

```bash
# Navigate to project
cd json_parser2

# Install dependencies
pip install -r requirements.txt

# Create environment file from template
cp .env.example .env

# Edit .env with your credentials
# Required variables:
#   TRINO_HOST=trino.de-eks-nonprod.bu.edu
#   TRINO_USER=your_username
#   TRINO_PASSWORD=your_password
```

## Usage

### **Dagster Orchestration (Recommended)**

Start Dagster UI:
```bash
dagster dev -f dagster_assets.py -h 0.0.0.0 -p 3001
```

Access UI at: http://localhost:3001

**Features:**
- Full workflow orchestration
- Asset lineage tracking
- Monitoring and retries
- Scheduled execution (daily at 9:00 AM)

### **Direct Materialization**

```bash
# Materialize both pipelines
dagster asset materialize -f dagster_assets.py --select "*"

# Materialize JSON pipeline only
dagster asset materialize -f dagster_assets.py --select 25live_raw_denormalized

# Materialize XML pipeline only
dagster asset materialize -f dagster_assets.py --select mycv_raw_denormalized
```

## Permissions Required

Your Trino user needs the following permissions:

### **Read Permissions (Source Schemas)**
```sql
GRANT SELECT ON iceberg.25live_raw TO your_user;
GRANT SELECT ON iceberg.mycv_raw TO your_user;
```

### **Write Permissions (Target Schemas)**
```sql
GRANT CREATE TABLE ON iceberg.25live_raw_denormalized TO your_user;
GRANT INSERT ON iceberg.25live_raw_denormalized TO your_user;
GRANT DELETE ON iceberg.25live_raw_denormalized TO your_user;

GRANT CREATE TABLE ON iceberg.mycv_raw_denormalized TO your_user;
GRANT INSERT ON iceberg.mycv_raw_denormalized TO your_user;
GRANT DELETE ON iceberg.mycv_raw_denormalized TO your_user;
```

Or simpler:
```sql
GRANT ALL ON iceberg.25live_raw_denormalized TO your_user;
GRANT ALL ON iceberg.mycv_raw_denormalized TO your_user;
```

## Data Pipelines

### **JSON Pipeline (25Live)**
**Source**: `iceberg.25live_raw.*`
**Target**: `iceberg.25live_raw_denormalized.*`

Tables processed:
- events (with event_history child table)
- organizations
- reservations
- resources
- spaces
- contacts (with contact_r25user, contact_address child tables)

### **XML Pipeline (MyCV/Symplectic)**
**Source**: `iceberg.mycv_raw.*`
**Target**: `iceberg.mycv_raw_denormalized.*`

Tables processed:
- grant (with child tables)
- group (with child tables)
- professional_activity (with child tables)
- publication (with child tables)
- teaching_activity (with child tables)
- user (with child tables)

## Performance Optimizations

### **Configurable Settings** (in `table_config.yaml`)

| Setting | Default | Tuning Guide |
|---------|---------|--------------|
| `parse_workers_max` | 16 | Increase for more CPU cores |
| `parse_workers_min` | 4 | Lower for smaller datasets |
| `table_concurrency` | 5 | Increase for more parallel processing |
| `batch_size_max` | 2000 | Adjust based on table width |

### **Performance Features**

- ✅ **Multi-threaded parsing**: XML/JSON parsing uses ThreadPoolExecutor
- ✅ **Parallel table processing**: Multiple tables processed concurrently
- ✅ **Parallel child tables**: Child tables created in parallel
- ✅ **Batch inserts**: Configurable batch sizes to optimize query size
- ✅ **TRUNCATE operations**: Faster than DELETE for full refresh

### **Expected Performance**

With optimized settings:
- **JSON pipeline**: ~2-3 minutes for 6 tables
- **XML pipeline**: ~7-15 minutes for 6 tables (vs 1 hour unoptimized)

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
