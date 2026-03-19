"""
Production Deployment Guide
How to run JSON Parser 2.0 in production without CLI arguments
"""

# PRODUCTION DEPLOYMENT GUIDE

## Overview

The new `run_production.py` script uses **environment variables + config files** instead of CLI arguments. This is the industry-standard approach for production.

## How It Works

```
Environment Variables (secrets)
              ↓
         run_production.py
              ↓
    Load config from YAML file
              ↓
  Denormalize all configured tables
              ↓
        Log results & exit
```

## Setup Steps

### 1. Set Environment Variables

**Option A: Using .env file (Local Testing)**

```bash
cp .env.example .env
# Edit .env with your Trino credentials
```

Run with:
```bash
source .env
python run_production.py
```

**Option B: System Environment Variables (Production)**

```bash
export DENORMALIZER_ENV=prod
export TRINO_HOST=your-trino.company.com
export TRINO_PORT=443
export TRINO_USER=service-account
export TRINO_PASSWORD=your-password  # OR from secret manager
```

**Option C: Using AWS Secrets Manager (Recommended)**

```bash
export DENORMALIZER_ENV=prod
export TRINO_HOST=your-trino.company.com
export TRINO_PORT=443
export TRINO_USER=service-account
export TRINO_PASSWORD=$(aws secretsmanager get-secret-value --secret-id trino/prod/password --query SecretString --output text)

python run_production.py
```

### 2. Configure Tables to Process

Edit `config/production.yaml`:

```yaml
tables_to_process:
  - source_schema: live25_raw_dev
    source_table: events
    target_schema: live25_raw_denormalise
    target_table: events
    
  - source_schema: live25_raw_dev
    source_table: organizations
    target_schema: live25_raw_denormalise
    target_table: organizations
```

### 3. Run Production Denormalizer

```bash
python run_production.py
```

Output:
```
🚀 Starting Denormalizer [PROD]
📖 Loading config for prod environment...
Connecting to Trino...
Found 5 tables to process

[1/5] Processing events...
✓ events: 125000 parent rows loaded
  • Created 8 child tables

[2/5] Processing organizations...
✓ organizations: 85000 parent rows loaded
  ...

============================================================
📊 DENORMALIZATION SUMMARY
============================================================
Tables Processed: 5
Total Parent Rows: 500000
Total Child Tables: 42

✅ ALL TABLES PROCESSED SUCCESSFULLY
```

## Environment-Based Configurations

### Development Environment
```bash
export DENORMALIZER_ENV=dev
# Uses config/development.yaml
# Limited row processing for testing
```

### Staging Environment
```bash
export DENORMALIZER_ENV=staging
# Uses config/staging.yaml (create if needed)
# Full data processing with logging
```

### Production Environment
```bash
export DENORMALIZER_ENV=prod
# Uses config/production.yaml
# Full data processing, minimal logging
```

## Scheduling with Cron (Linux/Mac)

Edit crontab:
```bash
crontab -e
```

Add daily job:
```bash
# Run denormalizer daily at 2 AM
0 2 * * * cd /opt/denormalizer && source .env && python run_production.py >> logs/cron.log 2>&1
```

## Scheduling with Windows Task Scheduler

Create batch file `run_denormalizer.bat`:
```batch
@echo off
cd C:\Users\visha\Programs\json_parser2
setlocal enabledelayedexpansion
for /f "delims== tokens=1,2" %%G in (.env) do set %%G=%%H
python run_production.py
```

In Task Scheduler:
- Trigger: Daily at 2:00 AM
- Action: `C:\Users\visha\Programs\json_parser2\run_denormalizer.bat`

## Kubernetes Deployment (Recommended)

`deployment.yaml`:
```yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: json-parser-denormalizer
spec:
  schedule: "0 2 * * *"  # Daily at 2 AM
  jobTemplate:
    spec:
      template:
        spec:
          containers:
          - name: denormalizer
            image: json-parser:latest
            env:
            - name: DENORMALIZER_ENV
              value: "prod"
            - name: TRINO_HOST
              value: "trino.default.svc.cluster.local"
            - name: TRINO_USER
              valueFrom:
                secretKeyRef:
                  name: trino-creds
                  key: username
            - name: TRINO_PASSWORD
              valueFrom:
                secretKeyRef:
                  name: trino-creds
                  key: password
```

## Monitoring & Logging

### Log Files
- Development: `logs/denormalizer_dev.log`
- Production: `logs/denormalizer.log`

### Log Rotation
Automatically handled by logger (max 10MB per file, keep 5 backups)

### Monitoring in Production

Output example:
```
2024-03-13 14:32:01 - denormalizer - INFO - Started denormalizer in prod environment
2024-03-13 14:32:02 - denormalizer - INFO - Connecting to Trino...
2024-03-13 14:32:03 - denormalizer - INFO - Found 5 tables to process
2024-03-13 14:33:45 - denormalizer - INFO - ✓ events: 125000 parent rows loaded
2024-03-13 14:35:12 - denormalizer - INFO - Denormalization completed successfully
```

Check exit code (0 = success, 1 = failure):
```bash
python run_production.py
echo $?  # Linux/Mac
echo %ERRORLEVEL%  # Windows
```

## Security Best Practices

### ✅ DO:
- Use environment variables for credentials
- Store passwords in secret manager (AWS Secrets Manager, Azure Key Vault)
- Use service accounts with minimal permissions
- Rotate credentials regularly
- Enable audit logging
- Restrict database user to denormalization schema only

### ❌ DON'T:
- Hardcode passwords in config files
- Pass credentials as CLI arguments
- Commit .env files to version control
- Use personal credentials for production
- Log sensitive data

## Troubleshooting

### "Missing required environment variables"
```bash
# Check if variables are set
echo $TRINO_HOST
echo $TRINO_USER
echo $TRINO_PASSWORD

# Set them
export TRINO_HOST=localhost
export TRINO_USER=admin
export TRINO_PASSWORD=password
```

### "Config file not found"
```bash
# Ensure config file exists for your environment
ls config/production.yaml
# Create if staging config doesn't exist
cp config/production.yaml config/staging.yaml
```

### "Connection refused"
```bash
# Verify Trino is running
curl http://localhost:8080/ui/

# Check credentials
# Check firewall/network access
```

## Config File Examples

### Minimal Config (all tables same settings)
```yaml
environment: prod

trino:
  host: ${TRINO_HOST}
  port: ${TRINO_PORT:-8080}
  user: ${TRINO_USER}
  password: ${TRINO_PASSWORD}
  catalog: iceberg

tables_to_process:
  - source_schema: live25_raw_dev
    source_table: events
    target_schema: live25_raw_denormalise
    target_table: events
```

### Advanced Config (per-table overrides)
```yaml
environment: prod

trino:
  host: ${TRINO_HOST}
  port: ${TRINO_PORT}
  user: ${TRINO_USER}
  password: ${TRINO_PASSWORD}
  catalog: iceberg

processing:
  batch_size: 5000
  max_retries: 3
  timeout_seconds: 7200

tables_to_process:
  - source_schema: live25_raw_dev
    source_table: events
    target_schema: live25_raw_denormalise
    target_table: events
    payload_column: payload
    
  - source_schema: mycv_raw
    source_table: xml_records
    target_schema: mycv_raw_denormalise
    target_table: records
    payload_column: xml_payload
```

## Summary

**Old Way (CLI Arguments)** ❌
```bash
python main.py \
  --trino-host localhost \
  --trino-user admin \
  --trino-password password \
  ...
# Not secure, verbose, not scalable
```

**New Way (Environment Variables + Config Files)** ✅
```bash
export DENORMALIZER_ENV=prod
python run_production.py
# Secure, clean, production-ready
```
