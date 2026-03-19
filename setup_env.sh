#!/bin/bash
# Production Environment Setup
# Source this file before running the denormalizer

# Option 1: Load from .env file (recommended for local testing)
if [ -f .env ]; then
    set -a
    source .env
    set +a
fi

# Option 2: Set environment variables directly (for deployment scripts)
# Uncomment and configure for your environment

# === DEVELOPMENT ENVIRONMENT ===
# export DENORMALIZER_ENV=dev
# export TRINO_HOST=localhost
# export TRINO_PORT=8080
# export TRINO_USER=admin
# export TRINO_PASSWORD=password

# === PRODUCTION ENVIRONMENT ===
# export DENORMALIZER_ENV=prod
# export TRINO_HOST=your-trino-prod.company.com
# export TRINO_PORT=8080
# export TRINO_USER=your-service-account
# export TRINO_PASSWORD=$(aws secretsmanager get-secret-value --secret-id trino/prod/password --query SecretString --output text)

# === STAGING ENVIRONMENT ===
# export DENORMALIZER_ENV=staging
# export TRINO_HOST=your-trino-staging.company.com
# export TRINO_PORT=8080
# export TRINO_USER=your-service-account
# export TRINO_PASSWORD=$(aws secretsmanager get-secret-value --secret-id trino/staging/password --query SecretString --output text)

echo "[OK] Environment variables configured"
echo "   Environment: ${DENORMALIZER_ENV:-prod}"
echo "   Trino Host: ${TRINO_HOST:-not set}"
