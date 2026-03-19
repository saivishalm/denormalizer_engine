# Windows PowerShell Environment Setup
# Run: . .\setup_env.ps1

# Option 1: Load from .env file
if (Test-Path .env) {
    Write-Host "Loading environment variables from .env file..."
    Get-Content .env | ForEach-Object {
        if ($_ -match '^\s*([^=]+)\s*=\s*(.+)\s*$') {
            $varName = $matches[1].Trim()
            $varValue = $matches[2].Trim()
            [Environment]::SetEnvironmentVariable($varName, $varValue, "Process")
        }
    }
}

# Option 2: Set manually
# === PRODUCTION ENVIRONMENT ===
[Environment]::SetEnvironmentVariable("DENORMALIZER_ENV", "prod", "Process")
[Environment]::SetEnvironmentVariable("TRINO_HOST", "trino.de-eks-nonprod.bu.edu", "Process")
[Environment]::SetEnvironmentVariable("TRINO_PORT", "443", "Process")
[Environment]::SetEnvironmentVariable("TRINO_USER", "dataeng_svmiriya", "Process")
[Environment]::SetEnvironmentVariable("TRINO_PASSWORD", "8fd0399d333bf9dc91b4eeb840ba6ab4ef9c1d7dd327549c", "Process")

# === DEVELOPMENT ENVIRONMENT (uncomment to use) ===
# [Environment]::SetEnvironmentVariable("DENORMALIZER_ENV", "dev", "Process")
# [Environment]::SetEnvironmentVariable("TRINO_HOST", "localhost", "Process")
# [Environment]::SetEnvironmentVariable("TRINO_PORT", "8080", "Process")
# [Environment]::SetEnvironmentVariable("TRINO_USER", "admin", "Process")
# [Environment]::SetEnvironmentVariable("TRINO_PASSWORD", "password", "Process")

Write-Host "Environment variables configured" -ForegroundColor Green
Write-Host "   Environment: $($env:DENORMALIZER_ENV)" -ForegroundColor Cyan
Write-Host "   Trino Host: $($env:TRINO_HOST)" -ForegroundColor Cyan
Write-Host "Ready to run: python run_production.py" -ForegroundColor Yellow
