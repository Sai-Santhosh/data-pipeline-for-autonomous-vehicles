# Fleet Data Pipeline — Quickstart (Windows PowerShell)
# Run from repo root: .\scripts\quickstart.ps1
# 1) Starts Docker services  2) Creates Kafka topics  3) Runs verify  4) Prints next steps

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
if (-not (Test-Path (Join-Path $root "docker-compose.yml"))) {
    Write-Host "Run this script from repo root. Not found: $root\docker-compose.yml"
    exit 1
}
Set-Location $root

Write-Host "=== Fleet Data Pipeline — Quickstart ===" -ForegroundColor Cyan
Write-Host ""

# 1. Docker
Write-Host "1. Starting Docker (Kafka + TimescaleDB)..." -ForegroundColor Yellow
docker-compose up -d
if ($LASTEXITCODE -ne 0) { Write-Host "Docker failed."; exit 1 }
Write-Host "   Waiting 15s for services to be ready..."
Start-Sleep -Seconds 15

# 2. Create topics (retry once)
Write-Host ""
Write-Host "2. Creating Kafka topics..." -ForegroundColor Yellow
$topicScript = Join-Path $root "scripts\create_topics.ps1"
& $topicScript
if ($LASTEXITCODE -ne 0) {
    Write-Host "   Retrying in 5s..."
    Start-Sleep -Seconds 5
    & $topicScript
}

# 3. Verify
Write-Host ""
Write-Host "3. Running verify.py..." -ForegroundColor Yellow
python (Join-Path $root "scripts\verify.py")
$verifyExit = $LASTEXITCODE

Write-Host ""
Write-Host "=== Next steps ===" -ForegroundColor Green
Write-Host "Open 3 terminals and run (from repo root):"
Write-Host "  Terminal 1: python scripts\run_consumer.py"
Write-Host "  Terminal 2: python scripts\run_producer.py live"
Write-Host "  Terminal 3: python scripts\run_dashboard.py"
Write-Host "Then open http://localhost:8501"
Write-Host ""
exit $verifyExit
