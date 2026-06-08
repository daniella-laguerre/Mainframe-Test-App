# ============================================================
# Trading Platform - Windows Install Script
# ============================================================
# Installs everything needed to run the platform on Windows
# without Docker. Idempotent - safe to re-run.
#
# Usage (in PowerShell as Administrator):
#   powershell -ExecutionPolicy Bypass -File install.ps1
#
# Assumes project is at C:\trading-platform
# ============================================================

$ErrorActionPreference = "Stop"
$ProgressPreference = "SilentlyContinue"   # speeds up Invoke-WebRequest

$ROOT = "C:\trading-platform"
$PG_PASSWORD = "trading123"

function Write-Section($msg) {
    Write-Host "`n============================================================" -ForegroundColor Cyan
    Write-Host " $msg" -ForegroundColor Cyan
    Write-Host "============================================================" -ForegroundColor Cyan
}

function Refresh-Path {
    $env:Path = [System.Environment]::GetEnvironmentVariable("Path","Machine") + ";" +
                [System.Environment]::GetEnvironmentVariable("Path","User")
}

function Test-Admin {
    $current = [Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()
    return $current.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
}

# ============================================================
# Pre-flight checks
# ============================================================

if (-not (Test-Admin)) {
    Write-Host "ERROR: Run this script as Administrator." -ForegroundColor Red
    exit 1
}

if (-not (Test-Path $ROOT)) {
    Write-Host "ERROR: Project not found at $ROOT" -ForegroundColor Red
    Write-Host "Clone the repo first: git clone <YOUR_REPO> $ROOT" -ForegroundColor Yellow
    exit 1
}

# ============================================================
# Part 1: Chocolatey
# ============================================================

Write-Section "Installing Chocolatey"

if (-not (Get-Command choco -ErrorAction SilentlyContinue)) {
    Set-ExecutionPolicy Bypass -Scope Process -Force
    [System.Net.ServicePointManager]::SecurityProtocol = [System.Net.ServicePointManager]::SecurityProtocol -bor 3072
    Invoke-Expression ((New-Object System.Net.WebClient).DownloadString('https://community.chocolatey.org/install.ps1'))
    Refresh-Path
} else {
    Write-Host "Chocolatey already installed."
}

# ============================================================
# Part 2: Runtimes and tools
# ============================================================

Write-Section "Installing runtimes (Python, Go, Node, Java, Git, 7zip)"

choco install -y --no-progress python311 golang nodejs-lts openjdk17 git 7zip

Write-Section "Installing PostgreSQL 16"
choco install -y --no-progress postgresql16 --params "/Password:$PG_PASSWORD"

Write-Section "Installing Memurai (Redis-compatible)"
choco install -y --no-progress memurai-developer

Refresh-Path

# ============================================================
# Part 3: Kafka
# ============================================================

Write-Section "Installing Kafka"

if (-not (Test-Path "C:\kafka\bin\windows\kafka-server-start.bat")) {
    Push-Location C:\
    curl.exe -L -o kafka.tgz "https://archive.apache.org/dist/kafka/3.7.0/kafka_2.13-3.7.0.tgz"
    & "C:\Program Files\7-Zip\7z.exe" x kafka.tgz -y | Out-Null
    & "C:\Program Files\7-Zip\7z.exe" x kafka.tar -y | Out-Null
    if (Test-Path C:\kafka) { Remove-Item C:\kafka -Recurse -Force }
    Rename-Item kafka_2.13-3.7.0 kafka
    Remove-Item kafka.tgz, kafka.tar -ErrorAction SilentlyContinue
    Pop-Location
} else {
    Write-Host "Kafka already installed at C:\kafka"
}

# ============================================================
# Part 4: Elasticsearch
# ============================================================

Write-Section "Installing Elasticsearch"

if (-not (Test-Path "C:\elasticsearch\bin\elasticsearch.bat")) {
    Push-Location C:\
    curl.exe -L -o es.zip "https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-8.11.0-windows-x86_64.zip"
    Expand-Archive es.zip -DestinationPath . -Force
    if (Test-Path C:\elasticsearch) { Remove-Item C:\elasticsearch -Recurse -Force }
    Rename-Item elasticsearch-8.11.0 elasticsearch
    Remove-Item es.zip
    Pop-Location
} else {
    Write-Host "Elasticsearch already installed at C:\elasticsearch"
}

Write-Section "Configuring Elasticsearch"

$esYml = "C:\elasticsearch\config\elasticsearch.yml"
$esConfig = @"

# Trading platform overrides
discovery.type: single-node
xpack.security.enabled: false
cluster.routing.allocation.disk.threshold_enabled: false
"@

if (-not ((Get-Content $esYml -Raw) -match "discovery.type: single-node")) {
    Add-Content -Path $esYml -Value $esConfig
    Write-Host "Added single-node config to elasticsearch.yml"
}

$jvmOpts = "C:\elasticsearch\config\jvm.options"
if (-not ((Get-Content $jvmOpts -Raw) -match "-Xms512m")) {
    Add-Content -Path $jvmOpts -Value "`n-Xms512m`n-Xmx512m"
    Write-Host "Set JVM heap to 512MB"
}

# ============================================================
# Part 5: PostgreSQL schema
# ============================================================

Write-Section "Initializing PostgreSQL schema"

$env:PGPASSWORD = $PG_PASSWORD
$psqlExists = Get-Command psql -ErrorAction SilentlyContinue
if (-not $psqlExists) {
    $env:Path += ";C:\Program Files\PostgreSQL\16\bin"
}

# Wait for PG service to be ready
Start-Service "postgresql-x64-16" -ErrorAction SilentlyContinue
$retries = 0
while ($retries -lt 30) {
    $check = & psql -U postgres -c "SELECT 1" 2>&1
    if ($LASTEXITCODE -eq 0) { break }
    Start-Sleep -Seconds 2
    $retries++
}

# Create user and DB (ignore errors if they already exist)
$savedPref = $ErrorActionPreference
$ErrorActionPreference = "Continue"

$userExists = (& psql -U postgres -tAc "SELECT 1 FROM pg_roles WHERE rolname='trading'" 2>$null).Trim()
if ($userExists -ne "1") {
    & psql -U postgres -c "CREATE USER trading WITH PASSWORD '$PG_PASSWORD' SUPERUSER;" 2>&1 | Out-Null
    Write-Host "Created user 'trading'"
} else {
    Write-Host "User 'trading' already exists - skipping"
}

$dbExists = (& psql -U postgres -tAc "SELECT 1 FROM pg_database WHERE datname='trading'" 2>$null).Trim()
if ($dbExists -ne "1") {
    & psql -U postgres -c "CREATE DATABASE trading OWNER trading;" 2>&1 | Out-Null
    Write-Host "Created database 'trading'"
} else {
    Write-Host "Database 'trading' already exists - skipping"
}

# Load schema (only if tables don't already exist)
$initSql = "$ROOT\configs\postgres\init.sql"
if (Test-Path $initSql) {
    $tableExists = (& psql -U trading -d trading -tAc "SELECT 1 FROM information_schema.tables WHERE table_name='instruments'" 2>$null).Trim()
    if ($tableExists -ne "1") {
        & psql -U trading -d trading -f $initSql 2>&1 | Out-Host
        Write-Host "Schema loaded from init.sql"
    } else {
        Write-Host "Schema already loaded - skipping"
    }
} else {
    Write-Host "WARNING: $initSql not found - skipping schema load" -ForegroundColor Yellow
}

$ErrorActionPreference = $savedPref

# ============================================================
# Part 6: System environment variables
# ============================================================

Write-Section "Setting system environment variables"

$envVars = @{
    "DATABASE_URL"          = "postgresql://trading:trading123@localhost:5432/trading"
    "KAFKA_BROKERS"         = "localhost:9092"
    "REDIS_URL"             = "redis://localhost:6379"
    "ELASTICSEARCH_URL"     = "http://localhost:9200"
    "ORDER_SERVICE_URL"     = "http://localhost:8001"
    "QUOTE_SERVICE_URL"     = "http://localhost:8002"
    "ANALYTICS_SERVICE_URL" = "http://localhost:8003"
    "RISK_ENGINE_URL"       = "http://localhost:8004"
    "GATEWAY_URL"           = "http://localhost:3000"
}

foreach ($k in $envVars.Keys) {
    [System.Environment]::SetEnvironmentVariable($k, $envVars[$k], "Machine")
    Write-Host "  $k = $($envVars[$k])"
}

# ============================================================
# Part 7: Python venvs + deps
# ============================================================

Write-Section "Setting up Python services"

$pythonServices = @(
    "services\order-service",
    "services\risk-engine",
    "services\analytics",
    "services\event-processor",
    "services\legacy-adapter",
    "services\batch-reconciler",
    "traffic-generator"
)

foreach ($svc in $pythonServices) {
    $svcPath = Join-Path $ROOT $svc
    if (-not (Test-Path $svcPath)) {
        Write-Host "  SKIP: $svc (not found)" -ForegroundColor Yellow
        continue
    }
    $venv = Join-Path $svcPath "venv"
    Write-Host "`n  -> $svc"
    if (-not (Test-Path $venv)) {
        Push-Location $svcPath
        python -m venv venv
        Pop-Location
    }
    if (Test-Path (Join-Path $svcPath "requirements.txt")) {
        & "$venv\Scripts\python.exe" -m pip install --upgrade pip --quiet
        & "$venv\Scripts\python.exe" -m pip install -r (Join-Path $svcPath "requirements.txt") --quiet
    }
}

# ============================================================
# Part 8: Node.js deps
# ============================================================

Write-Section "Installing Node.js dependencies"

foreach ($svc in @("services\gateway", "services\ui")) {
    $svcPath = Join-Path $ROOT $svc
    if (-not (Test-Path $svcPath)) {
        Write-Host "  SKIP: $svc (not found)" -ForegroundColor Yellow
        continue
    }
    Write-Host "`n  -> $svc"
    Push-Location $svcPath
    npm install --silent
    Pop-Location
}

# ============================================================
# Part 9: Go deps (pre-fetch modules)
# ============================================================

Write-Section "Pre-fetching Go modules"

foreach ($svc in @("services\quote-service", "services\market-data-sim")) {
    $svcPath = Join-Path $ROOT $svc
    if (-not (Test-Path $svcPath)) {
        Write-Host "  SKIP: $svc (not found)" -ForegroundColor Yellow
        continue
    }
    Write-Host "`n  -> $svc"
    Push-Location $svcPath
    go mod download
    Pop-Location
}

# ============================================================
# Part 10: Create start-all.ps1
# ============================================================

Write-Section "Writing start-all.ps1"

$startScript = @'
$root = "C:\trading-platform"

Start-Process powershell -ArgumentList "-NoExit","-Command","cd C:\kafka; .\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties"
Start-Sleep -Seconds 10
Start-Process powershell -ArgumentList "-NoExit","-Command","cd C:\kafka; .\bin\windows\kafka-server-start.bat .\config\server.properties"
Start-Process powershell -ArgumentList "-NoExit","-Command","cd C:\elasticsearch; .\bin\elasticsearch.bat"
Start-Sleep -Seconds 30

Start-Process powershell -ArgumentList "-NoExit","-Command","cd $root\services\quote-service; `$env:PORT='8002'; `$env:KAFKA_BROKERS='localhost:9092'; `$env:REDIS_URL='localhost:6379'; go run ."
Start-Process powershell -ArgumentList "-NoExit","-Command","cd $root\services\market-data-sim; `$env:KAFKA_BROKERS='localhost:9092'; `$env:REDIS_URL='localhost:6379'; `$env:TICK_INTERVAL_MS='100'; go run ."

Start-Process powershell -ArgumentList "-NoExit","-Command","cd $root\services\order-service; .\venv\Scripts\Activate.ps1; `$env:PORT='8001'; uvicorn main:app --host 0.0.0.0 --port 8001"
Start-Process powershell -ArgumentList "-NoExit","-Command","cd $root\services\risk-engine; .\venv\Scripts\Activate.ps1; `$env:PORT='8004'; uvicorn main:app --host 0.0.0.0 --port 8004"
Start-Process powershell -ArgumentList "-NoExit","-Command","cd $root\services\analytics; .\venv\Scripts\Activate.ps1; `$env:PORT='8003'; uvicorn main:app --host 0.0.0.0 --port 8003"
Start-Process powershell -ArgumentList "-NoExit","-Command","cd $root\services\event-processor; .\venv\Scripts\Activate.ps1; python main.py"
Start-Process powershell -ArgumentList "-NoExit","-Command","cd $root\services\legacy-adapter; .\venv\Scripts\Activate.ps1; `$env:MQ_STYLE='ibm-mq'; python main.py"
Start-Process powershell -ArgumentList "-NoExit","-Command","cd $root\services\batch-reconciler; .\venv\Scripts\Activate.ps1; `$env:RECONCILIATION_INTERVAL='300'; python main.py"

Start-Sleep -Seconds 15
Start-Process powershell -ArgumentList "-NoExit","-Command","cd $root\services\gateway; `$env:PORT='3000'; node server.js"
Start-Process powershell -ArgumentList "-NoExit","-Command","cd $root\services\ui; npm run dev -- --host 0.0.0.0 --port 8080"

Start-Sleep -Seconds 10
Start-Process powershell -ArgumentList "-NoExit","-Command","cd $root\traffic-generator; .\venv\Scripts\Activate.ps1; `$env:TARGET_RPS='500'; `$env:BURST_MULTIPLIER='10'; `$env:CHAOS_ENABLED='true'; python main.py"
'@

Set-Content -Path "$ROOT\start-all.ps1" -Value $startScript -Encoding UTF8
Write-Host "Created $ROOT\start-all.ps1"

# ============================================================
# Done
# ============================================================

Write-Section "Install complete"

Write-Host @"

Next steps:
  1. Close and reopen PowerShell (so env vars apply)
  2. Run: powershell -ExecutionPolicy Bypass -File $ROOT\start-all.ps1
  3. Wait ~90 seconds for everything to boot
  4. Open http://localhost:8080 in a browser

To check if services are up:
  netstat -an | findstr "LISTENING" | findstr "2181 5432 6379 9092 9200 3000 8001 8002 8003 8004 8080"

"@ -ForegroundColor Green
