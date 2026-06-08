# ============================================================
# Trading Platform - Daily Restart
# ============================================================
# Run this any time you want a clean start after the initial
# install.ps1 has been run once.
#
# What it does:
#   1. Stops anything currently running
#   2. Applies known fixes (idempotent):
#      - aiokafka batch_size -> max_batch_size in legacy-adapter
#      - elasticsearch client pinned to 8.x
#   3. Starts all services
#   4. Waits 90s for ports to bind
#   5. Shows port + process status
#   6. Tails traffic-generator activity
#
# Usage:
#   powershell -ExecutionPolicy Bypass -File C:\trading-platform\restart.ps1
#
# Add -NoTail to skip the live log tail at the end.
# ============================================================

param(
    [switch]$NoTail
)

$ErrorActionPreference = "Continue"
$ProgressPreference    = "SilentlyContinue"
$ROOT                  = "C:\trading-platform"

function Write-Section($msg) {
    Write-Host "`n============================================================" -ForegroundColor Cyan
    Write-Host " $msg" -ForegroundColor Cyan
    Write-Host "============================================================" -ForegroundColor Cyan
}

# ============================================================
# 1. Stop anything running
# ============================================================
Write-Section "Stopping any running services"

if (Test-Path "$ROOT\stop-all.ps1") {
    & powershell -ExecutionPolicy Bypass -File "$ROOT\stop-all.ps1"
} else {
    Get-Process java, python, node, go, quote-service, market-data-sim -ErrorAction SilentlyContinue | Stop-Process -Force
}
Start-Sleep -Seconds 5

# Make sure infrastructure Windows services are running
foreach ($svc in @("postgresql-x64-16","Memurai")) {
    if (Get-Service $svc -ErrorAction SilentlyContinue) {
        Start-Service $svc -ErrorAction SilentlyContinue
    }
}

# ============================================================
# 2. Apply known code/dependency fixes (idempotent)
# ============================================================
Write-Section "Applying known fixes"

# Fix: legacy-adapter must use max_batch_size (newer aiokafka rename)
$adapter = "$ROOT\services\legacy-adapter\main.py"
if (Test-Path $adapter) {
    $content = Get-Content $adapter -Raw
    if ($content -match "(?<!max_)batch_size=") {
        $content = $content -replace "(?<!max_)batch_size=", "max_batch_size="
        Set-Content -Path $adapter -Value $content -NoNewline
        Write-Host "  [FIX]  legacy-adapter: batch_size -> max_batch_size" -ForegroundColor Yellow
    } else {
        Write-Host "  [OK]   legacy-adapter: batch_size already correct" -ForegroundColor Green
    }
}

# Fix: pin elasticsearch python client to <9 to match ES 8.11 server
foreach ($svc in @("analytics","event-processor","batch-reconciler")) {
    $venv = "$ROOT\services\$svc\venv\Scripts\python.exe"
    if (-not (Test-Path $venv)) { continue }

    $version = & $venv -c "import elasticsearch; print(elasticsearch.__version__[0])" 2>$null
    if ($version -and $version -ge 9) {
        Write-Host "  [FIX]  $svc`: downgrading elasticsearch client to 8.x..." -ForegroundColor Yellow
        & $venv -m pip uninstall elasticsearch -y --quiet | Out-Null
        & $venv -m pip install "elasticsearch>=8.11,<9.0" --quiet | Out-Null
        Write-Host "  [OK]   $svc`: elasticsearch pinned" -ForegroundColor Green
    } elseif ($version) {
        Write-Host "  [OK]   $svc`: elasticsearch v$version (compatible)" -ForegroundColor Green
    }
}

# Verify all Python venvs exist (warn if any missing - don't auto-recreate, that's install.ps1's job)
$missingVenvs = @()
foreach ($svc in @("services\order-service","services\risk-engine","services\analytics","services\event-processor","services\legacy-adapter","services\batch-reconciler","traffic-generator")) {
    if (-not (Test-Path "$ROOT\$svc\venv\Scripts\python.exe")) {
        $missingVenvs += $svc
    }
}
if ($missingVenvs.Count -gt 0) {
    Write-Host "`n  [WARN] Missing venvs detected:" -ForegroundColor Red
    $missingVenvs | ForEach-Object { Write-Host "         - $_" -ForegroundColor Red }
    Write-Host "         Run install.ps1 first to create them.`n" -ForegroundColor Red
}

# Make sure Kafka log dir is writable (avoids log4j rename errors)
if (Test-Path "C:\kafka\logs") {
    Get-ChildItem "C:\kafka\logs\*.log" -ErrorAction SilentlyContinue |
        Where-Object { $_.LastWriteTime -lt (Get-Date).AddDays(-1) } |
        Remove-Item -Force -ErrorAction SilentlyContinue
}

# ============================================================
# 3. Start all services
# ============================================================
Write-Section "Starting services (this takes ~90s)"

if (-not (Test-Path "$ROOT\start-all.ps1")) {
    Write-Host "ERROR: $ROOT\start-all.ps1 not found. Run install.ps1 first." -ForegroundColor Red
    exit 1
}
& powershell -ExecutionPolicy Bypass -File "$ROOT\start-all.ps1"

# ============================================================
# 4. Wait for ports to bind
# ============================================================
Write-Section "Waiting for services to bind their ports"

$expectedPorts = @{
    2181 = "Zookeeper";    9092 = "Kafka";       9200 = "Elasticsearch"
    5432 = "PostgreSQL";   6379 = "Redis";       3000 = "Gateway"
    8001 = "Order";        8002 = "Quote";       8003 = "Analytics"
    8004 = "Risk";         8080 = "UI"
}

$deadline = (Get-Date).AddSeconds(120)
$lastReport = Get-Date
while ((Get-Date) -lt $deadline) {
    $listening = (netstat -an | Select-String "LISTENING").Line
    $up = 0
    foreach ($p in $expectedPorts.Keys) {
        if ($listening -match ":$p\s") { $up++ }
    }
    if ($up -eq $expectedPorts.Count) {
        Write-Host "  All $up ports listening." -ForegroundColor Green
        break
    }
    if (((Get-Date) - $lastReport).TotalSeconds -ge 10) {
        Write-Host "  $up / $($expectedPorts.Count) ports up..." -ForegroundColor DarkGray
        $lastReport = Get-Date
    }
    Start-Sleep -Seconds 2
}

# ============================================================
# 5. Show final status
# ============================================================
Write-Section "Status"

if (Test-Path "$ROOT\status.ps1") {
    & powershell -ExecutionPolicy Bypass -File "$ROOT\status.ps1"
} else {
    Write-Host "status.ps1 missing, dumping listening ports:"
    netstat -an | Select-String "LISTENING" | Select-String "2181|9092|9200|5432|6379|3000|8001|8002|8003|8004|8080"
}

# ============================================================
# 6. Tail traffic
# ============================================================
if (-not $NoTail) {
    Write-Section "Tailing traffic-generator (Ctrl+C to stop)"

    $tgLog = "$ROOT\logs\traffic-generator.err.log"
    if (Test-Path $tgLog) {
        Get-Content $tgLog -Tail 5 -Wait
    } else {
        Write-Host "traffic-generator log not yet present at $tgLog" -ForegroundColor Yellow
        Write-Host "Wait a few seconds and run:" -ForegroundColor Yellow
        Write-Host "  Get-Content $tgLog -Tail 20 -Wait" -ForegroundColor Yellow
    }
} else {
    Write-Host "`nDone. To watch live traffic:" -ForegroundColor Green
    Write-Host "  Get-Content $ROOT\logs\traffic-generator.err.log -Tail 20 -Wait" -ForegroundColor Green
}
