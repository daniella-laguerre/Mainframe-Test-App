# ============================================================
# Trading Platform - Status
# ============================================================
# Shows which services are listening, which processes are alive,
# and the last few lines of each log.
# ============================================================

$ROOT = "C:\trading-platform"
$LOGS = "$ROOT\logs"

$expectedPorts = @{
    2181 = "Zookeeper"
    9092 = "Kafka"
    9200 = "Elasticsearch"
    5432 = "PostgreSQL"
    6379 = "Redis"
    8002 = "Quote Service"
    8001 = "Order Service"
    8003 = "Analytics"
    8004 = "Risk Engine"
    3000 = "Gateway"
    8080 = "UI"
}

Write-Host "`n=== PORT STATUS ===" -ForegroundColor Cyan
$listening = (netstat -an | Select-String "LISTENING").Line
foreach ($port in ($expectedPorts.Keys | Sort-Object)) {
    $svcName = $expectedPorts[$port]
    if ($listening -match ":$port\s") {
        Write-Host ("  [UP]   {0,-5} {1}" -f $port, $svcName) -ForegroundColor Green
    } else {
        Write-Host ("  [DOWN] {0,-5} {1}" -f $port, $svcName) -ForegroundColor Red
    }
}

Write-Host "`n=== TRACKED PROCESSES ===" -ForegroundColor Cyan
if (Test-Path "$ROOT\pids.json") {
    $pids = Get-Content "$ROOT\pids.json" | ConvertFrom-Json
    foreach ($prop in $pids.PSObject.Properties) {
        $proc = Get-Process -Id $prop.Value -ErrorAction SilentlyContinue
        if ($proc) {
            $cpu = [math]::Round($proc.CPU, 1)
            $mem = [math]::Round($proc.WorkingSet64 / 1MB, 0)
            Write-Host ("  [ALIVE] {0,-20} PID={1,-6} CPU={2,6}s  MEM={3,4}MB" -f $prop.Name, $prop.Value, $cpu, $mem) -ForegroundColor Green
        } else {
            Write-Host ("  [DEAD]  {0,-20} PID={1} (check $LOGS\{0}.err.log)" -f $prop.Name, $prop.Value) -ForegroundColor Red
        }
    }
} else {
    Write-Host "  No pids.json - has start-all.ps1 been run?" -ForegroundColor Yellow
}

Write-Host "`n=== LATEST ERRORS ===" -ForegroundColor Cyan
Get-ChildItem "$LOGS\*.err.log" -ErrorAction SilentlyContinue | ForEach-Object {
    $errs = Get-Content $_.FullName -Tail 3 -ErrorAction SilentlyContinue
    if ($errs) {
        Write-Host "`n  $($_.BaseName.Replace('.err',''))" -ForegroundColor Yellow
        $errs | ForEach-Object { Write-Host "    $_" -ForegroundColor DarkGray }
    }
}

Write-Host ""
