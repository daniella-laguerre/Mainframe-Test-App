# ============================================================
# Trading Platform - Start All (background mode)
# ============================================================
# Launches every service hidden. Output goes to C:\trading-platform\logs\
# PIDs are saved so stop-all.ps1 can clean them up.
# ============================================================

$ErrorActionPreference = "Stop"
$ROOT = "C:\trading-platform"
$LOGS = "$ROOT\logs"
$PID_FILE = "$ROOT\pids.json"

if (-not (Test-Path $LOGS)) { New-Item -ItemType Directory -Path $LOGS | Out-Null }

# Service definitions: name, executable, args, working dir
$services = @(
    # --- Infrastructure ---
    @{ Name="zookeeper";       Exe="C:\kafka\bin\windows\zookeeper-server-start.bat"; Args=@("C:\kafka\config\zookeeper.properties"); Cwd="C:\kafka" }
    @{ Name="kafka";           Exe="C:\kafka\bin\windows\kafka-server-start.bat";     Args=@("C:\kafka\config\server.properties");    Cwd="C:\kafka"; WaitBefore=10 }
    @{ Name="elasticsearch";   Exe="C:\elasticsearch\bin\elasticsearch.bat";          Args=@();                                       Cwd="C:\elasticsearch" }

    # --- Go services (wait 30s for Kafka/ES to settle) ---
    @{ Name="quote-service";    Exe="go.exe"; Args=@("run","."); Cwd="$ROOT\services\quote-service";    WaitBefore=30; Env=@{ PORT="8002"; KAFKA_BROKERS="localhost:9092"; REDIS_URL="localhost:6379" } }
    @{ Name="market-data-sim";  Exe="go.exe"; Args=@("run","."); Cwd="$ROOT\services\market-data-sim"; Env=@{ KAFKA_BROKERS="localhost:9092"; REDIS_URL="localhost:6379"; TICK_INTERVAL_MS="100" } }

    # --- Python services ---
    @{ Name="order-service";   Exe="$ROOT\services\order-service\venv\Scripts\python.exe";   Args=@("-m","uvicorn","main:app","--host","0.0.0.0","--port","8001"); Cwd="$ROOT\services\order-service";   Env=@{ PORT="8001" } }
    @{ Name="risk-engine";     Exe="$ROOT\services\risk-engine\venv\Scripts\python.exe";     Args=@("-m","uvicorn","main:app","--host","0.0.0.0","--port","8004"); Cwd="$ROOT\services\risk-engine";     Env=@{ PORT="8004" } }
    @{ Name="analytics";       Exe="$ROOT\services\analytics\venv\Scripts\python.exe";       Args=@("-m","uvicorn","main:app","--host","0.0.0.0","--port","8003"); Cwd="$ROOT\services\analytics";       Env=@{ PORT="8003" } }
    @{ Name="event-processor"; Exe="$ROOT\services\event-processor\venv\Scripts\python.exe"; Args=@("main.py"); Cwd="$ROOT\services\event-processor" }
    @{ Name="legacy-adapter";  Exe="$ROOT\services\legacy-adapter\venv\Scripts\python.exe";  Args=@("main.py"); Cwd="$ROOT\services\legacy-adapter";  Env=@{ MQ_STYLE="ibm-mq" } }
    @{ Name="batch-reconciler";Exe="$ROOT\services\batch-reconciler\venv\Scripts\python.exe";Args=@("main.py"); Cwd="$ROOT\services\batch-reconciler"; Env=@{ RECONCILIATION_INTERVAL="300" } }

    # --- Gateway and UI (wait 15s for backends) ---
    @{ Name="gateway"; Exe="node.exe"; Args=@("server.js"); Cwd="$ROOT\services\gateway"; WaitBefore=15; Env=@{ PORT="3000" } }
    @{ Name="ui";      Exe="$ROOT\services\ui\node_modules\.bin\vite.cmd"; Args=@("--host","0.0.0.0","--port","8080"); Cwd="$ROOT\services\ui" }

    # --- Traffic generator (wait 10s for gateway) ---
    @{ Name="traffic-generator"; Exe="$ROOT\traffic-generator\venv\Scripts\python.exe"; Args=@("main.py"); Cwd="$ROOT\traffic-generator"; WaitBefore=10; Env=@{ TARGET_RPS="500"; BURST_MULTIPLIER="10"; CHAOS_ENABLED="true" } }
)

$pids = @{}
$startTime = Get-Date

foreach ($svc in $services) {
    $name = $svc.Name

    if ($svc.WaitBefore) {
        Write-Host "  ...waiting $($svc.WaitBefore)s before $name" -ForegroundColor DarkGray
        Start-Sleep -Seconds $svc.WaitBefore
    }

    # Apply per-service env vars
    if ($svc.Env) {
        foreach ($k in $svc.Env.Keys) {
            [System.Environment]::SetEnvironmentVariable($k, $svc.Env[$k], "Process")
        }
    }

    $outLog = "$LOGS\$name.out.log"
    $errLog = "$LOGS\$name.err.log"

    try {
        $params = @{
            FilePath               = $svc.Exe
            WorkingDirectory       = $svc.Cwd
            WindowStyle            = "Hidden"
            PassThru               = $true
            RedirectStandardOutput = $outLog
            RedirectStandardError  = $errLog
        }
        if ($svc.Args -and $svc.Args.Count -gt 0) {
            $params.ArgumentList = $svc.Args
        }
        $proc = Start-Process @params
        $pids[$name] = $proc.Id
        Write-Host "  [OK]  $name (PID $($proc.Id))" -ForegroundColor Green
    } catch {
        Write-Host "  [FAIL] $name : $_" -ForegroundColor Red
    }
}

# Save PIDs for stop-all.ps1
$pids | ConvertTo-Json | Set-Content $PID_FILE

$elapsed = [int]((Get-Date) - $startTime).TotalSeconds
Write-Host ""
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host " Started $($pids.Count) services in ${elapsed}s" -ForegroundColor Cyan
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Logs:    $LOGS"
Write-Host "PIDs:    $PID_FILE"
Write-Host ""
Write-Host "Wait ~60 seconds for everything to bind, then check status:"
Write-Host "  powershell -File $ROOT\status.ps1"
Write-Host ""
Write-Host "Tail a log:"
Write-Host "  Get-Content $LOGS\gateway.out.log -Tail 50 -Wait"
Write-Host ""
Write-Host "Stop everything:"
Write-Host "  powershell -File $ROOT\stop-all.ps1"
