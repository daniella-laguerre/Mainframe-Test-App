# ============================================================
# Trading Platform - Stop All
# ============================================================
# Reads pids.json from start-all.ps1 and kills each tracked
# process plus any child processes it spawned.
# ============================================================

$ROOT = "C:\trading-platform"
$PID_FILE = "$ROOT\pids.json"

if (-not (Test-Path $PID_FILE)) {
    Write-Host "No PID file found at $PID_FILE" -ForegroundColor Yellow
    Write-Host "Trying to kill known process names anyway..."
    Get-Process -Name "java","python","node","go","quote-service","market-data-sim" -ErrorAction SilentlyContinue | Stop-Process -Force
    Write-Host "Done."
    return
}

$pids = Get-Content $PID_FILE | ConvertFrom-Json

foreach ($prop in $pids.PSObject.Properties) {
    $name = $prop.Name
    $procId = $prop.Value
    try {
        $proc = Get-Process -Id $procId -ErrorAction Stop
        # Kill children first (uvicorn, go run, etc. spawn workers)
        Get-CimInstance Win32_Process -Filter "ParentProcessId = $procId" -ErrorAction SilentlyContinue |
            ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue }
        Stop-Process -Id $procId -Force
        Write-Host "  [STOPPED] $name (PID $procId)" -ForegroundColor Green
    } catch {
        Write-Host "  [GONE]    $name (PID $procId already exited)" -ForegroundColor DarkGray
    }
}

Remove-Item $PID_FILE -Force -ErrorAction SilentlyContinue
Write-Host ""
Write-Host "All tracked services stopped." -ForegroundColor Cyan
