# ============================================================
# Trading Platform - IIS Setup
# ============================================================
# Installs IIS + URL Rewrite + ARR, builds the UI, creates an
# IIS site, and configures HTTPS with a self-signed cert.
#
# Architecture:
#   IIS :80/:443  -->  serves React UI from C:\trading-platform\services\ui\dist
#                 -->  proxies /api/* and /ws to gateway on localhost:3000
#                 -->  /health, /metrics also proxy to gateway
#
# Usage (PowerShell as Administrator):
#   powershell -ExecutionPolicy Bypass -File install-iis.ps1
# ============================================================

$ErrorActionPreference = "Stop"
$ProgressPreference = "SilentlyContinue"

$ROOT     = "C:\trading-platform"
$UI_DIR   = "$ROOT\services\ui"
$DIST_DIR = "$UI_DIR\dist"
$SITE     = "trading-platform"
$HOSTNAME = "legacy-trade-host"

function Write-Section($msg) {
    Write-Host "`n============================================================" -ForegroundColor Cyan
    Write-Host " $msg" -ForegroundColor Cyan
    Write-Host "============================================================" -ForegroundColor Cyan
}

function Test-Admin {
    $current = [Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()
    return $current.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
}

if (-not (Test-Admin)) {
    Write-Host "ERROR: Run this script as Administrator." -ForegroundColor Red
    exit 1
}

# ============================================================
# Part 1: Install IIS + features
# ============================================================

Write-Section "Installing IIS and required features"

$features = @(
    "Web-Server",
    "Web-Common-Http",
    "Web-Static-Content",
    "Web-Default-Doc",
    "Web-Http-Errors",
    "Web-Http-Redirect",
    "Web-Http-Logging",
    "Web-Stat-Compression",
    "Web-Filtering",
    "Web-WebSockets",
    "Web-Mgmt-Console",
    "Web-Scripting-Tools"
)

foreach ($f in $features) {
    $state = (Get-WindowsFeature -Name $f).InstallState
    if ($state -ne "Installed") {
        Install-WindowsFeature -Name $f | Out-Null
        Write-Host "  Installed: $f"
    } else {
        Write-Host "  Already installed: $f"
    }
}

# ============================================================
# Part 2: Install URL Rewrite + ARR
# ============================================================

Write-Section "Installing URL Rewrite and Application Request Routing"

if (-not (Get-Command choco -ErrorAction SilentlyContinue)) {
    Write-Host "ERROR: Chocolatey not found. Run install.ps1 first." -ForegroundColor Red
    exit 1
}

choco install -y --no-progress urlrewrite
choco install -y --no-progress iis-arr

# Enable ARR proxy at server level
Import-Module WebAdministration
Set-WebConfigurationProperty -Filter 'system.webServer/proxy' -PSPath 'MACHINE/WEBROOT/APPHOST' -Name 'enabled' -Value 'True'
Write-Host "  ARR proxy enabled at server level"

# ============================================================
# Part 3: Build the UI
# ============================================================

Write-Section "Building the React UI"

if (-not (Test-Path "$UI_DIR\node_modules")) {
    Push-Location $UI_DIR
    npm install --silent
    Pop-Location
}

Push-Location $UI_DIR
npm run build
Pop-Location

if (-not (Test-Path "$DIST_DIR\index.html")) {
    Write-Host "ERROR: UI build did not produce $DIST_DIR\index.html" -ForegroundColor Red
    exit 1
}

# Make sure web.config is in dist/ (Vite doesn't copy it by default)
Copy-Item "$UI_DIR\web.config" "$DIST_DIR\web.config" -Force
Write-Host "  Copied web.config to $DIST_DIR"

# ============================================================
# Part 4: Configure IIS site
# ============================================================

Write-Section "Configuring IIS site"

# Remove default site if present
if (Get-Website -Name "Default Web Site" -ErrorAction SilentlyContinue) {
    Stop-Website -Name "Default Web Site" -ErrorAction SilentlyContinue
    Remove-Website -Name "Default Web Site"
    Write-Host "  Removed Default Web Site"
}

# Remove our site if it exists, then recreate
if (Get-Website -Name $SITE -ErrorAction SilentlyContinue) {
    Stop-Website -Name $SITE -ErrorAction SilentlyContinue
    Remove-Website -Name $SITE
    Write-Host "  Removed existing $SITE site"
}

# App pool
if (-not (Get-IISAppPool -Name $SITE -ErrorAction SilentlyContinue)) {
    New-WebAppPool -Name $SITE | Out-Null
    Set-ItemProperty "IIS:\AppPools\$SITE" -Name managedRuntimeVersion -Value ""
    Set-ItemProperty "IIS:\AppPools\$SITE" -Name processModel.identityType -Value ApplicationPoolIdentity
}

# Site (HTTP on :80)
New-Website -Name $SITE -PhysicalPath $DIST_DIR -ApplicationPool $SITE -Port 80 -Force | Out-Null
Write-Host "  Created site '$SITE' on port 80 -> $DIST_DIR"

# Grant the app pool read access to dist/
$acl = Get-Acl $DIST_DIR
$rule = New-Object System.Security.AccessControl.FileSystemAccessRule(
    "IIS AppPool\$SITE",
    "ReadAndExecute",
    "ContainerInherit,ObjectInherit",
    "None",
    "Allow"
)
$acl.SetAccessRule($rule)
Set-Acl $DIST_DIR $acl
Write-Host "  Granted IIS AppPool\$SITE read access to $DIST_DIR"

# ============================================================
# Part 5: HTTPS with self-signed cert
# ============================================================

Write-Section "Configuring HTTPS with self-signed cert"

$cert = Get-ChildItem Cert:\LocalMachine\My | Where-Object { $_.Subject -eq "CN=$HOSTNAME" } | Select-Object -First 1
if (-not $cert) {
    $cert = New-SelfSignedCertificate `
        -DnsName $HOSTNAME, "localhost", "127.0.0.1" `
        -CertStoreLocation Cert:\LocalMachine\My `
        -NotAfter (Get-Date).AddYears(3) `
        -KeyAlgorithm RSA `
        -KeyLength 2048
    Write-Host "  Created self-signed cert: $($cert.Thumbprint)"
} else {
    Write-Host "  Reusing existing cert: $($cert.Thumbprint)"
}

# Bind HTTPS on :443
if (-not (Get-WebBinding -Name $SITE -Protocol https -ErrorAction SilentlyContinue)) {
    New-WebBinding -Name $SITE -Protocol https -Port 443 -IPAddress "*" | Out-Null
    $binding = Get-WebBinding -Name $SITE -Protocol https
    $binding.AddSslCertificate($cert.Thumbprint, "My")
    Write-Host "  HTTPS binding added on port 443"
} else {
    Write-Host "  HTTPS binding already present"
}

# ============================================================
# Part 6: Windows Firewall
# ============================================================

Write-Section "Opening Windows Firewall for 80/443"

foreach ($port in @(80, 443)) {
    $name = "IIS-Port-$port"
    if (-not (Get-NetFirewallRule -DisplayName $name -ErrorAction SilentlyContinue)) {
        New-NetFirewallRule -DisplayName $name -Direction Inbound -LocalPort $port -Protocol TCP -Action Allow | Out-Null
        Write-Host "  Opened port $port"
    } else {
        Write-Host "  Port $port already open"
    }
}

# ============================================================
# Part 7: Start the site
# ============================================================

Write-Section "Starting the site"

Start-Website -Name $SITE
Write-Host "  $SITE started"

# ============================================================
# Done
# ============================================================

Write-Section "IIS setup complete"

Write-Host @"

Site configured:
  Name:     $SITE
  Path:     $DIST_DIR
  Bindings: http://*:80, https://*:443

Routing:
  /                  -> static UI files (with SPA fallback)
  /api/*             -> http://localhost:3000 (gateway)
  /ws                -> http://localhost:3000 (WebSocket)
  /health, /metrics  -> http://localhost:3000

Test from the VM:
  curl.exe http://localhost/
  curl.exe http://localhost/api/v1/quotes/AAPL
  curl.exe http://localhost/health

Test from outside (after opening :80 and :443 in GCE firewall):
  http://<VM_EXTERNAL_IP>/
  https://<VM_EXTERNAL_IP>/    (browser will warn about self-signed cert)

Don't forget:
  1. Open ports 80/443 in your GCE firewall:
     gcloud compute firewall-rules update allow-trading-platform \``
       --rules=...,tcp:80,tcp:443
  2. Make sure the gateway is running on port 3000 (start-all.ps1)

Logs:
  IIS access logs:   C:\inetpub\logs\LogFiles\
  Gateway logs:      $ROOT\logs\gateway.out.log

"@ -ForegroundColor Green
