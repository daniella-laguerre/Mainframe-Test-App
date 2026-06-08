# Running the Trading Platform on a Windows GCE VM

Step-by-step instructions for running the platform natively on Windows Server 2022 (no Docker).

---

## Part 1: RDP Into the VM

1. Open your local Remote Desktop client (Windows: `mstsc`, Mac: Microsoft Remote Desktop)
2. Connect to the VM's **external IP** (from `gcloud compute instances list`)
3. Username/password from `gcloud compute reset-windows-password`
4. Once inside, open **PowerShell as Administrator** for everything below

---

## Part 2: Install Chocolatey (Package Manager)

This makes installing everything else much easier.

```powershell
Set-ExecutionPolicy Bypass -Scope Process -Force
[System.Net.ServicePointManager]::SecurityProtocol = [System.Net.ServicePointManager]::SecurityProtocol -bor 3072
iex ((New-Object System.Net.WebClient).DownloadString('https://community.chocolatey.org/install.ps1'))
```

Close PowerShell and reopen it as Administrator so the `choco` command is on your PATH.

---

## Part 3: Install Runtimes and Dependencies

Run these in PowerShell (Admin):

```powershell
# Languages
choco install -y python311
choco install -y golang
choco install -y nodejs-lts
choco install -y git

# Databases and infrastructure
choco install -y postgresql16 --params "/Password:trading123"
choco install -y memurai-developer   # Redis-compatible for Windows

# Utilities
choco install -y 7zip
choco install -y openjdk17           # Kafka + Elasticsearch need Java
```

Close and reopen PowerShell so the new tools are on your PATH. Verify:

```powershell
python --version
go version
node --version
java -version
psql --version
```

---

## Part 4: Install Kafka + Zookeeper

Kafka doesn't have a Chocolatey package, so install manually.

```powershell
cd C:\
curl.exe -L -o kafka.tgz "https://archive.apache.org/dist/kafka/3.7.0/kafka_2.13-3.7.0.tgz"
7z x kafka.tgz
7z x kafka.tar
Rename-Item kafka_2.13-3.7.0 kafka
Remove-Item kafka.tgz, kafka.tar
```

Kafka is now at `C:\kafka`.

---

## Part 5: Install Elasticsearch

```powershell
cd C:\
curl.exe -L -o es.zip "https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-8.11.0-windows-x86_64.zip"
Expand-Archive es.zip -DestinationPath .
Rename-Item elasticsearch-8.11.0 elasticsearch
Remove-Item es.zip
```

Disable security and set single-node mode:

```powershell
notepad C:\elasticsearch\config\elasticsearch.yml
```

Add these lines:
```yaml
discovery.type: single-node
xpack.security.enabled: false
cluster.routing.allocation.disk.threshold_enabled: false
```

Set the JVM heap:

```powershell
notepad C:\elasticsearch\config\jvm.options
```

Add:
```
-Xms512m
-Xmx512m
```

---

## Part 6: Clone the Project

```powershell
cd C:\
git clone <YOUR_REPO_URL> trading-platform
cd trading-platform
```

If you don't have a Git remote, transfer the code via:
- `gcloud compute scp --recurse C:\path\to\trading-platform VM_NAME:C:\ --zone=us-central1-a`
- Or zip it, upload to a Cloud Storage bucket, and download on the VM

---

## Part 7: Initialize PostgreSQL Schema

```powershell
# Set the postgres password env var so psql doesn't prompt
$env:PGPASSWORD = "trading123"

# Create the database and user
psql -U postgres -c "CREATE USER trading WITH PASSWORD 'trading123' SUPERUSER;"
psql -U postgres -c "CREATE DATABASE trading OWNER trading;"

# Load the schema
psql -U trading -d trading -f C:\trading-platform\configs\postgres\init.sql
```

---

## Part 8: Start the Infrastructure (5 PowerShell windows)

Open **a new PowerShell window for each** of the following. Leave them running.

### Window 1 — Zookeeper
```powershell
cd C:\kafka
.\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties
```

### Window 2 — Kafka
Wait ~10 seconds for Zookeeper to be ready, then:
```powershell
cd C:\kafka
.\bin\windows\kafka-server-start.bat .\config\server.properties
```

### Window 3 — Redis (Memurai)
Memurai installs as a Windows service and auto-starts. Verify:
```powershell
memurai-cli ping
# Should return: PONG
```

### Window 4 — Elasticsearch
```powershell
cd C:\elasticsearch
.\bin\elasticsearch.bat
```
Wait for `"started"` in the output, then verify in another window:
```powershell
curl http://localhost:9200
```

### Window 5 — PostgreSQL
PostgreSQL runs as a Windows service. Verify:
```powershell
Get-Service postgresql*
# Should show "Running"
```

---

## Part 9: Set Environment Variables

Open **PowerShell as Administrator** and set these system-wide (persist across reboots):

```powershell
# Run this script to set all the shared env vars
[System.Environment]::SetEnvironmentVariable("DATABASE_URL", "postgresql://trading:trading123@localhost:5432/trading", "Machine")
[System.Environment]::SetEnvironmentVariable("KAFKA_BROKERS", "localhost:9092", "Machine")
[System.Environment]::SetEnvironmentVariable("REDIS_URL", "redis://localhost:6379", "Machine")
[System.Environment]::SetEnvironmentVariable("ELASTICSEARCH_URL", "http://localhost:9200", "Machine")
[System.Environment]::SetEnvironmentVariable("ORDER_SERVICE_URL", "http://localhost:8001", "Machine")
[System.Environment]::SetEnvironmentVariable("QUOTE_SERVICE_URL", "http://localhost:8002", "Machine")
[System.Environment]::SetEnvironmentVariable("ANALYTICS_SERVICE_URL", "http://localhost:8003", "Machine")
[System.Environment]::SetEnvironmentVariable("RISK_ENGINE_URL", "http://localhost:8004", "Machine")
```

Close and reopen all PowerShell windows so they pick up the new env vars.

Reference: the full list is in [env-windows-local.txt](env-windows-local.txt).

---

## Part 10: Start the Services (11 more PowerShell windows)

Each service gets its own window. Start them in this order.

### Phase 1 — Quote Service (Go)
```powershell
cd C:\trading-platform\services\quote-service
$env:PORT="8002"
go run .
```

### Phase 2 — Market Data Sim (Go)
```powershell
cd C:\trading-platform\services\market-data-sim
$env:TICK_INTERVAL_MS="100"
go run .
```

### Phase 3 — Order Service (Python)
```powershell
cd C:\trading-platform\services\order-service
python -m venv venv
.\venv\Scripts\Activate.ps1
pip install -r requirements.txt
$env:PORT="8001"
uvicorn main:app --host 0.0.0.0 --port 8001
```

### Phase 4 — Risk Engine (Python)
```powershell
cd C:\trading-platform\services\risk-engine
python -m venv venv
.\venv\Scripts\Activate.ps1
pip install -r requirements.txt
$env:PORT="8004"
uvicorn main:app --host 0.0.0.0 --port 8004
```

### Phase 5 — Analytics (Python)
```powershell
cd C:\trading-platform\services\analytics
python -m venv venv
.\venv\Scripts\Activate.ps1
pip install -r requirements.txt
$env:PORT="8003"
uvicorn main:app --host 0.0.0.0 --port 8003
```

### Phase 6 — Event Processor (Python)
```powershell
cd C:\trading-platform\services\event-processor
python -m venv venv
.\venv\Scripts\Activate.ps1
pip install -r requirements.txt
python main.py
```

### Phase 7 — Legacy Adapter (Python)
```powershell
cd C:\trading-platform\services\legacy-adapter
python -m venv venv
.\venv\Scripts\Activate.ps1
pip install -r requirements.txt
$env:MQ_STYLE="ibm-mq"
python main.py
```

### Phase 8 — Batch Reconciler (Python)
```powershell
cd C:\trading-platform\services\batch-reconciler
python -m venv venv
.\venv\Scripts\Activate.ps1
pip install -r requirements.txt
$env:RECONCILIATION_INTERVAL="300"
python main.py
```

### Phase 9 — Gateway (Node.js)
```powershell
cd C:\trading-platform\services\gateway
npm install
$env:PORT="3000"
node server.js
```

### Phase 10 — UI (React/Vite)
```powershell
cd C:\trading-platform\services\ui
npm install
npm run dev -- --host 0.0.0.0 --port 8080
```

### Phase 11 — Traffic Generator (Python)
```powershell
cd C:\trading-platform\traffic-generator
python -m venv venv
.\venv\Scripts\Activate.ps1
pip install -r requirements.txt
$env:GATEWAY_URL="http://localhost:3000"
$env:TARGET_RPS="500"
$env:BURST_MULTIPLIER="10"
$env:CHAOS_ENABLED="true"
python main.py
```

---

## Part 11: Verify It's Running

From the VM:

```powershell
# Check each service is responding
curl http://localhost:3000/health    # Gateway
curl http://localhost:8001/health    # Order Service
curl http://localhost:8002/health    # Quote Service
curl http://localhost:8003/health    # Analytics
curl http://localhost:8004/health    # Risk Engine
curl http://localhost:9200           # Elasticsearch
curl http://localhost:8080           # UI
```

From your **local machine**, using the VM's external IP:

```
http://<VM_EXTERNAL_IP>:8080    # Trading UI
http://<VM_EXTERNAL_IP>:3000    # Gateway API
```

(Requires the firewall rule from the previous step to be in place.)

---

## Part 12: Stopping Everything

To stop: close each PowerShell window, or press `Ctrl+C` in each.

For infrastructure services running as Windows services:
```powershell
Stop-Service postgresql*
Stop-Service Memurai
```

---

## Troubleshooting

| Problem | Likely cause | Fix |
|---|---|---|
| `pip install` fails on a package needing C compiler | No Visual C++ Build Tools | `choco install -y visualstudio2022-workload-vctools` |
| Kafka won't start, "path too long" | Windows 260-char path limit | Move Kafka to `C:\k\` instead of `C:\kafka\` |
| Elasticsearch crashes with OOM | VM RAM exhausted | Lower `-Xmx` in `jvm.options` or upsize VM |
| `psql: command not found` | PATH not refreshed | Close and reopen PowerShell |
| Services can't reach Kafka | Using `kafka:29092` instead of `localhost:9092` | Check env vars point to `localhost` |
| UI loads but no data | Gateway can't reach backend services | Verify all 4 backend services are running |
| High CPU/memory | Expected — 4 vCPU / 15 GB is tight | Close RDP session (it keeps running) or upsize VM |

---

## Quick-Start Script (Optional)

To avoid opening 16+ PowerShell windows manually, save this as `start-all.ps1`:

```powershell
$root = "C:\trading-platform"

# Infrastructure
Start-Process powershell -ArgumentList "-NoExit","-Command","cd C:\kafka; .\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties"
Start-Sleep -Seconds 10
Start-Process powershell -ArgumentList "-NoExit","-Command","cd C:\kafka; .\bin\windows\kafka-server-start.bat .\config\server.properties"
Start-Process powershell -ArgumentList "-NoExit","-Command","cd C:\elasticsearch; .\bin\elasticsearch.bat"
Start-Sleep -Seconds 30

# Go services
Start-Process powershell -ArgumentList "-NoExit","-Command","cd $root\services\quote-service; `$env:PORT='8002'; go run ."
Start-Process powershell -ArgumentList "-NoExit","-Command","cd $root\services\market-data-sim; `$env:TICK_INTERVAL_MS='100'; go run ."

# Python services
Start-Process powershell -ArgumentList "-NoExit","-Command","cd $root\services\order-service; .\venv\Scripts\Activate.ps1; `$env:PORT='8001'; uvicorn main:app --host 0.0.0.0 --port 8001"
Start-Process powershell -ArgumentList "-NoExit","-Command","cd $root\services\risk-engine; .\venv\Scripts\Activate.ps1; `$env:PORT='8004'; uvicorn main:app --host 0.0.0.0 --port 8004"
Start-Process powershell -ArgumentList "-NoExit","-Command","cd $root\services\analytics; .\venv\Scripts\Activate.ps1; `$env:PORT='8003'; uvicorn main:app --host 0.0.0.0 --port 8003"
Start-Process powershell -ArgumentList "-NoExit","-Command","cd $root\services\event-processor; .\venv\Scripts\Activate.ps1; python main.py"
Start-Process powershell -ArgumentList "-NoExit","-Command","cd $root\services\legacy-adapter; .\venv\Scripts\Activate.ps1; `$env:MQ_STYLE='ibm-mq'; python main.py"
Start-Process powershell -ArgumentList "-NoExit","-Command","cd $root\services\batch-reconciler; .\venv\Scripts\Activate.ps1; python main.py"

# Gateway and UI
Start-Sleep -Seconds 15
Start-Process powershell -ArgumentList "-NoExit","-Command","cd $root\services\gateway; `$env:PORT='3000'; node server.js"
Start-Process powershell -ArgumentList "-NoExit","-Command","cd $root\services\ui; npm run dev -- --host 0.0.0.0 --port 8080"

# Traffic generator
Start-Sleep -Seconds 10
Start-Process powershell -ArgumentList "-NoExit","-Command","cd $root\traffic-generator; .\venv\Scripts\Activate.ps1; `$env:GATEWAY_URL='http://localhost:3000'; `$env:TARGET_RPS='500'; `$env:BURST_MULTIPLIER='10'; `$env:CHAOS_ENABLED='true'; python main.py"
```

Run it with:
```powershell
powershell -ExecutionPolicy Bypass -File C:\trading-platform\start-all.ps1
```

**Do the one-time setup (Parts 1-9) first** — the script only handles starting things, not installing them.
