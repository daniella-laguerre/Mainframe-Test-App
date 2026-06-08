# Running the Trading Platform on a Linux GCE VM

Step-by-step instructions for running the platform on Ubuntu 22.04 using Docker Compose.

---

## Part 1: SSH Into the VM

```bash
gcloud compute ssh legacy-trade-linux \
    --zone=us-central1-a \
    --project=project-d355a7b3-fd83-4391-af4
```

---

## Part 2: Verify Docker Is Installed

The startup script from [SETUP-gce-vms-linux.md](SETUP-gce-vms-linux.md) already installed Docker. Verify:

```bash
docker --version
docker compose version
```

If either command fails, install manually:

```bash
sudo apt-get update
sudo apt-get install -y ca-certificates curl gnupg git
sudo install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
sudo chmod a+r /etc/apt/keyrings/docker.gpg
echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu $(. /etc/os-release && echo $VERSION_CODENAME) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
sudo apt-get update
sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin
sudo systemctl enable --now docker
sudo usermod -aG docker $USER
```

Then log out and back in so the `docker` group applies:

```bash
exit
gcloud compute ssh legacy-trade-linux --zone=us-central1-a
```

Confirm you can run Docker without sudo:

```bash
docker ps
```

---

## Part 3: Get the Project Onto the VM

### Option A: Git clone (recommended)

```bash
cd ~
git clone <YOUR_REPO_URL> trading-platform
cd trading-platform
```

### Option B: Upload from local machine

From your **local machine**:

```bash
gcloud compute scp --recurse \
    /Users/xiap-al093/Projects/trading-platform \
    legacy-trade-linux:~/ \
    --zone=us-central1-a \
    --project=project-d355a7b3-fd83-4391-af4
```

Then on the VM:

```bash
cd ~/trading-platform
```

---

## Part 4: Start the Platform

One command does everything:

```bash
./scripts/start.sh
```

This runs the 5-phase startup defined in the script:
1. Infrastructure (Zookeeper, Kafka, Postgres, Redis, Elasticsearch)
2. Postgres replica
3. Core services (quote, order, risk, analytics, event-processor, gateway, legacy-adapter, batch-reconciler, market-data-sim)
4. UI
5. Traffic generator

First run takes **10-20 minutes** to pull and build all images. Subsequent starts are much faster.

If the script fails or isn't executable:

```bash
chmod +x scripts/*.sh
./scripts/start.sh
```

Or invoke Docker Compose directly:

```bash
docker compose up -d
```

---

## Part 5: Verify Everything Is Running

```bash
# See all running containers
docker compose ps

# Should show ~16 services: zookeeper, kafka, postgres, postgres-replica,
# redis, elasticsearch, gateway, order-service, quote-service, analytics,
# risk-engine, event-processor, legacy-adapter, market-data-sim,
# batch-reconciler, ui, traffic-generator
```

Health checks from the VM:

```bash
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

Get the external IP:
```bash
gcloud compute instances describe legacy-trade-linux \
    --zone=us-central1-a \
    --format='get(networkInterfaces[0].accessConfigs[0].natIP)'
```

---

## Part 6: Watching Logs

```bash
# All services (follow mode)
docker compose logs -f

# One service
docker compose logs -f gateway
docker compose logs -f order-service

# Last 100 lines
docker compose logs --tail=100 kafka

# Using the provided script
./scripts/logs.sh gateway
```

---

## Part 7: Stopping the Platform

```bash
# Stop everything (keeps containers)
docker compose stop

# Stop and remove containers (keeps data volumes)
docker compose down

# Stop, remove containers, AND delete data
docker compose down -v

# Or using the provided script
./scripts/stop.sh
```

---

## Part 8: Restarting After VM Reboot

Containers aren't set to auto-start. After `gcloud compute instances start`:

```bash
gcloud compute ssh legacy-trade-linux --zone=us-central1-a
cd ~/trading-platform
docker compose up -d
```

To make containers auto-start on boot, edit [docker-compose.yml](docker-compose.yml) and add `restart: unless-stopped` to each service.

---

## Common Operations

### Restart a single service
```bash
docker compose restart gateway
```

### Rebuild after code changes
```bash
docker compose build gateway
docker compose up -d gateway
```

### Check resource usage
```bash
docker stats                     # Live CPU/RAM per container
htop                             # Overall VM resources (install: sudo apt install htop)
df -h                            # Disk usage
```

### Clean up disk space
```bash
docker system prune -a           # Remove unused images/containers
docker volume prune              # Remove unused volumes (DESTROYS DATA)
```

### Exec into a container
```bash
docker compose exec postgres psql -U trading -d trading
docker compose exec gateway sh
docker compose exec order-service bash
```

### Reset the database
```bash
docker compose down
docker volume rm trading-platform_pgdata
docker compose up -d
```

---

## Troubleshooting

| Problem | Likely cause | Fix |
|---|---|---|
| `docker: permission denied` | User not in `docker` group | `sudo usermod -aG docker $USER` then log out/in |
| `Cannot connect to the Docker daemon` | Docker not running | `sudo systemctl start docker` |
| `port is already allocated` | Something else using the port | `sudo lsof -i :PORT` to find it, or change the port in docker-compose.yml |
| Services restart in a loop | OOM kills | Check `docker compose logs SERVICE`; upsize VM or reduce ES heap |
| Elasticsearch fails with `vm.max_map_count` | Kernel param too low | `sudo sysctl -w vm.max_map_count=262144` |
| UI loads but shows no data | Gateway can't reach backends | `docker compose logs gateway` to see backend errors |
| High load, slow response | VM is at capacity | Expected on `n1-standard-4`; upsize or disable traffic-generator |
| Disk fills up | Kafka logs + ES indices growing | `docker system prune -a`, or mount larger disk |
| `start.sh: Permission denied` | Script not executable | `chmod +x scripts/*.sh` |

---

## Resource Expectations on `n1-standard-4`

The VM has **4 vCPU and 15 GB RAM**. With all services running plus the traffic generator at 500 RPS:

- **CPU:** 60-90% sustained, spikes to 100% during 10x bursts
- **Memory:** 12-14 GB used (tight margin)
- **Disk I/O:** High (Kafka writes, Postgres WAL, ES indexing)
- **Network:** Mostly loopback traffic, minimal egress

If the VM struggles:
- Stop the traffic generator: `docker compose stop traffic-generator`
- Lower load: set `TARGET_RPS=50` in `docker-compose.yml` under traffic-generator
- Upsize: `gcloud compute instances set-machine-type legacy-trade-linux --machine-type=n1-standard-8 --zone=us-central1-a` (VM must be stopped first)

---

## Full Cheat Sheet

```bash
# Connect
gcloud compute ssh legacy-trade-linux --zone=us-central1-a

# Start
cd ~/trading-platform && ./scripts/start.sh

# Status
docker compose ps

# Logs
docker compose logs -f SERVICE_NAME

# Stop
./scripts/stop.sh

# Clean rebuild
docker compose down -v && docker compose up -d --build

# Access UI
# http://<VM_EXTERNAL_IP>:8080
```

---

## Next Steps

- Scaling: run multiple VMs and point them at a shared Postgres/Kafka cluster (advanced)
- Monitoring: add Prometheus + Grafana containers to `docker-compose.yml`
- CI/CD: set up GitHub Actions to auto-deploy on push
- Backups: schedule `pg_dump` for Postgres volume
