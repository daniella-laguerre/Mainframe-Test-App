# GCE Linux VM Setup Guide

Instructions for creating Linux VMs on GCE to host the trading platform.

---

## Why Linux Instead of Windows?

| | Linux | Windows |
|---|---|---|
| **Docker** | Runs natively, no Docker Desktop needed | Requires WSL2 + nested virtualization |
| **Setup time** | ~10 minutes | ~1-2 hours |
| **Monthly cost** | ~$100/VM | ~$150/VM (licensing fee) |
| **Resource overhead** | ~500 MB OS | ~4 GB OS |
| **App compatibility** | All services Linux-native | Needs Memurai instead of Redis, manual Kafka/ES installs |

**For this trading platform, Linux is the recommended path.**

---

## Prerequisites

1. **Google Cloud SDK installed**
   - Download: https://cloud.google.com/sdk/docs/install
   - Verify: `gcloud --version`

2. **Authenticated to GCP**
   ```bash
   gcloud auth login
   gcloud config set project project-d355a7b3-fd83-4391-af4
   ```

3. **Compute Engine API enabled**
   ```bash
   gcloud services enable compute.googleapis.com
   ```

4. **Your public IP**
   ```bash
   curl ifconfig.me
   ```

---

## Configuration

```bash
PROJECT=project-d355a7b3-fd83-4391-af4
ZONE=us-central1-a
REGION=us-central1
YOUR_IP=REPLACE_WITH_YOUR_PUBLIC_IP   # e.g., 73.12.34.56
```

---

## Step 1: Create Firewall Rules

```bash
# External access (SSH + app ports) — locked to your IP
gcloud compute firewall-rules create allow-legacy-app-linux \
    --project=$PROJECT \
    --network=default \
    --direction=INGRESS \
    --action=ALLOW \
    --rules=tcp:22,tcp:3000,tcp:8000,tcp:8001-8004,tcp:8080,tcp:5432-5433,tcp:6379,tcp:9092,tcp:9200,tcp:2181,tcp:4317,udp:4318 \
    --source-ranges=$YOUR_IP/32 \
    --target-tags=legacy-app-linux

# Internal VM-to-VM
gcloud compute firewall-rules create allow-internal-vpc \
    --project=$PROJECT \
    --network=default \
    --direction=INGRESS \
    --action=ALLOW \
    --rules=tcp,udp,icmp \
    --source-ranges=10.128.0.0/9
```

> Skip `allow-internal-vpc` if you already created it from the Windows guide.

---

## Step 2: Create the VMs

### Single VM with Docker (Recommended)

The fastest path — the startup script installs Docker automatically on first boot:

```bash
gcloud compute instances create legacy-trade-linux \
    --project=$PROJECT \
    --zone=$ZONE \
    --machine-type=n1-standard-4 \
    --network-interface=network-tier=PREMIUM,stack-type=IPV4_ONLY,subnet=default \
    --maintenance-policy=MIGRATE \
    --provisioning-model=STANDARD \
    --image-family=ubuntu-2204-lts \
    --image-project=ubuntu-os-cloud \
    --boot-disk-size=80GB \
    --boot-disk-type=pd-standard \
    --tags=legacy-app-linux \
    --metadata=startup-script='#!/bin/bash
apt-get update
apt-get install -y ca-certificates curl gnupg git
install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | gpg --dearmor -o /etc/apt/keyrings/docker.gpg
chmod a+r /etc/apt/keyrings/docker.gpg
echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu $(. /etc/os-release && echo $VERSION_CODENAME) stable" | tee /etc/apt/sources.list.d/docker.list > /dev/null
apt-get update
apt-get install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin
systemctl enable --now docker
usermod -aG docker $(ls /home | head -1)'
```

### Additional VMs (if you need 3 hosts like the Windows setup)

```bash
# VM 2
gcloud compute instances create legacy-trade-linux-2 \
    --project=$PROJECT \
    --zone=$ZONE \
    --machine-type=n1-standard-4 \
    --image-family=ubuntu-2204-lts \
    --image-project=ubuntu-os-cloud \
    --boot-disk-size=80GB \
    --boot-disk-type=pd-standard \
    --tags=legacy-app-linux \
    --metadata-from-file=startup-script=startup.sh

# VM 3
gcloud compute instances create legacy-trade-linux-3 \
    --project=$PROJECT \
    --zone=$ZONE \
    --machine-type=n1-standard-4 \
    --image-family=ubuntu-2204-lts \
    --image-project=ubuntu-os-cloud \
    --boot-disk-size=80GB \
    --boot-disk-type=pd-standard \
    --tags=legacy-app-linux \
    --metadata-from-file=startup-script=startup.sh
```

Save the startup script contents to `startup.sh` first to reuse across VMs.

---

## Step 3: Connect via SSH

Unlike Windows, no password generation needed. SSH key pairs are created automatically.

```bash
gcloud compute ssh legacy-trade-linux --zone=$ZONE --project=$PROJECT
```

First connection may take ~1 minute as gcloud generates your SSH keys.

---

## Step 4: Verify Docker Is Ready

Once SSH'd in:

```bash
# Wait ~2 minutes after VM creation for the startup script to finish
# Check startup script output:
sudo journalctl -u google-startup-scripts.service

# Verify Docker
docker --version
docker compose version

# If you get "permission denied", log out and back in for group changes:
exit
gcloud compute ssh legacy-trade-linux --zone=$ZONE
docker ps
```

---

## Step 5: Verify Everything

```bash
gcloud compute instances list --project=$PROJECT
```

Expected output:
```
NAME                    ZONE            MACHINE_TYPE   STATUS   EXTERNAL_IP
legacy-trade-linux      us-central1-a   n1-standard-4  RUNNING  35.XXX.XXX.XXX
```

---

## VM Specs Reference

| Spec | Value |
|---|---|
| Machine type | `n1-standard-4` (4 vCPU, 15 GB RAM) |
| OS | Ubuntu 22.04 LTS |
| Boot disk | 80 GB `pd-standard` (HDD) |
| Zone | `us-central1-a` |
| Tags | `legacy-app-linux` |
| Network | `default` VPC |
| Startup script | Installs Docker automatically |

---

## Cost Estimate

| Item | Cost |
|---|---|
| 1× n1-standard-4 VM | ~$100/month |
| 1× 80 GB pd-standard disk | ~$3/month |
| Network egress | varies |
| **Total (1 VM 24/7)** | **~$105/month** |
| **Total (3 VMs 24/7)** | **~$315/month** |

**Stop VMs when not in use to save money:**
```bash
gcloud compute instances stop legacy-trade-linux --zone=$ZONE
gcloud compute instances start legacy-trade-linux --zone=$ZONE
```

---

## Teardown

```bash
# Delete VMs
gcloud compute instances delete legacy-trade-linux --zone=$ZONE --quiet

# Delete firewall rule
gcloud compute firewall-rules delete allow-legacy-app-linux --quiet
```

---

## Troubleshooting

| Problem | Fix |
|---|---|
| `docker: permission denied` | Log out and back in (`exit`, then SSH again) to pick up the `docker` group |
| Startup script didn't run | Check `sudo journalctl -u google-startup-scripts.service` for errors |
| SSH times out | Check firewall has port 22 open from your IP; verify VM is RUNNING |
| `out of memory` when running Docker Compose | Upsize to `n1-standard-8` or stop non-essential services |
| Disk fills up | `docker system prune -a` to clean up old images |

---

## Next Steps

1. SSH into the VM
2. Follow [RUNBOOK-linux-vm.md](RUNBOOK-linux-vm.md) to run the trading platform (Docker Compose path)
