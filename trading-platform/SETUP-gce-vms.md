# GCE VM Setup Guide

Instructions for creating the three Windows VMs that host the legacy trading app.

---

## Prerequisites

1. **Google Cloud SDK installed** on your local machine
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

4. **Your public IP** (for firewall rules)
   ```bash
   curl ifconfig.me
   ```
   Save this — you'll use it as `YOUR_IP` below.

---

## Configuration

Set these variables once. The rest of the commands reference them:

```bash
PROJECT=project-d355a7b3-fd83-4391-af4
ZONE=us-central1-a
REGION=us-central1
YOUR_IP=REPLACE_WITH_YOUR_PUBLIC_IP   # e.g., 73.12.34.56
```

---

## Step 1: Create Firewall Rules

Run these **once**. They apply to any VM tagged `legacy-app`.

```bash
# External access (RDP + app ports) — locked to your IP only
gcloud compute firewall-rules create allow-legacy-app \
    --project=$PROJECT \
    --network=default \
    --direction=INGRESS \
    --action=ALLOW \
    --rules=tcp:3389,tcp:3000,tcp:8000,tcp:8001-8004,tcp:8080,tcp:5432-5433,tcp:6379,tcp:9092,tcp:9200,tcp:2181,tcp:4317,udp:4318 \
    --source-ranges=$YOUR_IP/32 \
    --target-tags=legacy-app

# Internal VM-to-VM (so the 3 hosts can talk freely)
gcloud compute firewall-rules create allow-internal-vpc \
    --project=$PROJECT \
    --network=default \
    --direction=INGRESS \
    --action=ALLOW \
    --rules=tcp,udp,icmp \
    --source-ranges=10.128.0.0/9
```

Verify:
```bash
gcloud compute firewall-rules list --project=$PROJECT
```

---

## Step 2: Create the Three VMs

### VM 1 — Primary host (`legacy-trade-host`)

```bash
gcloud compute instances create legacy-trade-host \
    --project=$PROJECT \
    --zone=$ZONE \
    --machine-type=n1-standard-4 \
    --network-interface=network-tier=PREMIUM,stack-type=IPV4_ONLY,subnet=default \
    --metadata=enable-osconfig=TRUE \
    --maintenance-policy=MIGRATE \
    --provisioning-model=STANDARD \
    --min-cpu-platform="Intel Haswell" \
    --enable-nested-virtualization \
    --image-family=windows-2022 \
    --image-project=windows-cloud \
    --boot-disk-size=80GB \
    --boot-disk-type=pd-standard \
    --tags=legacy-app
```

### VM 2 — Second test host (`legacy-trade-host-2`)

```bash
gcloud compute instances create legacy-trade-host-2 \
    --project=$PROJECT \
    --zone=$ZONE \
    --machine-type=n1-standard-4 \
    --network-interface=network-tier=PREMIUM,stack-type=IPV4_ONLY,subnet=default \
    --metadata=enable-osconfig=TRUE \
    --maintenance-policy=MIGRATE \
    --provisioning-model=STANDARD \
    --min-cpu-platform="Intel Haswell" \
    --enable-nested-virtualization \
    --image-family=windows-2022 \
    --image-project=windows-cloud \
    --boot-disk-size=80GB \
    --boot-disk-type=pd-standard \
    --tags=legacy-app
```

### VM 3 — Third test host (`legacy-trade-host-3`)

```bash
gcloud compute instances create legacy-trade-host-3 \
    --project=$PROJECT \
    --zone=$ZONE \
    --machine-type=n1-standard-4 \
    --network-interface=network-tier=PREMIUM,stack-type=IPV4_ONLY,subnet=default \
    --metadata=enable-osconfig=TRUE \
    --maintenance-policy=MIGRATE \
    --provisioning-model=STANDARD \
    --min-cpu-platform="Intel Haswell" \
    --enable-nested-virtualization \
    --image-family=windows-2022 \
    --image-project=windows-cloud \
    --boot-disk-size=80GB \
    --boot-disk-type=pd-standard \
    --tags=legacy-app
```

**Creation takes ~1-2 minutes per VM.** Windows boot initialization takes an additional 3-5 minutes before you can RDP in.

---

## Step 3: Generate Windows Passwords

Wait ~5 minutes after VM creation, then generate RDP credentials for each VM:

```bash
gcloud compute reset-windows-password legacy-trade-host \
    --zone=$ZONE \
    --user=legacyops

gcloud compute reset-windows-password legacy-trade-host-2 \
    --zone=$ZONE \
    --user=legacyops

gcloud compute reset-windows-password legacy-trade-host-3 \
    --zone=$ZONE \
    --user=legacyops
```

Each command outputs:
```
ip_address: 35.XXX.XXX.XXX
password:   <generated_password>
username:   legacyops
```

**Save these credentials somewhere safe** — you need them to RDP in.

---

## Step 4: Verify Everything

List your VMs and confirm status:

```bash
gcloud compute instances list --project=$PROJECT
```

You should see:
```
NAME                  ZONE            MACHINE_TYPE   STATUS   EXTERNAL_IP
legacy-trade-host     us-cent
...n1-standard-4  RUNNING  35.XXX.XXX.XXX
legacy-trade-host-2   us-central1-a   n1-standard-4  RUNNING  35.XXX.XXX.XXX
legacy-trade-host-3   us-central1-a   n1-standard-4  RUNNING  35.XXX.XXX.XXX
```

Confirm tags and firewall:
```bash
gcloud compute instances describe legacy-trade-host \
    --zone=$ZONE \
    --format="value(tags.items)"
# Should output: legacy-app
```

---

## Step 5: Connect via RDP

### From Windows
Open Remote Desktop (`mstsc`), enter the VM's external IP, and log in with `legacyops` + the generated password.

### From Mac
Install **Microsoft Remote Desktop** from the App Store. Add a new PC with the external IP.

### From Linux
Install Remmina: `sudo apt install remmina remmina-plugin-rdp`

---

## VM Specs Reference

All three VMs are identical:

| Spec | Value |
|---|---|
| Machine type | `n1-standard-4` (4 vCPU, 15 GB RAM) |
| CPU platform | Intel Haswell (minimum) |
| Nested virtualization | Enabled (for WSL2 if needed) |
| OS | Windows Server 2022 |
| Boot disk | 80 GB `pd-standard` (HDD) |
| Zone | `us-central1-a` |
| Tags | `legacy-app` |
| Network | `default` VPC |
| External IP | Ephemeral (changes on restart) |

---

## Cost Estimate

Rough monthly cost if all 3 VMs run 24/7:

| Item | Cost |
|---|---|
| 3× n1-standard-4 VMs | ~$430/month |
| 3× 80 GB pd-standard disks | ~$10/month |
| Windows licensing | ~$50/month per VM |
| Network egress | varies |
| **Total** | **~$580-650/month** |

**To save money:** stop VMs when not in use.
```bash
gcloud compute instances stop legacy-trade-host legacy-trade-host-2 legacy-trade-host-3 --zone=$ZONE
gcloud compute instances start legacy-trade-host legacy-trade-host-2 legacy-trade-host-3 --zone=$ZONE
```

Stopped VMs only incur disk cost (~$3/disk/month).

---

## Teardown (When Done Testing)

```bash
# Delete VMs
gcloud compute instances delete legacy-trade-host legacy-trade-host-2 legacy-trade-host-3 \
    --zone=$ZONE \
    --quiet

# Delete firewall rules
gcloud compute firewall-rules delete allow-legacy-app allow-internal-vpc --quiet
```

---

## Troubleshooting

| Problem | Fix |
|---|---|
| `reset-windows-password` fails: "instance not ready" | Wait another 2-3 minutes and retry |
| RDP times out | Check firewall: is port 3389 open from your IP? Did your IP change? |
| `ERROR: ... quota exceeded` | Your project has a low CPU quota. Request increase in GCP Console → IAM → Quotas |
| `Nested virtualization is not allowed` | Some GCP projects block it — contact your admin, or create without `--enable-nested-virtualization` |
| VM won't start after stop | Check zone capacity. Try a different zone (`us-central1-b`, `us-central1-c`) |
| IP changed after restart | Ephemeral IPs change. Reserve a static IP if you need a fixed one |

---

## Next Steps

1. RDP into each VM
2. Follow [RUNBOOK-windows-vm.md](RUNBOOK-windows-vm.md) to install and run the trading platform
3. Reference [env-windows-local.txt](env-windows-local.txt) for environment variable values
