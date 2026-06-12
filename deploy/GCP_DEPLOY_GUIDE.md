# Deploy TitanChat to GCP Compute Engine via GitHub

## 1. Create the GCP Compute Engine VM

```bash
gcloud compute instances create titanchat-vm \
  --zone=us-central1-a \
  --machine-type=e2-standard-2 \
  --image-family=ubuntu-2404-lts-amd64 \
  --image-project=ubuntu-os-cloud \
  --boot-disk-size=30GB \
  --tags=http-server,https-server

# Open required ports
gcloud compute firewall-rules create allow-titanchat \
  --allow=tcp:8000,tcp:8001 \
  --target-tags=http-server
```

## 2. Push Your Code to GitHub

```bash
cd F:\titanchatsystem
git init
git remote add origin git@github.com:avihanwat/titanchatsystem.git
git add .
git commit -m "initial commit"
git push -u origin main
```

## 3. SSH into the VM and Run Initial Setup

```bash
gcloud compute ssh titanchat-vm --zone=us-central1-a
```

On the VM:

```bash
# Clone your repo
sudo mkdir -p /opt/titanchat
sudo useradd -r -m -s /bin/bash titanchat
sudo chown titanchat:titanchat /opt/titanchat
sudo -u titanchat git clone git@github.com:avihanwat/titanchatsystem.git /opt/titanchat/repo

# Run the existing setup script
sudo bash /opt/titanchat/repo/deploy/setup-vm.sh
```

## 4. Edit Environment Variables

```bash
sudo nano /opt/titanchat/.env
# Fill in real values for JWT_SECRET, POSTGRES_PASSWORD, etc.
```

## 5. Set Up Automated Deployments via GitHub Actions

Create `.github/workflows/deploy.yml` in your repo:

```yaml
name: Deploy to GCP

on:
  push:
    branches: [main]

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - name: Deploy via SSH
        uses: appleboy/ssh-action@v1
        with:
          host: ${{ secrets.VM_EXTERNAL_IP }}
          username: titanchat
          key: ${{ secrets.SSH_PRIVATE_KEY }}
          script: bash /opt/titanchat/repo/deploy/deploy.sh
```

## 6. Configure GitHub Secrets

In your GitHub repo → **Settings** → **Secrets and variables** → **Actions**, add:

| Secret             | Value                                                                 |
|--------------------|-----------------------------------------------------------------------|
| `VM_EXTERNAL_IP`   | Your VM's external IP (`gcloud compute instances describe titanchat-vm`) |
| `SSH_PRIVATE_KEY`  | SSH private key for the `titanchat` user                              |

## 7. Set Up SSH Key Access

On your local machine:

```bash
ssh-keygen -t ed25519 -f titanchat-deploy-key -C "github-actions"
```

On the VM:

```bash
sudo -u titanchat mkdir -p /home/titanchat/.ssh
echo "ssh-ed25519 AAAA... github-actions" | sudo -u titanchat tee /home/titanchat/.ssh/authorized_keys
sudo chmod 600 /home/titanchat/.ssh/authorized_keys
```

Put the **private key** content into the `SSH_PRIVATE_KEY` GitHub secret.

## How It Works

1. You push to `main` on GitHub
2. GitHub Actions SSHs into the VM as `titanchat`
3. Runs `deploy/deploy.sh` which:
   - Pulls latest code from `origin/main`
   - Syncs code to `/opt/titanchat/`
   - Installs/updates Python dependencies
   - Restarts the 3 systemd services:
     - `titanchat-gateway` (WebSocket server on port 8000)
     - `titanchat-consumer` (Kafka consumer worker)
     - `titanchat-api` (Admin/Auth API on port 8001)

## Services Architecture (Single VM)

```
┌─────────────────────────────────────────────┐
│              GCP Compute Engine              │
│                                             │
│  ┌─────────────┐  ┌──────────────────────┐  │
│  │  Gateway    │  │  Consumer            │  │
│  │  :8000      │  │  (Kafka → Cassandra) │  │
│  └─────────────┘  └──────────────────────┘  │
│  ┌─────────────┐                            │
│  │  API        │                            │
│  │  :8001      │                            │
│  └─────────────┘                            │
│                                             │
│  Redis | Kafka | Cassandra | PostgreSQL     │
└─────────────────────────────────────────────┘
```

## Troubleshooting

```bash
# Check service status
sudo systemctl status titanchat-gateway titanchat-consumer titanchat-api

# View logs
tail -f /var/log/titanchat/gateway.log
tail -f /var/log/titanchat/consumer.log
tail -f /var/log/titanchat/api.log

# Manual deploy
sudo -u titanchat bash /opt/titanchat/repo/deploy/deploy.sh

# Health check
bash /opt/titanchat/repo/deploy/healthcheck.sh
```
