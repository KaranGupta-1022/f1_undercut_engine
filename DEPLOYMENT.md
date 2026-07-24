# Deployment

## 1. Local production stack (Docker Compose)

Runs the full stack (Zookeeper, Kafka, TimescaleDB, Redis, backend, frontend) in containers on one machine.

```bash
cp .env.example .env
# edit .env: set TIMESCALEDB_PASSWORD, FLASK_SECRET_KEY, CORS_ORIGINS=http://localhost
docker compose -f docker-compose.prod.yml up -d --build
docker exec kafka kafka-topics --create --topic f1-telemetry --bootstrap-server localhost:9092 --if-not-exists
```

Open `http://localhost`. Then run the producer separately (not containerized) to feed data:

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
cd backend
python producer.py
```

## 2. Cloud deployment used for the demo (GCP, temporary)

The original goal was Oracle Cloud's Always Free Ampere (A1.Flex) tier for an indefinite $0 deployment, but Oracle repeatedly returned "Out of capacity" across availability domains. Switched to Google Cloud's $300/90-day trial credit instead, intended to run for a few days for a demo, then be torn down — not a permanent free tier.

Steps taken:

1. **Create VM**: Compute Engine → e2-medium (2 vCPU, 4GB), Ubuntu 22.04 LTS, 30GB disk, "Allow HTTP traffic" checked. (Note: `us-central1` had no e2-medium capacity at the time; `us-east1-b` worked — capacity errors are transient and region-specific, just retry elsewhere.)
2. **Install Docker**: official Docker apt repo for Ubuntu 22.04 (`docker-ce`, `docker-ce-cli`, `containerd.io`, `docker-buildx-plugin`, `docker-compose-plugin`), then `sudo usermod -aG docker $USER` and reconnect.
3. **Clone the repo** onto the VM.
4. **Set `.env`** with `CORS_ORIGINS=http://<EXTERNAL_IP>` (no trailing slash) and strong values for `TIMESCALEDB_PASSWORD` / `FLASK_SECRET_KEY` (e.g. via `openssl rand -hex 32`).
5. **Build and start**: `docker compose -f docker-compose.prod.yml up -d --build`, then create the Kafka topic as in Step 1.
6. **Verify**: `curl http://<EXTERNAL_IP>/api/status` and open `http://<EXTERNAL_IP>` in a browser — check the dashboard loads, the connection indicator is green, and there are no CORS errors.

## 3. FastF1 cloud IP blocking (known issue + workaround)

Running `producer.py` directly on the GCP VM failed: every FastF1 data category returned "Failed to load," and `session.laps` raised `DataNotLoadedError`. Root cause, confirmed with `curl -I` against `livetiming.formula1.com`: **HTTP 403 from CloudFront** on the VM's IP, while the same request succeeds from a residential/laptop IP. This is F1's anti-scraping CDN rule blocking known cloud provider IP ranges (GCP/AWS/Azure) — not a bug in this app or its Docker setup.

**Workaround:** FastF1 caches session data to disk as `.ff1pkl` files under `cache/<year>/<event>/<session>/`. Since the target session was already cached locally from prior development runs:

1. Zip just that session's cache folder, e.g. `cache/2024/2024-06-23_Spanish_Grand_Prix/2024-06-23_Race/`.
2. Upload the zip to the VM (e.g. via the GCP console's SSH-in-browser "Upload File" button).
3. Unzip it into `~/f1_undercut_engine/cache/` on the VM, preserving the folder structure.

With the cache pre-populated, `producer.py` reads the session from disk and never needs to contact the blocked live-timing endpoint. No application code was changed for this — it's a data-provisioning workaround specific to demoing from a cloud VM. A longer-lived deployment would need either a non-cloud egress path (residential proxy, VPN) or to always pre-cache sessions locally before deploying.

## 4. Running the producer on the VM

Once the cache is in place:

```bash
cd ~/f1_undercut_engine
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
cd backend
python producer.py
```

Kafka is reachable from the host at `localhost:9092` (published by compose). The dashboard should start showing live standings, lap times, and undercut alerts (after ~lap 10).

## 5. Teardown

Since the GCP deployment is meant to run only a few days:

```bash
cd ~/f1_undercut_engine
docker compose -f docker-compose.prod.yml down
```

Then in the GCP Console: Compute Engine → VM instances → select the instance → **Stop** (keeps the disk, resumable later, small storage cost) or **Delete** (fully removes it, no further cost). Stopping or deleting matters because leaving the VM running past the trial credit or 90 days bills at roughly $25–30/month for an e2-medium — this is not a permanent free tier.

## 6. Optional future work

- **Oracle Always Free Ampere (A1.Flex)**: retry if capacity opens up, for a true $0/indefinite deployment with the same self-hosted architecture.
- **Managed free tiers** (Upstash Kafka/Redis, Neon/Supabase Postgres, Render + Vercel/Netlify): removes server maintenance but trades away self-hosted TimescaleDB hypertable optimizations — worth mentioning as an alternative if asked.
- **Domain & SSL**: skipped for this demo (IP + HTTP is simpler for a short-lived demo). Would involve a free domain, an A record, Nginx reverse proxy config, and Certbot for Let's Encrypt if a longer-lived deployment is set up later.
