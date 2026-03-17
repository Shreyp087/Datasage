# DataSage

DataLoader Platform built with FastAPI, React, Celery, and CrewAI.

## Tech Stack
- **Backend**: Python 3.11, FastAPI, SQLAlchemy (async), Alembic, PostgreSQL
- **Worker**: Celery 5 + Redis
- **Storage**: MinIO (S3-compatible, local dev)
- **Frontend**: React 18 + TypeScript + TailwindCSS + Recharts
- **AI Agents**: CrewAI + Anthropic Claude API (claude-sonnet-4-6)
- **Containerization**: Docker + docker-compose

## Requirements
- Docker & Docker Compose
- Node.js 18+ (for local frontend dev)
- Python 3.11+ (for local backend dev)

## ⚡ Quickstart

### Method 1 — Auto-download (simplest)
```bash
pip install -r requirements.txt
jupyter notebook AIID_Research_Notebook.ipynb
# Then: Kernel → Restart & Run All
# The notebook downloads the snapshot automatically
```

### Method 2 — Download with curl first (recommended for slow connections)
```bash
# Step 1: Download snapshot manually
mkdir -p aiid_cache
curl -L --progress-bar \
  "https://pub-72b2b2fc36ec423189843747af98f80e.r2.dev/backup-20260223102103.tar.bz2" \
  -o "aiid_cache/aiid_latest.tar.bz2"

# Step 2: In Cell 3 of the notebook, set:
#   LOCAL_TARBALL = "aiid_cache/aiid_latest.tar.bz2"
#   SNAPSHOT_URL  = None

# Step 3: Run
pip install -r requirements.txt
jupyter notebook AIID_Research_Notebook.ipynb
```

### Method 3 — wget
```bash
mkdir -p aiid_cache
wget --show-progress \
  "https://pub-72b2b2fc36ec423189843747af98f80e.r2.dev/backup-20260223102103.tar.bz2" \
  -O "aiid_cache/aiid_latest.tar.bz2"
```

### Method 4 — Pre-extract for fastest startup
```bash
mkdir -p aiid_cache/extracted
tar -xjf aiid_cache/aiid_latest.tar.bz2 -C aiid_cache/extracted

# In Cell 3 set:
#   EXTRACTED_DIR = "aiid_cache/extracted"
#   SNAPSHOT_URL  = None
```

### Method 5 — Google Colab (no install needed)
1. Open notebook in Colab via the badge at top
2. Uncomment `upload_in_colab()` in the Colab Upload cell
3. Upload your downloaded `.tar.bz2` file
4. Update `LOCAL_TARBALL` in Cell 3
5. Runtime → Run All

### Method 6 — Use a different snapshot
```bash
# Browse all snapshots at:
# https://incidentdatabase.ai/research/snapshots/
# Copy any .tar.bz2 URL and paste into SNAPSHOT_URL in Cell 3
curl -L --progress-bar \
  "PASTE_URL_HERE" \
  -o "aiid_cache/my_snapshot.tar.bz2"
```

## Supported Snapshot Formats
The notebook auto-detects whichever format is in the tarball:

| Format | Status | Notes |
|--------|--------|-------|
| CSV | ✅ Primary | Fastest, always preferred |
| JSON | ✅ Fallback | Used if no CSV found |
| MongoDB BSON | ✅ Last resort | Requires `pip install pymongo` |

## Running Locally

1. Setup environment variables:
   ```bash
   cp .env.example .env
   ```
   *Note: Edit `.env` to select your `LLM_PROVIDER` (e.g., `openai` or `anthropic`) and supply the corresponding API key (e.g., `OPENAI_API_KEY`) before starting processing tasks.*

2. **Option A: Run Hybrid Locally (Host Machine + Cloud DBs + MinIO mock)**
   Ensure you've provided `DATABASE_URL` (e.g. Neon.tech) and `REDIS_URL` (e.g. Upstash) inside `.env`.
   Run the background services concurrently via the provided PowerShell script:
   ```powershell
   .\start_local.ps1
   ```
   *This starts the FastAPI backend, Celery worker, and React frontend in separate windows.*

3. **Option B: Run Fully Containerized**
   Spin up the infrastructure (Postgres, Redis, MinIO) and application natively with Docker Compose:
   ```bash
   docker-compose up --build -d
   ```

3. Services available at:
   - Frontend: [http://localhost:3000](http://localhost:3000)
   - Backend API: [http://localhost:8000](http://localhost:8000)
   - API Docs: [http://localhost:8000/docs](http://localhost:8000/docs)
   - MinIO Console: [http://localhost:9001](http://localhost:9001) (Login: admin / admin1234)

## Upload Support
The platform natively supports large chunked uploads for files up to 10GB. The chunks are processed via FastAPI, dispatched to MinIO multipart endpoints (local), and finalized seamlessly via the background Celery workers utilizing CrewAI for intelligence EDA.
