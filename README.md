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
