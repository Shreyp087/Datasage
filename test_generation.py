#!/usr/bin/env python3
"""
DataSage — README + Notebook Generation Test Runner
====================================================
Run this INSIDE Docker OR locally with Python 3.8+

Usage (from datasage/ directory):
  # Option A: run locally
  python3 test_generation.py

  # Option B: run inside Docker API container
  docker-compose exec api python3 /app/test_generation.py

  # Option C: run with custom settings
  python3 test_generation.py --url http://localhost:8000 --email you@test.com
"""

import os
import sys
import json
import time
import argparse
import tempfile
import textwrap
import subprocess
from pathlib import Path
from datetime import datetime

# ── Try imports, install if missing ──────────────────────────
try:
    import requests
except ImportError:
    subprocess.run([sys.executable, "-m", "pip", "install",
                    "requests", "--quiet"], check=True)
    import requests

# ── Config ───────────────────────────────────────────────────
BASE_URL  = "http://localhost:8000"
EMAIL     = "test@datasage.com"
PASSWORD  = "testpass123"
NB_PATH   = Path("./notebooks/AIID_Research_Notebook.ipynb")

# ── Counters ─────────────────────────────────────────────────
results = {"pass": 0, "fail": 0, "warn": 0}
TOKEN    = None
DATASET_ID = None

# ── Pretty print helpers ──────────────────────────────────────
GREEN  = "\033[92m"
RED    = "\033[91m"
YELLOW = "\033[93m"
CYAN   = "\033[96m"
GRAY   = "\033[90m"
RESET  = "\033[0m"
BOLD   = "\033[1m"


def passed(msg):
    print(f"  {GREEN}[PASS]{RESET} {msg}")
    results["pass"] += 1

def failed(msg):
    print(f"  {RED}[FAIL]{RESET} {msg}")
    results["fail"] += 1

def warned(msg):
    print(f"  {YELLOW}[WARN]{RESET} {msg}")
    results["warn"] += 1

def info(msg):
    print(f"  {CYAN}[INFO]{RESET} {msg}")

def section(title):
    print()
    print(f"{GRAY}{'='*65}{RESET}")
    print(f"{BOLD}  {title}{RESET}")
    print(f"{GRAY}{'='*65}{RESET}")

def preview(text, lines=20, label="Preview"):
    print(f"\n  {GRAY}── {label} (first {lines} lines) ───────────────{RESET}")
    for line in text.split("\n")[:lines]:
        print(f"  {GRAY}{line}{RESET}")
    print(f"  {GRAY}[...]{RESET}")

# ================================================================
# PHASE 1 — INFRASTRUCTURE
# ================================================================
section("PHASE 1 — Infrastructure Health")

try:
    r = requests.get(f"{BASE_URL}/health", timeout=5)
    if r.json().get("status") == "ok":
        passed("API is up")
    else:
        failed(f"API health: {r.json()}")
        sys.exit(1)
except Exception as e:
    failed(f"API unreachable: {e}")
    print(f"  Make sure docker-compose is running: docker-compose up -d")
    sys.exit(1)

for endpoint, label in [
    ("/api/v1/health/db", "Database"),
]:
    try:
        r = requests.get(f"{BASE_URL}{endpoint}", timeout=5)
        data = r.json()
        if "connected" in str(data):
            passed(f"{label} connected")
        else:
            warned(f"{label} status unclear: {data}")
    except Exception as e:
        warned(f"{label} health check failed: {e}")

# ================================================================
# PHASE 2 — AUTHENTICATION
# ================================================================
section("PHASE 2 — Authentication")

# Register
try:
    r = requests.post(f"{BASE_URL}/api/v1/auth/register", json={
        "email": EMAIL, "password": PASSWORD, "name": "Test User"
    }, timeout=10)
    if r.status_code in (200, 201):
        passed("Register works")
    elif r.status_code == 400:
        info("User already exists — OK")
    elif r.status_code == 404:
        failed("Auth route not found (404) — add auth router to main.py")
        info("Fix: app.include_router(auth.router, prefix='/api/v1')")
    else:
        warned(f"Register returned {r.status_code}: {r.text[:100]}")
except Exception as e:
    failed(f"Register failed: {e}")

# Login
try:
    r = requests.post(f"{BASE_URL}/api/v1/auth/login", json={
        "email": EMAIL, "password": PASSWORD
    }, timeout=10)
    data = r.json()
    TOKEN = (data.get("access_token") or
             data.get("token") or
             (data.get("data") or {}).get("access_token"))
    if TOKEN:
        passed(f"Login OK — token obtained ({TOKEN[:20]}...)")
    else:
        failed(f"Login returned no token: {data}")
        sys.exit(1)
except Exception as e:
    failed(f"Login failed: {e}")
    sys.exit(1)

HEADERS = {"Authorization": f"Bearer {TOKEN}"}

# ================================================================
# PHASE 3 — DATA INGESTION
# ================================================================
section("PHASE 3 — AIID Data Ingestion")

# Build test CSV
CSV_CONTENT = """incident_id,title,date,allegedDeployerOfAISystem,allegedDeveloperOfAISystem,description,harm_type,sector_of_deployment
1,Autonomous Vehicle Pedestrian Strike,2019-03-18,Uber,Uber ATG,Self-driving car struck pedestrian,Physical Harm,Transportation
2,Facial Recognition False Arrest,2020-06-24,Detroit Police Dept,Amazon,Man wrongfully arrested,Harm to Civil Liberties,Law Enforcement
3,Hiring Algorithm Gender Bias,2018-10-10,Amazon,Amazon,AI recruiting penalized women,Discrimination,Employment
4,Trading Algorithm Flash Crash,2010-05-06,Multiple Firms,Multiple,Algorithms caused market crash,Financial Harm,Finance
5,Deepfake Political Video,2023-01-15,Unknown,Unknown,AI video spread misinformation,Harm to Social Systems,Politics
6,Healthcare AI Misdiagnosis,2021-07-20,Hospital Network,IBM Watson Health,AI recommended wrong treatments,Physical Harm,Healthcare
7,Chatbot Suicide Encouragement,2023-03-28,Character.ai,Character.ai,Chatbot encouraged self-harm,Psychological Harm,Entertainment
8,Credit Scoring Racial Bias,2019-11-01,Apple Card,Goldman Sachs,Algorithm gave lower limits to minorities,Discrimination,Finance
9,Predictive Policing Overreach,2020-08-14,Chicago PD,ShotSpotter,System led to wrongful stops,Harm to Civil Liberties,Law Enforcement
10,Content Moderation Failure,2022-05-03,Facebook,Meta,Algorithm amplified hate speech,Harm to Social Systems,Social Media
11,Autonomous Weapon Misidentification,2021-03-25,Military,Defense Contractor,Weapon misidentified civilian,Physical Harm,Military
12,Biometric Surveillance Abuse,2022-09-14,Government Agency,NEC Corporation,Facial recognition tracked protesters,Harm to Civil Liberties,Government
13,Student Exam AI Bias,2020-08-20,University Systems,Proctorio,Exam AI flagged minorities at higher rates,Discrimination,Education
14,Loan Denial Algorithm Bias,2021-02-10,Major US Bank,Internal,Mortgage AI denied minorities,Discrimination,Finance
15,Medical Triage AI Failure,2023-05-12,NHS England,Babylon Health,Triage AI missed critical symptoms,Physical Harm,Healthcare
"""

# Write temp CSV
tmp_csv = Path(tempfile.mktemp(suffix=".csv"))
tmp_csv.write_text(CSV_CONTENT)
info(f"Test CSV created: {tmp_csv} (15 rows)")

# Try AIID-specific endpoint first
info("Trying /api/v1/aiid/ingest endpoint...")
try:
    r = requests.post(f"{BASE_URL}/api/v1/aiid/ingest",
        headers=HEADERS,
        json={
            "snapshot_url": "https://pub-72b2b2fc36ec423189843747af98f80e.r2.dev/backup-20260223102103.tar.bz2",
            "snapshot_date": "2026-02-23"
        },
        timeout=30
    )
    if r.status_code in (200, 201):
        DATASET_ID = (r.json().get("dataset_id") or
                      r.json().get("id"))
        passed(f"AIID ingest endpoint works — dataset_id: {DATASET_ID}")
    elif r.status_code == 404:
        warned("AIID ingest endpoint not built yet — using file upload")
    else:
        warned(f"AIID ingest returned {r.status_code} — using file upload")
except Exception as e:
    warned(f"AIID ingest failed: {e} — using file upload")

# Fallback: multipart file upload
if not DATASET_ID:
    info("Uploading AIID-format test CSV via /upload/file...")
    try:
        with open(tmp_csv, "rb") as f:
            r = requests.post(
                f"{BASE_URL}/api/v1/upload/file",
                headers=HEADERS,
                files={"file": ("aiid_test.csv", f, "text/csv")},
                data={"domain": "ai_incidents",
                      "name": "AIID Test Snapshot 2026-02-23"},
                timeout=30
            )
        data = r.json()
        DATASET_ID = (data.get("dataset_id") or
                      data.get("id") or
                      (data.get("data") or {}).get("dataset_id"))
        if DATASET_ID:
            passed(f"File upload succeeded — dataset_id: {DATASET_ID}")
        else:
            failed(f"Upload returned no dataset_id. Response: {data}")
            sys.exit(1)
    except Exception as e:
        failed(f"File upload failed: {e}")
        sys.exit(1)

tmp_csv.unlink(missing_ok=True)

# ================================================================
# PHASE 4 — PROCESSING PIPELINE
# ================================================================
section("PHASE 4 — Processing Pipeline")

info("Polling every 5s (max 120s)...")
status   = "unknown"
deadline = time.time() + 120

while time.time() < deadline:
    time.sleep(5)
    try:
        r = requests.get(
            f"{BASE_URL}/api/v1/datasets/{DATASET_ID}",
            headers=HEADERS, timeout=10
        )
        data     = r.json()
        status   = data.get("status", "unknown")
        progress = data.get("progress_pct", "?")
        elapsed  = int(120 - (deadline - time.time()))
        print(f"  {GRAY}[{elapsed}s] Status: {status} | Progress: {progress}%{RESET}")

        if status == "complete":
            passed(f"Processing completed in ~{elapsed}s")
            break
        elif status == "failed":
            err = data.get("error_message", "unknown error")
            failed(f"Processing FAILED: {err}")
            info("Check: docker-compose logs worker --tail=50")
            break
    except Exception as e:
        warned(f"Poll failed: {e}")

if status not in ("complete", "failed"):
    warned(f"Timeout — status still '{status}' after 120s")
    warned("Continuing tests anyway — endpoints may still respond")

try:
    r    = requests.get(f"{BASE_URL}/api/v1/datasets/{DATASET_ID}",
                        headers=HEADERS, timeout=10)
    data = r.json()
    info(f"Rows: {data.get('row_count')} | "
         f"Cols: {data.get('col_count')} | "
         f"Status: {data.get('status')}")
except Exception:
    pass

# ================================================================
# PHASE 5 — README GENERATION
# ================================================================
section("PHASE 5 — README Generation")

readme_md   = None
readme_html = None

# ── Test A: Markdown ─────────────────────────────────────────
info("Test A: Markdown README...")
try:
    r = requests.get(
        f"{BASE_URL}/api/v1/datasets/{DATASET_ID}/readme",
        params={"format": "markdown"},
        headers=HEADERS, timeout=30
    )
    if r.status_code == 200:
        readme_md = r.text
        passed(f"README endpoint OK "
               f"({len(readme_md):,} bytes, "
               f"{readme_md.count(chr(10))} lines)")
    elif r.status_code == 404:
        failed("README endpoint not found (404)")
        info("Add to datasets.py router:")
        info("  @router.get('/datasets/{dataset_id}/readme')")
    else:
        failed(f"README returned HTTP {r.status_code}: {r.text[:100]}")
except Exception as e:
    failed(f"README request failed: {e}")

# ── Validate content ──────────────────────────────────────────
if readme_md:
    checks = {
        "Has markdown headers":         readme_md.startswith("#") or "\n## " in readme_md,
        "Has Overview section":         "overview" in readme_md.lower(),
        "Has code block":               "```" in readme_md,
        "Has table (|)":                "|" in readme_md,
        "Has Data Quality section":     "quality" in readme_md.lower(),
        "Has Assumptions":              "assumption" in readme_md.lower(),
        "Has Limitations":              "limitation" in readme_md.lower(),
        "Has Citation":                 "citation" in readme_md.lower(),
        "Has Reproducibility section":  "reproduc" in readme_md.lower(),
        "Has AIID reference":           "incident" in readme_md.lower(),
        "Has McGregor/IAAI citation":   "mcgregor" in readme_md.lower() or "IAAI" in readme_md,
        "Has quality score":            "/100" in readme_md or "score" in readme_md.lower(),
        "Substantial (>500 chars)":     len(readme_md) > 500,
        "Enough lines (>20)":           readme_md.count("\n") > 20,
    }
    for label, result in checks.items():
        if result:
            passed(label)
        else:
            failed(label)

    # Save to temp
    tmp_readme = Path(tempfile.mktemp(suffix="_README.md"))
    tmp_readme.write_text(readme_md, encoding="utf-8")
    info(f"README saved: {tmp_readme}")
    preview(readme_md, lines=25, label="README Preview")

# ── Test B: HTML ──────────────────────────────────────────────
info("\nTest B: HTML README...")
try:
    r = requests.get(
        f"{BASE_URL}/api/v1/datasets/{DATASET_ID}/readme",
        params={"format": "html"},
        headers=HEADERS, timeout=30
    )
    if r.status_code == 200:
        readme_html = r.text
        passed(f"HTML README generated ({len(readme_html):,} bytes)")

        has_table   = "<table" in readme_html.lower()
        has_heading = any(f"<h{i}" in readme_html.lower() for i in range(1, 5))

        if has_table:   passed("HTML has <table> tags")
        else:           warned("HTML missing <table> tags")
        if has_heading: passed("HTML has heading tags")
        else:           warned("HTML missing heading tags")

        tmp_html = Path(tempfile.mktemp(suffix="_README.html"))
        tmp_html.write_text(readme_html, encoding="utf-8")
        info(f"HTML saved: {tmp_html}")
    else:
        warned(f"HTML README returned {r.status_code}")
except Exception as e:
    warned(f"HTML README failed: {e}")

# ================================================================
# PHASE 6 — NOTEBOOK TESTS
# ================================================================
section("PHASE 6 — Jupyter Notebook")

# ── Test A: File exists ───────────────────────────────────────
info("Test A: Notebook file...")
if NB_PATH.exists():
    size_kb = NB_PATH.stat().st_size / 1024
    passed(f"Notebook found ({size_kb:.1f} KB)")
else:
    failed(f"Notebook not found: {NB_PATH.resolve()}")
    info("Expected: ./notebooks/AIID_Research_Notebook.ipynb")

# ── Test B: Valid JSON + structure ────────────────────────────
info("Test B: Notebook structure...")
nb_json = None
if NB_PATH.exists():
    try:
        nb_json   = json.loads(NB_PATH.read_text(encoding="utf-8"))
        cells     = nb_json.get("cells", [])
        code      = [c for c in cells if c["cell_type"] == "code"]
        md        = [c for c in cells if c["cell_type"] == "markdown"]
        all_src   = " ".join("".join(c.get("source", [])) for c in cells)

        passed(f"Valid JSON — {len(cells)} cells "
               f"({len(code)} code, {len(md)} markdown)")

        if len(cells) >= 15:
            passed(f"Sufficient cells ({len(cells)})")
        else:
            warned(f"Only {len(cells)} cells — expected 15+")

        content_checks = {
            "SNAPSHOT_URL config":       "SNAPSHOT_URL" in all_src,
            "Download function":         "download" in all_src.lower(),
            "load_snapshot function":    "load_snapshot" in all_src or "load_from" in all_src,
            "plot_ functions":           "def plot_" in all_src,
            "show_chart helper":         "show_chart" in all_src,
            "Assumptions section":       "assumption" in all_src.lower(),
            "Citation present":          "mcgregor" in all_src.lower() or "IAAI" in all_src,
            "PS5 relevance banner":      "PS5" in all_src or "problem statement" in all_src.lower(),
            "AI insights cell":          "AI_API_KEY" in all_src,
            "Summary report export":     "AIID_Summary_Report" in all_src,
            "Windows Agg backend fix":   "Agg" in all_src or "agg" in all_src,
        }
        for label, ok in content_checks.items():
            if ok: passed(label)
            else:  warned(f"Missing: {label}")

    except json.JSONDecodeError as e:
        failed(f"Notebook is not valid JSON: {e}")

# ── Test C: Dependencies ──────────────────────────────────────
info("Test C: Python dependencies...")
deps = ["pandas", "matplotlib", "seaborn", "requests", "tqdm", "numpy"]
for dep in deps:
    try:
        result = subprocess.run(
            [sys.executable, "-c",
             f"import {dep}; print({dep}.__version__)"],
            capture_output=True, text=True, timeout=5
        )
        if result.returncode == 0:
            passed(f"{dep} {result.stdout.strip()}")
        else:
            warned(f"{dep} not installed — run: pip install {dep}")
    except Exception:
        warned(f"Cannot check {dep}")

# ── Test D: Headless execution ────────────────────────────────
info("Test D: Headless notebook execution...")

nbconvert_ok = subprocess.run(
    [sys.executable, "-m", "jupyter", "nbconvert", "--version"],
    capture_output=True
).returncode == 0

if not nbconvert_ok:
    warned("nbconvert not installed — run: pip install nbconvert")
    info("Skipping headless execution test")
elif NB_PATH.exists():
    out_path = Path(tempfile.mktemp(suffix="_executed.ipynb"))
    info(f"Executing notebook (timeout=360s)...")
    info("This downloads AIID data — takes 2-5 minutes")

    start = time.time()
    proc = subprocess.run(
        [sys.executable, "-m", "jupyter", "nbconvert",
         "--to", "notebook",
         "--execute",
         "--ExecutePreprocessor.timeout=360",
         "--ExecutePreprocessor.kernel_name=python3",
         "--output", str(out_path),
         str(NB_PATH.resolve())],
        capture_output=True, text=True, timeout=400
    )
    elapsed = int(time.time() - start)

    if proc.returncode == 0:
        passed(f"Notebook executed successfully ({elapsed}s)")
    else:
        failed(f"Execution failed (exit {proc.returncode})")
        # Show last 20 lines of error
        err_lines = proc.stderr.strip().split("\n")
        for line in err_lines[-20:]:
            print(f"  {RED}{line}{RESET}")

    # Check executed notebook for errors
    if out_path.exists():
        executed = json.loads(out_path.read_text(encoding="utf-8"))
        errored  = []
        outputs_found = 0
        for i, cell in enumerate(executed.get("cells", [])):
            if cell["cell_type"] != "code":
                continue
            outs = cell.get("outputs", [])
            outputs_found += len(outs)
            for out in outs:
                if out.get("output_type") == "error":
                    errored.append({
                        "cell": i + 1,
                        "name": out.get("ename"),
                        "msg":  out.get("evalue", "")[:80]
                    })

        info(f"Total cell outputs: {outputs_found}")
        if errored:
            failed(f"{len(errored)} cells had errors:")
            for e in errored:
                print(f"    Cell {e['cell']}: {e['name']}: {e['msg']}")
        else:
            passed("No cell errors in executed notebook")

        # Convert to HTML
        html_out = Path(tempfile.mktemp(suffix="_report.html"))
        subprocess.run(
            [sys.executable, "-m", "jupyter", "nbconvert",
             "--to", "html", "--no-input",
             str(out_path), "--output", str(html_out)],
            capture_output=True
        )
        if html_out.exists():
            sz = html_out.stat().st_size / 1024
            passed(f"HTML report generated ({sz:.1f} KB): {html_out}")
        else:
            warned("HTML export failed")

# ── Test E: Output files ──────────────────────────────────────
info("Test E: Output file check...")
cache_dir = NB_PATH.parent / "aiid_cache"
expected  = [
    "fig_01_incidents_per_year.png",
    "fig_incidents_by_harm_type.png",
    "fig_heatmap_harm_sector.png",
    "fig_harm_trend_over_time.png",
    "AIID_Summary_Report.txt",
]

if cache_dir.exists():
    all_files = list(cache_dir.iterdir())
    passed(f"aiid_cache has {len(all_files)} files")
    for fname in expected:
        fp = cache_dir / fname
        if fp.exists():
            sz = fp.stat().st_size
            if sz > 100:
                passed(f"{fname} ({sz/1024:.1f} KB)")
            else:
                warned(f"{fname} exists but tiny ({sz} bytes)")
        else:
            warned(f"{fname} not yet created — run notebook first")
else:
    warned("aiid_cache/ not created — run notebook first")

# ================================================================
# PHASE 7 — PLATFORM vs NOTEBOOK CROSS-CHECK
# ================================================================
section("PHASE 7 — Platform vs Notebook Cross-Check")

info("Checking platform-side analysis for same dataset...")

for endpoint, label in [
    (f"/api/v1/datasets/{DATASET_ID}/eda",     "EDA report"),
    (f"/api/v1/datasets/{DATASET_ID}/reports", "Agent reports"),
    (f"/api/v1/datasets/{DATASET_ID}/job",     "Job status"),
]:
    try:
        r = requests.get(f"{BASE_URL}{endpoint}",
                         headers=HEADERS, timeout=15)
        if r.status_code == 200:
            data = r.json()
            if isinstance(data, list):
                passed(f"{label}: {len(data)} items")
                for item in data[:3]:
                    name  = item.get("agent_name", "")
                    model = item.get("model_used", "")
                    if name:
                        info(f"  - {name} ({model})")
            else:
                passed(f"{label} available")
                score = (data.get("json_summary") or {}).get("dataset_quality_score")
                if score:
                    info(f"  Quality score: {score}/100")
        elif r.status_code == 404:
            warned(f"{label} endpoint not found")
        else:
            warned(f"{label} returned {r.status_code}")
    except Exception as e:
        warned(f"{label} check failed: {e}")

# ================================================================
# FINAL SUMMARY
# ================================================================
section("FINAL RESULTS")

total = results["pass"] + results["fail"] + results["warn"]
print(f"\n  {GREEN}PASSED  : {results['pass']}{RESET}")
print(f"  {RED}FAILED  : {results['fail']}{RESET}")
print(f"  {YELLOW}WARNINGS: {results['warn']}{RESET}")
print()

if results["fail"] == 0 and results["pass"] > 0:
    print(f"  {GREEN}{BOLD}ALL TESTS PASSED{RESET}")
    print(f"  {GREEN}README and Notebook generation verified{RESET}")
elif results["fail"] > 0:
    print(f"  {RED}Fix {results['fail']} failures above then re-run{RESET}")
    print(f"  Share this output for targeted help")
else:
    print(f"  {YELLOW}No tests ran — check your setup{RESET}")

print()
print(f"  {GRAY}Dataset ID used: {DATASET_ID}{RESET}")
print(f"  {GRAY}Run at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}{RESET}")
print(f"  {GRAY}{'='*60}{RESET}\n")
