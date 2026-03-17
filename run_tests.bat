@echo off
REM ================================================================
REM  DataSage Quick Test Launcher
REM  Double-click this file from datasage\ directory
REM  OR run: .\run_tests.bat
REM ================================================================

echo.
echo  DataSage ^| README + Notebook Generation Tests
echo  ================================================
echo.

REM Check we're in the right directory
if not exist "docker-compose.yml" (
    echo  [ERROR] Not in datasage root directory
    echo  Move this file to your datasage\ folder and retry
    pause
    exit /b 1
)

REM Check Python
python --version >nul 2>&1
if %errorlevel% neq 0 (
    echo  [ERROR] Python not found
    echo  Install from https://python.org
    pause
    exit /b 1
)
echo  [OK] Python found

REM Check Docker is running
docker info >nul 2>&1
if %errorlevel% neq 0 (
    echo  [ERROR] Docker not running
    echo  Start Docker Desktop and retry
    pause
    exit /b 1
)
echo  [OK] Docker running

REM Check API
curl -s http://localhost:8000/health >nul 2>&1
if %errorlevel% neq 0 (
    echo  [WARN] API not responding - starting docker-compose...
    docker-compose up -d
    echo  Waiting 10 seconds for services...
    timeout /t 10 /nobreak >nul
)
echo  [OK] API reachable

echo.
echo  Choose test to run:
echo  [1] Full test suite (README + Notebook + Platform)
echo  [2] README only (fast, ~30 seconds)
echo  [3] Notebook structure check (no execution, fast)
echo  [4] Run notebook headlessly (slow, 3-5 min)
echo  [Q] Quit
echo.

set /p CHOICE="  Enter choice (1/2/3/4/Q): "

if /i "%CHOICE%"=="Q" exit /b 0

if "%CHOICE%"=="1" (
    echo.
    echo  Running full test suite...
    python test_generation.py
    goto END
)

if "%CHOICE%"=="2" (
    echo.
    echo  Running README tests only...
    python -c "
import requests, sys

BASE = 'http://localhost:8000'
EMAIL = 'test@datasage.com'
PASS  = 'testpass123'

# Login
r = requests.post(f'{BASE}/api/v1/auth/login',
    json={'email': EMAIL, 'password': PASS})
token = r.json().get('access_token')
if not token:
    print('Login failed:', r.text)
    sys.exit(1)
print('[OK] Logged in')

headers = {'Authorization': f'Bearer {token}'}

# List datasets, pick first complete one
r = requests.get(f'{BASE}/api/v1/datasets/', headers=headers)
datasets = [d for d in r.json() if d.get('status') == 'complete']

if not datasets:
    print('[WARN] No complete datasets found')
    print('       Upload a dataset first via the UI or run full test suite')
    sys.exit(0)

did = datasets[0]['id']
name = datasets[0].get('name', 'Unknown')
print(f'[OK] Using dataset: {name} ({did})')

# Fetch README
r = requests.get(f'{BASE}/api/v1/datasets/{did}/readme',
    params={'format': 'markdown'}, headers=headers)
if r.status_code == 200:
    print(f'[PASS] README generated ({len(r.text):,} bytes)')
    print()
    print('--- First 20 lines ---')
    for line in r.text.split(chr(10))[:20]:
        print(' ', line)
else:
    print(f'[FAIL] README returned {r.status_code}: {r.text[:200]}')
"
    goto END
)

if "%CHOICE%"=="3" (
    echo.
    echo  Checking notebook structure...
    python -c "
import json
from pathlib import Path

nb_path = Path('./notebooks/AIID_Research_Notebook.ipynb')
if not nb_path.exists():
    print(f'[FAIL] Not found: {nb_path.resolve()}')
    exit(1)

nb   = json.loads(nb_path.read_text(encoding='utf-8'))
cells = nb.get('cells', [])
code  = [c for c in cells if c['cell_type'] == 'code']
md    = [c for c in cells if c['cell_type'] == 'markdown']
src   = ' '.join(''.join(c.get('source',[])) for c in cells)

print(f'[OK] Notebook found: {nb_path}')
print(f'     Cells: {len(cells)} total ({len(code)} code, {len(md)} markdown)')
print()

checks = {
    'SNAPSHOT_URL':       'SNAPSHOT_URL' in src,
    'show_chart':         'show_chart' in src,
    'def plot_':          'def plot_' in src,
    'Assumptions':        'assumption' in src.lower(),
    'PS5 banner':         'PS5' in src or 'problem statement' in src.lower(),
    'AI_API_KEY':         'AI_API_KEY' in src,
    'Agg backend':        'Agg' in src,
    'Citation':           'mcgregor' in src.lower() or 'IAAI' in src,
}

for name, ok in checks.items():
    print(f'  {\"[PASS]\" if ok else \"[WARN]\"} {name}')
"
    goto END
)

if "%CHOICE%"=="4" (
    echo.
    echo  Running notebook headlessly - this takes 3-5 minutes...
    echo  (Downloads live AIID data from incidentdatabase.ai)
    echo.
    jupyter nbconvert ^
        --to notebook ^
        --execute ^
        --ExecutePreprocessor.timeout=360 ^
        --ExecutePreprocessor.kernel_name=python3 ^
        --output "%TEMP%\AIID_Executed.ipynb" ^
        "notebooks\AIID_Research_Notebook.ipynb"

    if %errorlevel% equ 0 (
        echo.
        echo  [PASS] Notebook executed successfully
        echo  Converting to HTML report...
        jupyter nbconvert --to html --no-input ^
            "%TEMP%\AIID_Executed.ipynb" ^
            --output "%TEMP%\AIID_Report.html"
        echo  [OK] Report saved: %TEMP%\AIID_Report.html
        echo  Opening report...
        start "" "%TEMP%\AIID_Report.html"
    ) else (
        echo  [FAIL] Notebook execution failed
        echo  Check the error output above
    )
    goto END
)

echo  Invalid choice: %CHOICE%

:END
echo.
echo  ================================================
echo  Done. Press any key to exit.
pause >nul
