# ================================================================
#  DataSage — README + Notebook Generation Test Script
#  Platform : Windows PowerShell
#  Run from : C:\Users\SHREY PATEL\TAI2.0\datasage\
#  Usage    : .\test_datasage_generation.ps1
# ================================================================

$BASE_URL  = "http://localhost:8000"
$EMAIL     = "test@datasage.com"
$PASSWORD  = "testpass123"
$PASS      = 0
$FAIL      = 0
$WARNINGS  = 0

# ── Helpers ──────────────────────────────────────────────────
function Pass($msg) {
    Write-Host "  [PASS] $msg" -ForegroundColor Green
    $script:PASS++
}
function Fail($msg) {
    Write-Host "  [FAIL] $msg" -ForegroundColor Red
    $script:FAIL++
}
function Warn($msg) {
    Write-Host "  [WARN] $msg" -ForegroundColor Yellow
    $script:WARNINGS++
}
function Info($msg) {
    Write-Host "  [INFO] $msg" -ForegroundColor Cyan
}
function Section($msg) {
    Write-Host ""
    Write-Host "================================================================" -ForegroundColor DarkGray
    Write-Host "  $msg" -ForegroundColor White
    Write-Host "================================================================" -ForegroundColor DarkGray
}

# ================================================================
# PHASE 1 — INFRASTRUCTURE
# ================================================================
Section "PHASE 1 — Infrastructure Health"

# API
try {
    $r = Invoke-RestMethod -Uri "$BASE_URL/health" -Method GET -TimeoutSec 5
    if ($r.status -eq "ok") { Pass "API is up" }
    else { Fail "API health returned: $($r.status)" }
} catch {
    Fail "API unreachable — is docker-compose up? $_"
    Write-Host "  Run: docker-compose up -d" -ForegroundColor Yellow
    exit 1
}

# DB
try {
    $r = Invoke-RestMethod -Uri "$BASE_URL/api/v1/health/db" -Method GET -TimeoutSec 5
    if ($r.database -eq "connected") { Pass "Database connected" }
    else { Fail "DB status: $($r.database)" }
} catch { Warn "DB health endpoint not found — skipping" }

# Redis via docker
try {
    $ping = docker-compose exec -T redis redis-cli ping 2>$null
    if ($ping -match "PONG") { Pass "Redis responding" }
    else { Warn "Redis ping unexpected: $ping" }
} catch { Warn "Cannot check Redis directly" }

# MinIO
try {
    $null = Invoke-WebRequest -Uri "http://localhost:9000/minio/health/live" `
        -TimeoutSec 5 -UseBasicParsing
    Pass "MinIO healthy"
} catch { Warn "MinIO health check failed (may still work)" }

# Celery worker
try {
    $workers = docker-compose exec -T worker `
        celery -A celery_app inspect active 2>$null
    if ($workers -match "celery") { Pass "Celery worker active" }
    else { Warn "Celery worker status unclear" }
} catch { Warn "Cannot inspect Celery directly" }

# ================================================================
# PHASE 2 — AUTHENTICATION
# ================================================================
Section "PHASE 2 — Authentication"

# Register
try {
    $body = @{ email=$EMAIL; password=$PASSWORD; name="Test User" } | ConvertTo-Json
    $r = Invoke-RestMethod -Uri "$BASE_URL/api/v1/auth/register" `
        -Method POST -Body $body -ContentType "application/json" -TimeoutSec 10
    Pass "Register endpoint works"
} catch {
    $code = $_.Exception.Response.StatusCode.value__
    if ($code -eq 400) { Info "User already registered — continuing" }
    elseif ($code -eq 404) {
        Fail "Auth route not found (404) — add auth router to main.py"
        Write-Host "  Fix: app.include_router(auth.router, prefix='/api/v1')" -ForegroundColor Yellow
    } else {
        Fail "Register failed: $_"
    }
}

# Login
$TOKEN = $null
try {
    $body = @{ email=$EMAIL; password=$PASSWORD } | ConvertTo-Json
    $r = Invoke-RestMethod -Uri "$BASE_URL/api/v1/auth/login" `
        -Method POST -Body $body -ContentType "application/json" -TimeoutSec 10
    $TOKEN = $r.access_token
    if (-not $TOKEN) { $TOKEN = $r.token }
    if (-not $TOKEN) { $TOKEN = $r.data.access_token }

    if ($TOKEN) { Pass "Login succeeded — token obtained" }
    else {
        Fail "Login returned no token. Response: $($r | ConvertTo-Json)"
        exit 1
    }
} catch {
    Fail "Login failed: $_"
    exit 1
}

$HEADERS = @{ Authorization = "Bearer $TOKEN" }

# ================================================================
# PHASE 3 — AIID DATA INGESTION
# ================================================================
Section "PHASE 3 — AIID Data Ingestion"

# Create test CSV that mimics AIID format
$CSV_PATH = "$env:TEMP\aiid_test.csv"
$csvContent = @"
incident_id,title,date,allegedDeployerOfAISystem,allegedDeveloperOfAISystem,description,harm_type,sector_of_deployment
1,Autonomous Vehicle Pedestrian Strike,2019-03-18,Uber,Uber ATG,Self-driving car struck pedestrian,Physical Harm,Transportation
2,Facial Recognition False Arrest,2020-06-24,Detroit Police Dept,Amazon,Man wrongfully arrested by facial recognition error,Harm to Civil Liberties,Law Enforcement
3,Hiring Algorithm Gender Bias,2018-10-10,Amazon,Amazon,AI recruiting penalized women candidates,Discrimination,Employment
4,Trading Algorithm Flash Crash,2010-05-06,Multiple Firms,Multiple,Algorithms caused trillion dollar market crash,Financial Harm,Finance
5,Deepfake Political Video,2023-01-15,Unknown,Unknown,AI video spread election misinformation,Harm to Social Systems,Politics
6,Healthcare AI Misdiagnosis,2021-07-20,Hospital Network,IBM Watson Health,AI recommended incorrect cancer treatments,Physical Harm,Healthcare
7,Chatbot Suicide Encouragement,2023-03-28,Character.ai,Character.ai,Chatbot encouraged self-harm to teen,Psychological Harm,Entertainment
8,Credit Scoring Racial Bias,2019-11-01,Apple Card,Goldman Sachs,Algorithm gave lower limits to minorities,Discrimination,Finance
9,Predictive Policing Overreach,2020-08-14,Chicago PD,ShotSpotter,Predictive system led to wrongful stops,Harm to Civil Liberties,Law Enforcement
10,Content Moderation Failure,2022-05-03,Facebook,Meta,Algorithm amplified hate speech,Harm to Social Systems,Social Media
11,Autonomous Weapon Misidentification,2021-03-25,Military,Defense Contractor,Weapon misidentified civilian vehicle,Physical Harm,Military
12,Biometric Surveillance Abuse,2022-09-14,Government Agency,NEC Corporation,Facial recognition tracked protesters,Harm to Civil Liberties,Government
13,Student Exam AI Bias,2020-08-20,University Systems,Proctorio,Exam AI flagged minorities at higher rates,Discrimination,Education
14,Loan Denial Algorithm Bias,2021-02-10,Major US Bank,Internal,Mortgage AI denied minority applicants,Discrimination,Finance
15,Medical Triage AI Failure,2023-05-12,NHS England,Babylon Health,Triage AI missed critical emergency symptoms,Physical Harm,Healthcare
"@
$csvContent | Set-Content -Path $CSV_PATH -Encoding UTF8
Info "Test CSV created at $CSV_PATH (15 rows)"

# Try AIID-specific ingest endpoint first
$DATASET_ID = $null
Info "Trying AIID-specific ingest endpoint..."
try {
    $body = @{
        snapshot_url  = "https://pub-72b2b2fc36ec423189843747af98f80e.r2.dev/backup-20260223102103.tar.bz2"
        snapshot_date = "2026-02-23"
    } | ConvertTo-Json
    $r = Invoke-RestMethod -Uri "$BASE_URL/api/v1/aiid/ingest" `
        -Method POST -Headers $HEADERS -Body $body `
        -ContentType "application/json" -TimeoutSec 30
    $DATASET_ID = $r.dataset_id
    Pass "AIID ingest endpoint works — dataset_id: $DATASET_ID"
} catch {
    $code = $_.Exception.Response.StatusCode.value__
    if ($code -eq 404) {
        Warn "AIID ingest endpoint not built yet — falling back to file upload"
    } else {
        Warn "AIID ingest failed ($code) — falling back to file upload"
    }
}

# Fallback: upload test CSV
if (-not $DATASET_ID) {
    Info "Uploading AIID-format test CSV..."
    try {
        # Use curl for multipart (more reliable than PowerShell for file upload)
        $curlOutput = & curl -s -X POST "$BASE_URL/api/v1/upload/file" `
            -H "Authorization: Bearer $TOKEN" `
            -F "file=@$CSV_PATH" `
            -F "domain=ai_incidents" `
            -F "name=AIID Test Snapshot 2026-02-23" 2>&1

        $uploadData = $curlOutput | ConvertFrom-Json
        $DATASET_ID = $uploadData.dataset_id
        if (-not $DATASET_ID) { $DATASET_ID = $uploadData.id }
        if (-not $DATASET_ID) { $DATASET_ID = $uploadData.data.dataset_id }

        if ($DATASET_ID) { Pass "File upload succeeded — dataset_id: $DATASET_ID" }
        else {
            Fail "Upload returned no dataset_id. Response: $curlOutput"
            exit 1
        }
    } catch {
        Fail "File upload failed: $_"
        exit 1
    }
}

$JOB_ID = $null
try {
    $r = Invoke-RestMethod -Uri "$BASE_URL/api/v1/datasets/$DATASET_ID" `
        -Headers $HEADERS -TimeoutSec 10
    $JOB_ID = $r.job_id
    if (-not $JOB_ID) { $JOB_ID = $r.data.job_id }
    if ($JOB_ID) { Info "Job ID: $JOB_ID" }
} catch {}

# ================================================================
# PHASE 4 — WAIT FOR PROCESSING
# ================================================================
Section "PHASE 4 — Processing Pipeline"

Info "Polling status every 5 seconds (max 90s)..."
$STATUS = "unknown"
$ELAPSED = 0

for ($i = 1; $i -le 18; $i++) {
    Start-Sleep -Seconds 5
    $ELAPSED += 5
    try {
        $r = Invoke-RestMethod -Uri "$BASE_URL/api/v1/datasets/$DATASET_ID" `
            -Headers $HEADERS -TimeoutSec 10
        $STATUS   = $r.status
        $PROGRESS = $r.progress_pct
        if (-not $PROGRESS) { $PROGRESS = "?" }
        Write-Host "  [${ELAPSED}s] Status: $STATUS | Progress: $PROGRESS%" -ForegroundColor DarkGray

        if ($STATUS -eq "complete") {
            Pass "Processing completed in ${ELAPSED}s"
            break
        } elseif ($STATUS -eq "failed") {
            $ERR = $r.error_message
            Fail "Processing FAILED — $ERR"
            Info "Check worker logs: docker-compose logs worker --tail=50"
            break
        }
    } catch {
        Warn "Status poll failed: $_"
    }
}

if ($STATUS -ne "complete" -and $STATUS -ne "failed") {
    Warn "Timeout after 90s — status still: $STATUS"
    Warn "Continue anyway — endpoints may still respond"
}

# Final dataset info
try {
    $DATASET = Invoke-RestMethod -Uri "$BASE_URL/api/v1/datasets/$DATASET_ID" `
        -Headers $HEADERS -TimeoutSec 10
    Info "Dataset rows   : $($DATASET.row_count)"
    Info "Dataset cols   : $($DATASET.col_count)"
    Info "Final status   : $($DATASET.status)"
} catch {}

# ================================================================
# PHASE 5 — README GENERATION TESTS
# ================================================================
Section "PHASE 5 — README Generation"

$README_MD_PATH   = "$env:TEMP\datasage_readme.md"
$README_HTML_PATH = "$env:TEMP\datasage_readme.html"

# Test A: Markdown README
Info "Test A: Fetching README as Markdown..."
try {
    $r = Invoke-WebRequest `
        -Uri "$BASE_URL/api/v1/datasets/$DATASET_ID/readme?format=markdown" `
        -Headers $HEADERS -TimeoutSec 30 -UseBasicParsing
    $r.Content | Set-Content -Path $README_MD_PATH -Encoding UTF8
    Pass "README endpoint responded (HTTP $($r.StatusCode))"
} catch {
    Fail "README endpoint failed: $_"
    Info "Make sure /app/api/v1/routes/datasets.py has the /readme endpoint"
}

# Validate markdown content
if (Test-Path $README_MD_PATH) {
    $content = Get-Content $README_MD_PATH -Raw -Encoding UTF8
    $sizeKB  = [math]::Round((Get-Item $README_MD_PATH).Length / 1KB, 1)
    $lines   = ($content -split "`n").Count

    Info "README size: $sizeKB KB | Lines: $lines"

    $checks = @{
        "Has markdown headers (##)"     = $content -match "^#{1,3} " 
        "Has Overview section"          = $content -imatch "overview"
        "Has code block (```)"           = $content -match "``````"
        "Has table (|)"                 = $content -match "\|.+\|"
        "Has Data Quality section"      = $content -imatch "quality"
        "Has Assumptions section"       = $content -imatch "assumption"
        "Has Limitations section"       = $content -imatch "limitation"
        "Has Citation section"          = $content -imatch "citation"
        "Has Reproducibility section"   = $content -imatch "reproduc"
        "Has AIID/Incident reference"   = $content -imatch "incident database|AIID"
        "Has McGregor or IAAI citation" = $content -imatch "mcgregor|IAAI-21"
        "Has quality score"             = $content -imatch "/100|score"
        "Substantial length (>500B)"    = $content.Length -gt 500
        "Enough lines (>20)"            = $lines -gt 20
    }

    foreach ($check in $checks.GetEnumerator()) {
        if ($check.Value) { Pass $check.Key }
        else { Fail $check.Key }
    }
} else {
    Fail "README file not saved — endpoint likely failed"
}

# Test B: HTML README
Info ""
Info "Test B: Fetching README as HTML..."
try {
    $r = Invoke-WebRequest `
        -Uri "$BASE_URL/api/v1/datasets/$DATASET_ID/readme?format=html" `
        -Headers $HEADERS -TimeoutSec 30 -UseBasicParsing
    $r.Content | Set-Content -Path $README_HTML_PATH -Encoding UTF8
    $htmlSize = [math]::Round((Get-Item $README_HTML_PATH).Length / 1KB, 1)

    Pass "HTML README generated ($htmlSize KB)"

    $htmlContent = Get-Content $README_HTML_PATH -Raw
    if ($htmlContent -match "<table") { Pass "HTML has table tags" }
    else { Warn "HTML has no table tags — check markdown renderer" }

    if ($htmlContent -match "<h[1-3]") { Pass "HTML has heading tags" }
    else { Warn "HTML has no heading tags" }

} catch {
    Fail "HTML README failed: $_"
}

# Test C: Download README
Info ""
Info "Test C: Download README as file attachment..."
try {
    $r = Invoke-WebRequest `
        -Uri "$BASE_URL/api/v1/datasets/$DATASET_ID/readme" `
        -Headers $HEADERS -TimeoutSec 30 -UseBasicParsing
    $cd = $r.Headers["Content-Disposition"]
    if ($cd -and $cd -match "attachment") { Pass "Content-Disposition: attachment set" }
    else { Warn "Content-Disposition not set — file won't auto-download in browser" }
} catch {}

# Show preview
if (Test-Path $README_MD_PATH) {
    Write-Host ""
    Write-Host "  ── README Preview (first 30 lines) ────────────────" -ForegroundColor DarkGray
    Get-Content $README_MD_PATH -TotalCount 30 | ForEach-Object {
        Write-Host "  $_" -ForegroundColor Gray
    }
    Write-Host "  [... rest of README saved at $README_MD_PATH]" -ForegroundColor DarkGray
}

# ================================================================
# PHASE 6 — NOTEBOOK GENERATION TESTS
# ================================================================
Section "PHASE 6 — Jupyter Notebook Generation"

$NB_PATH        = ".\notebooks\AIID_Research_Notebook.ipynb"
$NB_EXEC_PATH   = "$env:TEMP\AIID_Executed.ipynb"
$NB_HTML_PATH   = "$env:TEMP\AIID_Report.html"
$NB_REPORT_PATH = ".\notebooks\aiid_cache\AIID_Summary_Report.txt"

# Test A: Notebook file exists
Info "Test A: Notebook file check..."
if (Test-Path $NB_PATH) {
    $nbSize = [math]::Round((Get-Item $NB_PATH).Length / 1KB, 1)
    Pass "Notebook file found ($nbSize KB)"
} else {
    Fail "AIID_Research_Notebook.ipynb not found at .\notebooks\"
    Info "Expected path: $((Resolve-Path '.\notebooks\' -ErrorAction SilentlyContinue).Path)"
}

# Test B: Valid JSON
if (Test-Path $NB_PATH) {
    Info "Test B: Notebook JSON validation..."
    try {
        $nbContent  = Get-Content $NB_PATH -Raw -Encoding UTF8
        $nbJson     = $nbContent | ConvertFrom-Json
        $cells      = $nbJson.cells
        $codeCells  = $cells | Where-Object { $_.cell_type -eq "code" }
        $mdCells    = $cells | Where-Object { $_.cell_type -eq "markdown" }

        Pass "Valid JSON notebook"
        Info "  Total cells    : $($cells.Count)"
        Info "  Code cells     : $($codeCells.Count)"
        Info "  Markdown cells : $($mdCells.Count)"

        if ($cells.Count -ge 15) { Pass "Sufficient cells ($($cells.Count) >= 15)" }
        else { Warn "Only $($cells.Count) cells — expected 15+" }

        # Check required content
        $allSource = ($cells | ForEach-Object { $_.source -join "" }) -join " "

        $contentChecks = @{
            "Has SNAPSHOT_URL config"    = $allSource -match "SNAPSHOT_URL"
            "Has download function"      = $allSource -imatch "download|fetch"
            "Has load_snapshot function" = $allSource -imatch "load_snapshot|load_from"
            "Has plot functions"         = $allSource -imatch "def plot_"
            "Has show_chart helper"      = $allSource -imatch "show_chart"
            "Has Assumptions section"    = $allSource -imatch "assumption"
            "Has Citation"               = $allSource -imatch "mcgregor|IAAI|citation"
            "Has PS5 relevance banner"   = $allSource -imatch "PS5|problem statement|PS 5"
            "Has AI insights cell"       = $allSource -imatch "AI_API_KEY|generate_ai_insights"
            "Has summary report export"  = $allSource -imatch "summary_report|AIID_Summary_Report"
        }
        foreach ($c in $contentChecks.GetEnumerator()) {
            if ($c.Value) { Pass $c.Key }
            else { Warn "Missing: $($c.Key)" }
        }
    } catch {
        Fail "Notebook is not valid JSON: $_"
    }
}

# Test C: Dependencies installed
Info ""
Info "Test C: Python dependencies..."
$deps = @("pandas", "matplotlib", "seaborn", "requests", "tqdm", "numpy")
foreach ($dep in $deps) {
    try {
        $result = python3 -c "import $dep; print($dep.__version__)" 2>&1
        if ($LASTEXITCODE -eq 0) { Pass "$dep installed ($result)" }
        else {
            Warn "$dep not installed — run: pip install $dep"
        }
    } catch {
        Warn "Cannot check $dep"
    }
}

# Test D: Execute notebook headlessly
Info ""
Info "Test D: Headless notebook execution..."
Info "(This downloads AIID data — takes 2-5 minutes)"
Info "Press Ctrl+C to skip and run manually later"

$nbExecAvailable = $false
try {
    $null = jupyter nbconvert --version 2>&1
    if ($LASTEXITCODE -eq 0) { $nbExecAvailable = $true }
} catch {}

if ($nbExecAvailable -and (Test-Path $NB_PATH)) {
    try {
        $proc = Start-Process -FilePath "jupyter" -ArgumentList @(
            "nbconvert",
            "--to", "notebook",
            "--execute",
            "--ExecutePreprocessor.timeout=360",
            "--ExecutePreprocessor.kernel_name=python3",
            "--output", $NB_EXEC_PATH,
            (Resolve-Path $NB_PATH).Path
        ) -NoNewWindow -Wait -PassThru -RedirectStandardOutput "$env:TEMP\nb_stdout.txt" `
          -RedirectStandardError "$env:TEMP\nb_stderr.txt"

        if ($proc.ExitCode -eq 0) {
            Pass "Notebook executed successfully"
        } else {
            $errOutput = Get-Content "$env:TEMP\nb_stderr.txt" -Raw -ErrorAction SilentlyContinue
            Fail "Notebook execution failed (exit $($proc.ExitCode))"
            Info "Error output:"
            Write-Host $errOutput -ForegroundColor Red
        }
    } catch {
        Fail "nbconvert execution error: $_"
    }
} elseif (-not $nbExecAvailable) {
    Warn "jupyter nbconvert not found"
    Warn "Install: pip install nbconvert  then re-run"
} else {
    Warn "Skipping execution — notebook file not found"
}

# Test E: Check output files were created
Info ""
Info "Test E: Output file verification..."
$cacheDir = ".\notebooks\aiid_cache"

if (Test-Path $cacheDir) {
    $outputFiles = Get-ChildItem $cacheDir -File
    if ($outputFiles.Count -gt 0) {
        Pass "aiid_cache directory has $($outputFiles.Count) output files"
        foreach ($f in $outputFiles) {
            $sizeKB = [math]::Round($f.Length / 1KB, 1)
            $icon   = if ($f.Length -gt 100) { "[OK]" } else { "[EMPTY]" }
            Write-Host "    $icon $($f.Name) ($sizeKB KB)" -ForegroundColor Gray
        }
    } else {
        Warn "aiid_cache exists but is empty — run the notebook first"
    }
} else {
    Warn "aiid_cache not created yet — notebook hasn't run successfully"
}

$expectedOutputs = @(
    ".\notebooks\aiid_cache\fig_01_incidents_per_year.png",
    ".\notebooks\aiid_cache\fig_incidents_by_harm_type.png",
    ".\notebooks\aiid_cache\fig_heatmap_harm_sector.png",
    ".\notebooks\aiid_cache\fig_harm_trend_over_time.png",
    ".\notebooks\aiid_cache\AIID_Summary_Report.txt"
)

foreach ($f in $expectedOutputs) {
    $name = Split-Path $f -Leaf
    if (Test-Path $f) {
        $sz = [math]::Round((Get-Item $f).Length / 1KB, 1)
        if ((Get-Item $f).Length -gt 100) { Pass "$name ($sz KB)" }
        else { Warn "$name exists but tiny ($sz KB) — may be corrupted" }
    } else {
        Warn "$name not yet generated (run notebook first)"
    }
}

# Test F: Convert to HTML report
if (Test-Path $NB_EXEC_PATH) {
    Info ""
    Info "Test F: HTML report export..."
    try {
        & jupyter nbconvert --to html --no-input $NB_EXEC_PATH `
            --output $NB_HTML_PATH 2>$null
        if (Test-Path $NB_HTML_PATH) {
            $sz = [math]::Round((Get-Item $NB_HTML_PATH).Length / 1KB, 1)
            Pass "HTML report generated ($sz KB) at $NB_HTML_PATH"
            Info "Open in browser: start $NB_HTML_PATH"
        } else {
            Fail "HTML export failed — file not created"
        }
    } catch {
        Warn "HTML export failed: $_"
    }
}

# ================================================================
# PHASE 7 — CROSS-CHECK: NOTEBOOK vs PLATFORM
# ================================================================
Section "PHASE 7 — Cross-Check: Notebook vs Platform"

Info "Comparing notebook analysis vs DataSage platform output..."

# Check if EDA report exists for dataset
try {
    $eda = Invoke-RestMethod `
        -Uri "$BASE_URL/api/v1/datasets/$DATASET_ID/eda" `
        -Headers $HEADERS -TimeoutSec 15
    Pass "Platform EDA report exists"
    if ($eda.json_summary.dataset_quality_score) {
        Info "  Platform quality score : $($eda.json_summary.dataset_quality_score)"
    }
} catch {
    Warn "EDA report not available — pipeline may not have completed"
}

# Check if agent reports exist
try {
    $agents = Invoke-RestMethod `
        -Uri "$BASE_URL/api/v1/datasets/$DATASET_ID/reports" `
        -Headers $HEADERS -TimeoutSec 15
    if ($agents.Count -gt 0) {
        Pass "Platform agent reports exist ($($agents.Count) agents ran)"
        foreach ($a in $agents) {
            Info "  - $($a.agent_name): $($a.model_used)"
        }
    } else {
        Warn "No agent reports yet — processing may be incomplete"
    }
} catch {
    Warn "Agent reports not available yet"
}

# Check notebook summary report if exists
if (Test-Path $NB_REPORT_PATH) {
    Pass "Notebook summary report exists"
    Write-Host ""
    Write-Host "  ── Notebook Summary Report ─────────────────────────" -ForegroundColor DarkGray
    Get-Content $NB_REPORT_PATH | ForEach-Object {
        Write-Host "  $_" -ForegroundColor Gray
    }
}

# ================================================================
# FINAL RESULTS
# ================================================================
Section "TEST RESULTS SUMMARY"

Write-Host ""
Write-Host "  Total PASSED   : $PASS" -ForegroundColor Green
Write-Host "  Total FAILED   : $FAIL" -ForegroundColor Red
Write-Host "  Total WARNINGS : $WARNINGS" -ForegroundColor Yellow
Write-Host ""

if ($FAIL -eq 0 -and $PASS -gt 0) {
    Write-Host "  ALL TESTS PASSED" -ForegroundColor Green
    Write-Host "  Both README and Notebook generation verified" -ForegroundColor Green
} elseif ($FAIL -gt 0) {
    Write-Host "  $FAIL tests failed — see output above for fixes" -ForegroundColor Red
} else {
    Write-Host "  No tests ran — check your setup" -ForegroundColor Yellow
}

Write-Host ""
Write-Host "  Output files saved:" -ForegroundColor Cyan
Write-Host "  README (markdown) : $README_MD_PATH" -ForegroundColor Gray
Write-Host "  README (html)     : $README_HTML_PATH" -ForegroundColor Gray
Write-Host "  Notebook executed : $NB_EXEC_PATH" -ForegroundColor Gray
Write-Host "  HTML report       : $NB_HTML_PATH" -ForegroundColor Gray
Write-Host ""
Write-Host "  Paste this output when asking for help." -ForegroundColor DarkGray
Write-Host "================================================================" -ForegroundColor DarkGray
