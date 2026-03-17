# DataSage Merge Studio — Complete Integration Guide
# ====================================================
# Run these steps in order from your datasage/ root directory.

# ================================================================
# STEP 1 — Copy files into the project structure
# ================================================================

# Backend files
cp auto_joiner.py       app/merge/auto_joiner.py
cp merge_routes.py      app/api/v1/routes/merge.py
cp merge_model.py       app/models/merge.py
cp migration_merge_operations.py  alembic/versions/0003_merge_operations.py

# Frontend file
cp MergeStudio.jsx      frontend/src/components/MergeStudio/MergeStudio.jsx

# Create __init__.py for merge module if not exists
touch app/merge/__init__.py


# ================================================================
# STEP 2 — Verify main.py already has the merge router
# ================================================================
# Your main.py already has:
#   from app.api.v1.routes import aiid, auth, datasets, jobs, merge, notebooks, stats, upload
#   app.include_router(merge.router, prefix="/api/v1")
# So NO changes to main.py needed!


# ================================================================
# STEP 3 — Run DB migration
# ================================================================

docker-compose exec api alembic upgrade head

# Verify table was created:
docker-compose exec api python3 -c "
from sqlalchemy import inspect
from app.core.database import engine
import asyncio

async def check():
    async with engine.begin() as conn:
        tables = await conn.run_sync(lambda c: inspect(c).get_table_names())
        print('Tables:', tables)
        if 'merge_operations' in tables:
            print('✅ merge_operations table exists')
        else:
            print('❌ merge_operations table missing')

asyncio.run(check())
"


# ================================================================
# STEP 4 — Install any missing Python deps
# ================================================================

docker-compose exec api pip install numpy --quiet
# pandas, sqlalchemy already in requirements.txt


# ================================================================
# STEP 5 — Restart API to pick up new routes
# ================================================================

docker-compose restart api
sleep 8

# Verify merge routes are registered:
curl -s http://localhost:8000/openapi.json | python3 -c "
import json, sys
paths = sorted(json.load(sys.stdin).get('paths', {}).keys())
merge_paths = [p for p in paths if 'merge' in p]
print(f'Merge routes: {len(merge_paths)}')
for p in merge_paths:
    print(f'  {p}')
"


# ================================================================
# STEP 6 — Wire frontend route
# ================================================================

# Add to your React router (e.g. App.jsx or router.jsx):
#
#   import MergeStudio from './components/MergeStudio/MergeStudio';
#
#   // Inside your <Routes>:
#   <Route path="/merge" element={<MergeStudio />} />
#
# Add to sidebar navigation:
#   { path: '/merge', label: 'Merge Studio', icon: '⟳' }


# ================================================================
# STEP 7 — Test the full flow
# ================================================================

# Get auth token
TOKEN=$(curl -s -X POST http://localhost:8000/api/v1/auth/login \
  -H "Content-Type: application/json" \
  -d '{"email":"test@datasage.com","password":"testpass123"}' \
  | python3 -c "import json,sys; print(json.load(sys.stdin).get('access_token',''))")

echo "Token: ${TOKEN:0:30}..."

# Get dataset IDs (need 2 complete datasets)
curl -s http://localhost:8000/api/v1/datasets/ \
  -H "Authorization: Bearer $TOKEN" \
  | python3 -c "
import json, sys
datasets = json.load(sys.stdin)
complete = [d for d in datasets if d.get('status') == 'complete']
print(f'Complete datasets: {len(complete)}')
for d in complete[:5]:
    print(f'  {d[\"id\"]} | {d[\"name\"]} | {d[\"row_count\"]} rows')
"

# Set your dataset IDs:
LEFT_ID="paste-first-dataset-id"
RIGHT_ID="paste-second-dataset-id"

# Test auto-detect
curl -s -X POST http://localhost:8000/api/v1/merge/detect \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d "{
    \"left_dataset_id\":  \"$LEFT_ID\",
    \"right_dataset_id\": \"$RIGHT_ID\",
    \"top_n\": 5
  }" | python3 -c "
import json, sys
data = json.load(sys.stdin)
candidates = data.get('candidates', [])
print(f'Left:  {data.get(\"left_name\")} ({data.get(\"left_rows\")} rows)')
print(f'Right: {data.get(\"right_name\")} ({data.get(\"right_rows\")} rows)')
print(f'Candidates found: {len(candidates)}')
print()
for i, c in enumerate(candidates, 1):
    print(f'  #{i} {c[\"left_col\"]} ↔ {c[\"right_col\"]}')
    print(f'     Confidence: {c[\"confidence\"]}/100')
    print(f'     Strategy:   {c[\"strategy\"]}')
    print(f'     Matches:    {c[\"match_count\"]} ({c[\"left_match_pct\"]:.0f}% of left)')
    print()
"


# ================================================================
# EXPECTED OUTPUT
# ================================================================

# Merge routes: 5
#   /api/v1/merge/detect
#   /api/v1/merge/preview
#   /api/v1/merge/apply
#   /api/v1/merge/history
#   /api/v1/merge/{merge_id}
#
# Candidates found: 3-5 depending on dataset
#   #1 incident_id ↔ inc_id
#      Confidence: 87/100
#      Strategy:   normalized
#      Matches:    847 (95% of left)
#
#   #2 date ↔ incident_date
#      Confidence: 72/100
#      Strategy:   date
#      Matches:    623 (70% of left)
