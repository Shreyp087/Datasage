
Write-Host "Starting DataSage Local Environment (Cloud DBs / Local Storage)" -ForegroundColor Green

# 1. Start Backend API
Write-Host "Starting FastAPI Backend..." -ForegroundColor Yellow
Start-Process powershell -ArgumentList "-NoExit -Command `"cd backend; python -m uvicorn main:app --reload`""

# 2. Start Celery Worker
Write-Host "Starting Celery Worker..." -ForegroundColor Yellow
Start-Process powershell -ArgumentList "-NoExit -Command `"cd backend; python -m celery -A celery_app worker --loglevel=info --pool=solo -Q fast,heavy,agents`""

# 3. Start Frontend
Write-Host "Starting React Frontend..." -ForegroundColor Yellow
Start-Process powershell -ArgumentList "-NoExit -Command `"cd frontend; npm run dev -- --host 127.0.0.1 --port 3000`""

Write-Host "All services started in separate windows! You can close this window." -ForegroundColor Cyan
Write-Host "Frontend: http://localhost:3000" -ForegroundColor Cyan
Write-Host "Backend API: http://localhost:8000/docs" -ForegroundColor Cyan
