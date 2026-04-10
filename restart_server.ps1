# Stop any existing uvicorn processes
Get-Process | Where-Object {$_.ProcessName -eq "python" -and $_.MainWindowTitle -like "*uvicorn*"} | Stop-Process -Force -ErrorAction SilentlyContinue

# Wait a moment
Start-Sleep -Seconds 2

# Start the server
Write-Host "Starting Firetiger Telemetry Demo server..."
python -m uvicorn main:app --reload --host 0.0.0.0 --port 8000
