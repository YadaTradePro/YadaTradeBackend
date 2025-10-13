# TGJU service (starts first)
tgju: powershell -NoProfile -Command "$env:PYTHONUTF8='1'; python services/tgju.py"

# Web service (Flask) starts with delay
web: powershell -NoProfile -Command "$env:PORT='5000'; $env:PYTHONUTF8='1'; Start-Sleep -Seconds 60; python main.py"

# Scheduler service starts later to avoid memory conflicts
scheduler: powershell -NoProfile -Command "$env:PYTHONUTF8='1'; Start-Sleep -Seconds 300; python scheduler.py"
