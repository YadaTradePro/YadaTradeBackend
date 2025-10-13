@echo off
cd /d "E:\BourseAnalysis\Final 20251012\YadaTradeBackend"
call venv\Scripts\activate
set FLASK_APP=main.py
honcho start
pause
