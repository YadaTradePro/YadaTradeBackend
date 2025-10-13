#!/bin/bash
echo "Waiting 5 seconds for TGJU Proxy to start..."
sleep 5   # مدت زمان صبر برای TGJU Proxy، می‌توان تغییر داد

echo "Starting API Service..."
exec gunicorn --workers 2 --worker-class gevent --bind 0.0.0.0:5000 --timeout 0 "main:create_app()"