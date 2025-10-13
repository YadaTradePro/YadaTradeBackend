# extensions.py
from flask_sqlalchemy import SQLAlchemy
from flask_bcrypt import Bcrypt
from flask_jwt_extended import JWTManager
from flask_apscheduler import APScheduler # اضافه شدن APScheduler

# تنظیمات SQLAlchemy با پشتیبانی Unicode
db = SQLAlchemy()
bcrypt = Bcrypt()
jwt = JWTManager()
scheduler = APScheduler() # اضافه شدن scheduler
