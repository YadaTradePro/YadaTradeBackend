# config.py
import os
from pathlib import Path
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


# ---------- مسیر پایه پروژه ----------
BASE_DIR = Path(__file__).resolve().parent

# ---------- تنظیمات پایگاه داده ----------
database_url = os.environ.get('DATABASE_URL')
if database_url:
    if database_url.startswith('sqlite:'):
        database_url += '?charset=utf8mb4'
else:
    database_file = BASE_DIR / 'app.db'
    database_url = f"sqlite:///{database_file}?charset=utf8mb4"

SQLALCHEMY_DATABASE_URI = database_url
SQLALCHEMY_TRACK_MODIFICATIONS = False

# ---------- تنظیمات Engine ----------
SQLALCHEMY_ENGINE_OPTIONS = {
    'echo': False,
    'pool_pre_ping': True,
    'connect_args': {
        'check_same_thread': False   # مهم برای threads مختلف در SQLite
    } if 'sqlite' in database_url else {}
}

# ---------- ساخت engine و session factory ----------
engine = create_engine(SQLALCHEMY_DATABASE_URI, **SQLALCHEMY_ENGINE_OPTIONS)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)



class Config:
    # SECRET_KEY برای امنیت Flask Session و CSRF استفاده می‌شود.
    # این را با یک کلید تصادفی و قوی متفاوت از JWT_SECRET_KEY جایگزین کنید.
    SECRET_KEY = os.environ.get(
        'SECRET_KEY') or 'dev_secret_key_change_in_production_12345'

    os.environ.pop("DATABASE_URL", None)  # Ignore Replit's PostgresSQL database URL



    # JWT_SECRET_KEY برای امضا و تایید توکن‌های JWT استفاده می‌شود.
    # این را با یک کلید تصادفی و قوی متفاوت از SECRET_KEY جایگزین کنید.
    JWT_SECRET_KEY = os.environ.get(
        'JWT_SECRET_KEY') or 'dev_jwt_secret_key_change_in_production_67890'
    JWT_ACCESS_TOKEN_EXPIRES = 3600 # 1 ساعت (به ثانیه)

    # تنظیمات لاگینگ
    LOG_LEVEL = os.environ.get('LOG_LEVEL', 'INFO') # DEBUG, INFO, WARNING, ERROR, CRITICAL

    # پرچم برای بررسی در دسترس بودن pytse-client
    # این پرچم در main.py مقداردهی می‌شود
    PYTSE_CLIENT_AVAILABLE = False

    # کلید API TraderMade برای دریافت قیمت کالاها
    TRADERMADE_API_KEY = os.environ.get('TRADERMADE_API_KEY', '3t4IP_kGMjN2wxz1BzvW')

   # کلید API MetalsDev برای دریافت قیمت کالاها
    METALS_DEV_API_KEY = os.environ.get('METALS_DEV_API_KEY', 'USXIBBPXNPFOPKR6BQ5N671R6BQ5N')


# --- تنظیمات پایگاه داده ---
    SQLALCHEMY_DATABASE_URI = os.environ.get(
        'DATABASE_URL') or 'sqlite:///' + os.path.join(
            os.path.abspath(os.path.dirname(__file__)), 'app.db')
    SQLALCHEMY_TRACK_MODIFICATIONS = False

# --- تنظیمات مسیردهی پروژه (جدید) ---
# این مسیر به پوشه 'backend' اشاره می کند
    PROJECT_ROOT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "backend")

# این مسیر به پوشه 'models' در داخل 'backend' اشاره می کند
    MODEL_DIR = os.path.join(PROJECT_ROOT_DIR, 'models')