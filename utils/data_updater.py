# -*- coding: utf-8 -*-
# utils/data_updater.py

import os
import requests
import pandas as pd
from datetime import datetime, timedelta
import time
import json
import logging
import jdatetime

# وارد کردن 'db' از extensions.py (به جای main.py)
from extensions import db 
# وارد کردن مدل‌های SQLAlchemy
from models import HistoricalData, ComprehensiveSymbolData, SignalsPerformance, FundamentalData, SentimentData # اضافه شدن FundamentalData و SentimentData

# --- تنظیمات لاگینگ (Logging Setup) ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- تنظیمات API ---
BRES_API_KEY = os.getenv('BRES_API_KEY')
BASE_URL = 'https://api.brsapi.ir/v1/'

if not BRES_API_KEY:
    logging.error("خطا: BRES_API_KEY در متغیرهای محیطی تنظیم نشده است! لطفا آن را در Replit (Secrets) تنظیم کنید.")

# --- توابع کمکی برای فراخوانی API ---

def fetch_data_from_brsapi(endpoint, params=None):
    """تابع عمومی برای فراخوانی هر Endpoint از BRSAPI."""
    if not BRES_API_KEY:
        logging.error("BRS_API_KEY تنظیم نشده است. نمی‌توان API را فراخوانی کرد.")
        return None

    headers = {
        'Authorization': f'Bearer {BRES_API_KEY}',
        'Content-Type': 'application/json'
    }
    url = f"{BASE_URL}{endpoint}"
    try:
        response = requests.get(url, headers=headers, params=params, timeout=30)
        response.raise_for_status() # برای خطاهای HTTP (4xx or 5xx)
        return response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"خطا در فراخوانی API از {url}: {e}")
        return None

def get_all_symbols_from_brsapi():
    """فراخوانی API برای دریافت لیست تمام نمادها."""
    logging.info("در حال دریافت لیست تمام نمادها از BRSAPI...")
    data = fetch_data_from_brsapi("symbol/all")
    if data and data.get('isSuccess') and data.get('data'):
        logging.info(f"{len(data['data'])} نماد با موفقیت دریافت شد.")
        return data['data']
    logging.error("موفق به دریافت لیست نمادها نشد.")
    return []

def get_daily_historical_data_from_brsapi(symbol_id, start_date, end_date):
    """فراخوانی API برای دریافت داده‌های تاریخی روزانه برای یک نماد خاص."""
    params = {
        'symbolId': symbol_id,
        'startDate': start_date.strftime('%Y-%m-%d'),
        'endDate': end_date.strftime('%Y-%m-%d')
    }
    data = fetch_data_from_brsapi("dailyHistoricalData", params=params)
    if data and data.get('isSuccess') and data.get('data'):
        return data['data']
    return []

def get_comprehensive_symbol_data_from_brsapi(symbol_id):
    """فراخوانی API برای دریافت داده‌های جامع یک نماد خاص."""
    params = {
        'symbolId': symbol_id
    }
    data = fetch_data_from_brsapi("symbol/comprehensiveData", params=params)
    if data and data.get('isSuccess') and data.get('data'):
        return data['data']
    return None

# --- توابع برای ذخیره داده‌ها در پایگاه داده (با SQLAlchemy) ---

def save_historical_data_to_db(data):
    """داده‌های تاریخی را در دیتابیس ذخیره یا به‌روزرسانی می‌کند."""
    if not data:
        return

    records_to_add = []

    existing_records = {}
    symbol_date_pairs = [(str(row['SymbolId']), str(row['Date'])) for row in data]

    if symbol_date_pairs:
        # برای بهبود عملکرد، می‌توانیم رکوردهای موجود را در یک پرس‌وجوی دسته‌ای دریافت کنیم.
        # اما برای تعداد زیاد جفت (symbol_id, date) این هنوز می‌تواند کند باشد.
        # یک رویکرد جایگزین برای حجم بسیار بالا، استفاده از SQLite upsert (INSERT OR REPLACE)
        # یا ابزارهای Bulk Insert/Update SQLAlchemy است.
        # برای این مثال، ابتدا بررسی می‌کنیم و سپس اضافه یا به‌روز می‌کنیم.

        # این بخش ممکن است در صورت بسیار بزرگ بودن symbol_date_pairs بهینه نباشد.
        # راه حل قوی‌تر: بارگذاری تمام رکوردهای موجود برای نمادهای مرتبط در یک بازه زمانی.

        # برای سادگی، فعلاً یک رویکرد کمتر بهینه اما کارآمد برای حجم متوسط داده را پیاده‌سازی می‌کنیم.
        # می‌توانید این بخش را برای کارایی بیشتر در حجم بالا بازنویسی کنید.
        for symbol_id, date in set(symbol_date_pairs): # استفاده از set برای جلوگیری از تکرار
            record = db.session.query(HistoricalData).filter_by(symbol_id=symbol_id, date=date).first()
            if record:
                existing_records[(symbol_id, date)] = record

    for row in data:
        symbol_id = str(row['SymbolId'])
        date = str(row['Date'])

        existing_record = existing_records.get((symbol_id, date))

        if existing_record:
            # به‌روزرسانی رکورد موجود
            existing_record.symbol_name = row['SymbolName']
            existing_record.open = row['Open']
            existing_record.high = row['High']
            existing_record.low = row['Low']
            existing_record.close = row['Close']
            existing_record.final = row['Final']
            existing_record.volume = row['Volume']
            existing_record.value = row['Value']
            existing_record.num_trades = row['NumberOfTrades']
            existing_record.yesterday_price = row['YesterdayPrice']
        else:
            # ایجاد رکورد جدید
            new_data = HistoricalData(
                symbol_id=symbol_id,
                symbol_name=row['SymbolName'],
                date=date,
                open=row['Open'],
                high=row['High'],
                low=row['Low'],
                close=row['Close'],
                final=row['Final'],
                volume=row['Volume'],
                value=row['Value'],
                num_trades=row['NumberOfTrades'],
                yesterday_price=row['YesterdayPrice']
            )
            records_to_add.append(new_data)

    if records_to_add:
        db.session.add_all(records_to_add)

    db.session.commit()
    logging.info(f"{len(records_to_add)} رکورد جدید و {len(data) - len(records_to_add)} رکورد موجود در HistoricalData به‌روزرسانی/اضافه شد.")


def save_comprehensive_symbol_data_to_db(data):
    """داده‌های جامع نماد را در دیتابیس ذخیره یا به‌روزرسانی می‌کند."""
    if not data:
        return

    symbol_id = str(data['SymbolId'])

    existing_data = db.session.query(ComprehensiveSymbolData).filter_by(symbol_id=symbol_id).first()

    if existing_data:
        existing_data.symbol_name = data['SymbolName']
        existing_data.market_type = data.get('MarketType')
        existing_data.flow = data.get('Flow')
        existing_data.industry = data.get('Industry')
        existing_data.capital = data.get('Capital')
        existing_data.legal_shareholder_percentage = data.get('LegalShareHolderPercentage')
        existing_data.real_shareholder_percentage = data.get('RealShareHolderPercentage')
        existing_data.float_shares = data.get('FloatShares')
        existing_data.base_volume = data.get('BaseVolume')
        existing_data.group_name = data.get('GroupName')
        existing_data.description = data.get('Description')
        existing_data.company_name = data.get('CompanyName')
        existing_data.isin = data.get('ISIN')
        logging.info(f"داده‌های جامع نماد {symbol_id} به‌روزرسانی شد.")
    else:
        new_data = ComprehensiveSymbolData(
            symbol_id=symbol_id,
            symbol_name=data['SymbolName'],
            market_type=data.get('MarketType'),
            flow=data.get('Flow'),
            industry=data.get('Industry'),
            capital=data.get('Capital'),
            legal_shareholder_percentage=data.get('LegalShareHolderPercentage'),
            real_shareholder_percentage=data.get('RealShareHolderPercentage'),
            float_shares=data.get('FloatShares'),
            base_volume=data.get('BaseVolume'),
            group_name=data.get('GroupName'),
            description=data.get('Description'),
            company_name=data.get('CompanyName'),
            isin=data.get('ISIN')
        )
        db.session.add(new_data)
        logging.info(f"داده‌های جامع نماد {symbol_id} جدید درج شد.")

    db.session.commit()


def update_all_stock_data_daily():
    """تابع اصلی برای به‌روزرسانی تمام داده‌های سهام."""
    if not BRES_API_KEY:
        logging.error("BRS_API_KEY تنظیم نشده است. به‌روزرسانی داده متوقف شد.")
        return

    logging.info("شروع به‌روزرسانی روزانه داده‌های سهام...")

    all_symbols = get_all_symbols_from_brsapi()
    if not all_symbols:
        logging.error("هیچ نمادی برای به‌روزرسانی یافت نشد. اتمام به‌روزرسانی.")
        return

    symbols_to_process = all_symbols[:100] # فقط 100 نماد اول را برای تست پردازش می‌کنیم

    today_miladi = datetime.now()
    start_date = today_miladi - timedelta(days=3)

    total_symbols_processed = 0

    for symbol in symbols_to_process:
        symbol_id = symbol['SymbolId']
        symbol_name = symbol['SymbolName']

        try:
            # --- دریافت و ذخیره داده‌های تاریخی ---
            historical_data = get_daily_historical_data_from_brsapi(symbol_id, start_date, today_miladi)
            if historical_data:
                save_historical_data_to_db(historical_data)
                logging.info(f"داده‌های تاریخی برای {symbol_name} ({symbol_id}) با موفقیت به‌روزرسانی شد.")
            else:
                logging.warning(f"داده تاریخی برای {symbol_name} ({symbol_id}) در بازه مشخص شده یافت نشد.")
            time.sleep(0.5)

            # --- دریافت و ذخیره داده‌های جامع نماد ---
            comprehensive_data = get_comprehensive_symbol_data_from_brsapi(symbol_id)
            if comprehensive_data:
                save_comprehensive_symbol_data_to_db(comprehensive_data)
                logging.info(f"داده‌های جامع برای {symbol_name} ({symbol_id}) با موفقیت به‌روزرسانی شد.")
            else:
                logging.warning(f"داده جامع برای {symbol_name} ({symbol_id}) یافت نشد.")
            time.sleep(0.5)

            total_symbols_processed += 1

        except Exception as e:
            db.session.rollback()
            logging.error(f"خطا در پردازش نماد {symbol_name} ({symbol_id}): {e}")
        finally:
            db.session.remove() # مهم برای آزاد کردن منابع

    logging.info(f"به‌روزرسانی روزانه داده‌های سهام به پایان رسید. {total_symbols_processed} نماد پردازش شد.")