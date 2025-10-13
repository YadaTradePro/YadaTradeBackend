# -*- coding: utf-8 -*-
import os
import sys
import logging
from flask import Flask, jsonify, request, current_app
from flask_jwt_extended import jwt_required, get_jwt_identity 
from extensions import db, bcrypt, jwt 
from flask_restx import Api, Namespace, Resource, fields
from flask_cors import CORS
from flask_apscheduler import APScheduler 
from flask_migrate import Migrate 
from datetime import datetime, date
import jdatetime
import pytz
import models
import click
import subprocess
import time

from config import Config


# --- وارد کردن سرویس ML
from services.ml_prediction_service import generate_and_save_predictions_for_watchlist

logger = logging.getLogger(__name__)
scheduler = APScheduler()

def create_app(test_config=None):
    """
    تابع اصلی برای ایجاد و پیکربندی برنامه Flask.
    """
    app = Flask(__name__)
    app.config.from_object(Config) # این خط برای پیکربندی از فایل config است

    
    # پیکربندی لاگینگ
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)

    logging.root.setLevel(logging.DEBUG)
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logging.root.addHandler(handler)
    
    logging.getLogger('services.golden_key_service').setLevel(logging.DEBUG)
    logging.getLogger('services.data_fetch_and_process').setLevel(logging.DEBUG)
    logging.getLogger('services.potential_buy_queues_service').setLevel(logging.DEBUG)
    logging.getLogger('services.performance_service').setLevel(logging.DEBUG) 
    logging.getLogger('services.ml_prediction_service').setLevel(logging.DEBUG) 

    app.logger.setLevel(logging.DEBUG)
    
    if test_config is None:
        app.config.from_object('config.Config')
    else:
        app.config.from_mapping(test_config)

    cors_origins = [
        "http://localhost:8000",
        "http://127.0.0.1:8000",
        "http://127.0.0.1:5000",
        "http://192.168.1.6:8000",  # Frontend on your phone (optional)
        "http://192.168.1.6:5000",  # Backend address for external access
        "http://10.0.2.2:5000",
        "http://localhost:3000",    # CRITICAL FIX: Add the Next.js development server URL
        "http://127.0.0.1:3000",      # CRITICAL FIX: Add the localhost loopback address
        "http://10.15.40.104:3000",
        "https://a8528c36-3864-4fde-aa02-b6c4d38572dd-00-3k36a6456ztnu.sisko.replit.dev:3001", # CRITICAL FIX: Add the new Replit frontend URL

        "https://a8528c36-3864-4fde-aa02-b6c4d38572dd-00-3k36a6456ztnu.sisko.replit.dev", # CRITICAL FIX: Add the new Replit frontend URL
    ]
    CORS(app, resources={r"/*": {"origins": cors_origins}}, supports_credentials=True)


    db.init_app(app)
    jwt.init_app(app)
    bcrypt.init_app(app)

    migrate = Migrate(app, db) 

    authorizations = {
        'Bearer Auth': {
            'type': 'apiKey',
            'in': 'header',
            'name': 'Authorization',
            'description': "JWT Authorization header using the Bearer scheme. Example: \"Authorization: Bearer {token}\""
        }
    }

    main_api = Api(
        app,
        version='1.0',
        title='Bourse Analysis API',
        description='API for Bourse Analysis with Technical, Fundamental data and User Management',
        doc='/api/swagger-ui/',
        prefix='/api',
        security='Bearer Auth',
        authorizations=authorizations
    )

    with app.app_context():
        # Remove db.create_all() and let Alembic handle migrations
        # db.create_all() 
        try:
            import pytse_client as tse_check
            current_app.logger.info("ماژول pytse-client با موفقیت وارد شد و در دسترس است.")
            app.config['PYTSE_CLIENT_AVAILABLE'] = True
        except ImportError:
            current_app.logger.error("خطا: ماژول pytse-client پیدا نشد. لطفا آن را با 'pip install pytse-client' نصب کنید. برنامه بدون دسترسی به داده‌های واقعی بازار ادامه خواهد یافت.")
            app.config['PYTSE_CLIENT_AVAILABLE'] = False
        except Exception as e:
            current_app.logger.error(f"خطای ناشناخته در وارد کردن pytse-client: {e}. برنامه بدون دسترسی به داده‌های واقعی بازار ادامه خواهد یافت.")
            app.config['PYTSE_CLIENT_AVAILABLE'] = False

    from routes.auth import auth_ns
    from routes.analysis import analysis_ns 
    from routes.golden_key import golden_key_ns
    from routes.weekly_watchlist import weekly_watchlist_ns 
    from routes.potential_queues import potential_queues_ns 
    from routes.performance import performance_ns
    # --- تغییر: وارد کردن namespace از فایل routes/market_data.py ---
    from routes.market_data import market_overview_ns 

    settings_ns = Namespace('settings', description='User settings operations')
    @settings_ns.route('/')
    class SettingsResource(Resource):
        @settings_ns.doc(security='Bearer Auth')
        @jwt_required() 
        def get(self):
            return {"message": "Settings endpoint. Not yet implemented."}, 200
    
    main_api.add_namespace(settings_ns, path='/settings')

    main_api.add_namespace(auth_ns, path='/auth')
    main_api.add_namespace(analysis_ns, path='/analysis') 
    # --- تغییر: اضافه کردن namespace از فایل وارد شده ---
    main_api.add_namespace(market_overview_ns, path='/market-overview') 
    main_api.add_namespace(golden_key_ns, path='/golden_key')
    main_api.add_namespace(weekly_watchlist_ns, path='/weekly_watchlist') 
    main_api.add_namespace(potential_queues_ns, path='/potential_queues') 
    main_api.add_namespace(performance_ns, path='/performance')

    @jwt.unauthorized_loader
    def unauthorized_response(callback):
        return jsonify({"message": "توکن احراز هویت موجود نیست یا نامعتبر است."}), 401

    @jwt.invalid_token_loader
    def invalid_token_response(callback):
        app.logger.error(f"خطای توکن نامعتبر: {callback}")
        return jsonify({"message": "اعتبار سنجی امضای توکن انجام نشد."}), 403

    @jwt.expired_token_loader
    def expired_token_response(jwt_header, jwt_data):
        current_app.logger.warning(f"Expired token detected. Header: {jwt_header}, Data: {jwt_data}")
        return jsonify({"message": "Your session has expired. Please log in again.", "code": "token_expired"}), 401

    @app.route('/')
    def home():
        return jsonify({
            "message": "به API تحلیل بورس Flask خوش آمدید! مستندات API در /api/swagger-ui/ در دسترس است."
        })




# ----------------------------------------------------
#  محل افزودن دستور جدید generate-ml-predictions
# ----------------------------------------------------

    @app.cli.command('generate-ml-predictions')
    @click.option('--date', default=None, help='تاریخ پیش‌بینی به فرمت YYYY-MM-DD (اختیاری، پیش‌فرض: امروز).')
    @click.option('--period', default=7, type=int, help='افق پیش‌بینی بر حسب روز (پیش‌فرض: 7).')
    def generate_predictions_command(date, period):
        """تولید و ذخیره پیش‌بینی‌های ML برای نمادها."""
        if date:
            try:
                prediction_date = datetime.strptime(date, '%Y-%m-%d').date()
            except ValueError:
                click.echo("خطا: فرمت تاریخ نامعتبر است. لطفاً از YYYY-MM-DD استفاده کنید.")
                return
        else:
            prediction_date = None
        
        with app.app_context():
            success, message = generate_and_save_predictions_for_watchlist(
                prediction_date_greg=prediction_date, 
                prediction_period_days=period
            )
            if success:
                click.echo(f"موفقیت: {message}")
            else:
                click.echo(f"خطا: {message}")





# ----------------------------------------------------
#  محل افزودن دستور جدید run-candlestick
# ----------------------------------------------------
    @app.cli.command('run-candlestick-detection')
    @click.option('--limit', default=None, type=int, help='تعداد نمادهایی که باید پردازش شوند (اختیاری).')
    def run_candlestick_detection_command(limit):
        """
        اجرای تشخیص و ذخیره الگوهای شمعی برای نمادها.
        """
        from services.data_fetch_and_process import run_candlestick_detection
        
        click.echo("🕯️ شروع تشخیص الگوهای شمعی...")
        
        with app.app_context():
            db_session = db.session
            try:
                processed_count = run_candlestick_detection(
                    db_session=db_session, 
                    limit=limit
                )
                click.echo(f"✅ موفقیت: {processed_count} الگوی شمعی برای نمادها پردازش و ذخیره شد.")
            except Exception as e:
                click.echo(f"❌ خطای بحرانی در اجرای Candlestick Detection: {e}", err=True)
                db_session.rollback()
                sys.exit(1)





# ----------------------------------------------------
#  محل افزودن دستور جدید run-technical-analysis
# ----------------------------------------------------
    @app.cli.command('run-technical-analysis')
    @click.option('--limit', default=None, type=int, help='تعداد نمادهایی که باید پردازش شوند (اختیاری).')
    @click.option('--symbols', default=None, help='لیست نمادها با جداکننده کاما (اختیاری).')
    def run_technical_analysis_command(limit, symbols):
        """
        اجرای محاسبات و ذخیره اندیکاتورهای تکنیکال برای نمادها.
        """
        from services.data_fetch_and_process import run_technical_analysis
        
        click.echo("📊 شروع تحلیل تکنیکال و محاسبه اندیکاتورها...")
        
        symbols_list = None
        if symbols:
            symbols_list = [s.strip() for s in symbols.split(',') if s.strip()]
        
        with app.app_context():
            db_session = db.session
            try:
                processed_count, message = run_technical_analysis(
                    db_session=db_session, 
                    limit=limit,
                    symbols_list=symbols_list
                )
                click.echo(f"✅ موفقیت: {message}")
            except Exception as e:
                click.echo(f"❌ خطای بحرانی در اجرای Technical Analysis: {e}", err=True)
                db_session.rollback()
                sys.exit(1)




# ----------------------------------------------------
#  محل افزودن دستور جدید run-historical-data
# ----------------------------------------------------
    @app.cli.command('run-historical-data')
    @click.option('--limit', default=None, type=int, help='تعداد نمادهایی که باید پردازش شوند (اختیاری).')
    @click.option('--symbols', default=None, help='لیست نمادها (symbol_name یا tse_index) با جداکننده کاما (اختیاری).')
    @click.option('--limit-per-run', default=None, type=int, help='نام جایگزین برای limit.')
    def fetch_historical_data_command(limit, symbols, limit_per_run):
        """
        اجرای دریافت و پردازش داده‌های تاریخی جامع برای نمادهای مشخص یا تمام نمادها.
        """
        from services.data_fetch_and_process import fetch_and_process_historical_data
        
        click.echo("📈 شروع دریافت داده‌های تاریخی و حقیقی/حقوقی...")
        
        # normalize limit
        if limit is None and limit_per_run is not None:
            limit = limit_per_run
            
        symbols_list = None
        if symbols:
            symbols_list = [s.strip() for s in symbols.split(',') if s.strip()]
        
        with app.app_context():
            db_session = db.session
            try:
                processed_count, message = fetch_and_process_historical_data(
                    db_session=db_session, 
                    limit=limit,
                    specific_symbols_list=symbols_list
                )
                click.echo(f"✅ موفقیت: {message}")
            except Exception as e:
                click.echo(f"❌ خطای بحرانی در اجرای Historical Data Fetch: {e}", err=True)
                db_session.rollback()
                sys.exit(1)


# ----------------------------------------------------
#  محل افزودن دستور جدید run-sector-analysis
# ----------------------------------------------------
    @app.cli.command('run-sector-analysis')
    def run_sector_analysis_command():
        """
        اجرای تحلیل و رتبه‌بندی صنایع بر اساس ارزش معاملات و ورود پول هوشمند.
        """
        # ⚠️ مسیر وارد کردن (Import) تابع اصلاح شد 
        from services.sector_analysis_service import run_daily_sector_analysis 
        
        click.echo("🏭 شروع تحلیل و رتبه‌بندی روزانه صنایع...")
        
        with app.app_context():
            try:
                run_daily_sector_analysis() # فراخوانی تابع تحلیل
                click.echo("✅ موفقیت: تحلیل صنعت با موفقیت به پایان رسید و در دیتابیس ذخیره شد.")
            except Exception as e:
                click.echo(f"❌ خطای بحرانی در اجرای تحلیل صنعت: {e}", err=True)
                db.session.rollback()
                sys.exit(1)



# 🆕 دستور جدید CLI برای Full Historical Refresh
    @app.cli.command('fetch-full-hist-fund-data')
    @click.option('--symbols', '-s', type=str, default=None, help='لیست نام نمادهای مشخص (با کاما جدا شوند) برای پردازش. اگر خالی باشد، همه نمادها پردازش می‌شوند. (مثال: خودرو,خساپا)')
    def fetch_full_hist_fund_data_command(symbols):
        """
        ⚠️ اجرای فرآیند کامل دریافت و به‌روزرسانی داده‌های تاریخی و بنیادی (TRUNCATE + Bulk Insert)
        """
        # ⚠️ مسیر وارد کردن (Import) تابع را بر اساس ساختار پروژه خود تنظیم کنید
        from services.fetch_full_historical_pytse import fetch_full_historical_pytse 

        click.echo("📊📈 شروع دریافت داده‌های تاریخی و بنیادی (Full Refresh)...")

        # تبدیل رشته نمادها به لیست (اگر وجود دارد)
        symbols_list = None
        if symbols:
            symbols_list = [s.strip() for s in symbols.split(',') if s.strip()]

        with app.app_context():
            # دریافت سشن دیتابیس
            db_session = db.session
            try:
                # فراخوانی تابع Full Refresh
                record_count, message = fetch_full_historical_pytse(
                    db_session, 
                    symbols_to_update=symbols_list
                )
            
                click.echo(f"✅ موفقیت: عملیات Full Refresh تکمیل شد. {message}")
            
            except Exception as e:
                click.echo(f"❌ خطای بحرانی در اجرای به‌روزرسانی داده‌های تاریخی و بنیادی (Full Refresh): {e}", err=True)
                db_session.rollback()
                sys.exit(1)


# ----------------------------------------------------
    @app.cli.command('populate-symbols')
    @click.option('--batch-size', 
                type=int, 
                default=200, 
                help='تعداد نمادهایی که در هر دسته (Batch) از بورس خوانده و در دیتابیس ثبت می‌شوند. پیش‌فرض: 200.')
    def populate_symbols_command(batch_size):
        """
        لیست نمادها را از بورس دریافت کرده و در ComprehensiveSymbolData درج/به‌روزرسانی می‌کند.
        این عملیات به صورت دسته‌ای (Batch) برای پایداری بیشتر اجرا می‌شود.
        """
        # ⚠️ مسیر وارد کردن (Import) تابع را بر اساس ساختار پروژه خود تنظیم کنید
        from services.data_fetch_and_process import populate_comprehensive_symbols 
    
        click.echo("📥 شروع دریافت و به‌روزرسانی لیست نمادها...")
    
        with app.app_context():
            # دریافت سشن دیتابیس
            db_session = db.session 
            try:
                # 💡 فراخوانی تابع با پارامتر جدید batch_size
                result = populate_comprehensive_symbols(db_session, batch_size=batch_size) 
        
                click.echo(f"✅ موفقیت: لیست نمادها به‌روزرسانی شد: {result['added']} نماد جدید اضافه و {result['updated']} نماد به‌روز شدند.")
        
            except Exception as e:
                click.echo(f"❌ خطای بحرانی در اجرای به‌روزرسانی نمادها: {e}", err=True)
                db_session.rollback()
                sys.exit(1)

# ----------------------------------------------------

    @app.cli.command('fetch-hist-fund-data')
    @click.option('--limit', type=int, default=None, help='محدود کردن تعداد نمادهای پردازش شده.')
    @click.option('--batch-size', type=int, default=200, help='تعداد نمادها در هر دسته برای پردازش (پیش‌فرض 200).')
    def fetch_hist_fund_data_command(limit, batch_size):
        """
        داده‌های تاریخی و بنیادی را برای نمادهای موجود در دیتابیس دریافت و به‌روزرسانی می‌کند.
        """
        # ⚠️ مسیر وارد کردن (Import) تابع را بر اساس ساختار پروژه خود تنظیم کنید
        from services.data_fetch_and_process import fetch_historical_and_fundamental_data 

        click.echo("📊📈 شروع دریافت داده‌های تاریخی و بنیادی...")

        with app.app_context():
            # دریافت سشن دیتابیس
            db_session = db.session
            try:
                # ===> تغییر اصلی: پاس دادن batch_size
                result = fetch_historical_and_fundamental_data(
                    db_session, 
                    limit=limit,
                    batch_size=batch_size # اضافه شدن پارامتر جدید
                )
        
                msg = f"✅ موفقیت: داده‌ها به‌روزرسانی شد. "
                msg += f"تاریخی برای {result['historical']} نماد، "
                msg += f"بنیادی برای {result['fundamental']} نماد."
        
                click.echo(msg)
        
            except Exception as e:
                click.echo(f"❌ خطای بحرانی در اجرای به‌روزرسانی داده‌های تاریخی و بنیادی: {e}", err=True)
                db_session.rollback()
                sys.exit(1)





    return app

# --- اضافه کردن کد برای اجرای خودکار سرور پراکسی در زمان اجرای برنامه اصلی ---
tgju_proxy_process = None

def start_tgju_proxy_service():
    """
    اجرای سرور پراکسی TGJU به عنوان یک فرآیند پس‌زمینه.
    """
    global tgju_proxy_process
    if tgju_proxy_process and tgju_proxy_process.poll() is None:
        return

    logger.info("در حال راه‌اندازی سرور پراکسی TGJU در پس‌زمینه...")
    try:
        tgju_proxy_process = subprocess.Popen(
            [sys.executable, 'services/tgju.py'],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        time.sleep(2)
        logger.info("سرور پراکسی TGJU با موفقیت راه‌اندازی شد.")
    except FileNotFoundError:
        logger.error("خطا: فایل services/tgju.py پیدا نشد. مطمئن شوید مسیر صحیح است.")
    except Exception as e:
        logger.error(f"خطا در راه‌اندازی سرور پراکسی TGJU: {e}", exc_info=True)


if __name__ == "__main__":
    app = create_app()

    # راه‌اندازی پراکسی TGJU (فقط در کانتینر API یا اجرا مستقیم)
    start_tgju_proxy_service()

    # فقط در حالت توسعه (نه در production و نه در scheduler)
    if os.environ.get("FLASK_ENV") == "development":
        with app.app_context():
            app.logger.info("Scheduler باید در scheduler.py اجرا شود، نه در main.py")

    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=True, use_reloader=False)