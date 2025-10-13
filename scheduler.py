import os
import time
import logging
from functools import wraps
from typing import Optional # برای type hinting در دکوراتور
from main import create_app
from extensions import scheduler
# 🛠️ ایمپورت مستقیم SessionLocal از config.py (چون آنجا تعریف شده است)
from config import SessionLocal 
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError 

# Import all service functions needed by the scheduler
from services.data_fetch_and_process import run_full_data_update
from services.data_fetch_and_process import run_daily_update
# 👇🏻 توابع جدید برای فرآیند تحلیل
from services.data_fetch_and_process import run_technical_analysis, run_candlestick_detection 
from services.weekly_watchlist_service import run_weekly_watchlist_selection, evaluate_weekly_watchlist_performance
from services.golden_key_service import run_golden_key_analysis_and_save
from services.potential_buy_queues_service import run_potential_buy_queue_analysis_and_save
from services.ml_prediction_service import generate_and_save_predictions_for_watchlist, update_ml_prediction_outcomes
from services import market_analysis_service
from services.sector_analysis_service import run_daily_sector_analysis
from services.fetch_latest_brsapi_eod import update_daily_eod_from_brsapi



# ----------------- Logging Setup -----------------
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
# تنظیم لاگینگ را با handlers ساده‌تر می‌کنیم (به جای استفاده از config)
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT, handlers=[
    logging.FileHandler("scheduler.log", encoding="utf-8"),
    logging.StreamHandler()
])

logger = logging.getLogger(__name__)

# ----------------- App Context and Error Handling Decorator -----------------
app = create_app()

def with_context_and_error_handling(func):
    """
    Decorator to run a function inside a Flask app context, provide a DB session (if needed), 
    and handle potential exceptions (including DB transaction management).
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        # بررسی می‌کنیم که آیا تابع ورودی به db_session نیاز دارد یا خیر
        needs_db_session = 'db_session' in func.__code__.co_varnames
        db_session: Optional[Session] = None
        
        with app.app_context():
            logger.info(f"✅ Executing job '{func.__name__}' inside Flask app context.")
            
            # 2. ایجاد Database Session در صورت نیاز (مانند run_daily_update)
            if needs_db_session:
                try:
                    db_session = SessionLocal() 
                    kwargs['db_session'] = db_session # 🛠️ تزریق db_session
                except Exception as e:
                    logger.error(f"❌ Failed to create DB Session for job '{func.__name__}': {e}", exc_info=True)
                    # اگر نتواند سشن ایجاد کند، Job نباید اجرا شود
                    raise 
                
            try:
                # 3. اجرای تابع با آرگومان‌های به‌روز شده
                result = func(*args, **kwargs)
                
                # 4. Commit کردن تغییرات دیتابیس
                if db_session:
                    db_session.commit()
                    logger.info(f"✅ Job '{func.__name__}' completed successfully and changes committed.")
                else:
                    logger.info(f"✅ Job '{func.__name__}' completed successfully.")
                return result
                
            except SQLAlchemyError as e:
                # 5. Rollback در صورت بروز خطای دیتابیس
                if db_session:
                    db_session.rollback()
                logger.error(f"❌ Database Error in job '{func.__name__}': {e}", exc_info=True)
                raise 
                
            except Exception as e:
                # 5. Rollback و مدیریت خطاهای عمومی
                if db_session:
                    db_session.rollback()
                logger.error(f"❌ An error occurred while running job '{func.__name__}': {e}", exc_info=True)
                raise
                
            finally:
                # 6. بستن سشن در هر صورت
                if db_session:
                    db_session.close() 
                
    return wrapper

# ----------------- Custom Flow Definition -----------------

@with_context_and_error_handling
def run_daily_analysis_flow(db_session: Session = None):
    """
    اجرای ترتیبی فرآیند آپدیت داده‌ها و تحلیل روزانه. 
    این تابع نیاز به db_session دارد تا بتواند آن را به توابع پایینی پاس دهد.
    """
    logger.info("🎬 شروع 'Daily Analysis Flow': EOD Update -> Technical Analysis -> Candlestick Detection")

    # 1. به‌روزرسانی داده‌های پایان روز (EOD)
    logger.info("➡️ مرحله 1/3: فراخوانی update_daily_eod_from_brsapi")
    # ===> تغییر اعمال شده: ارسال db_session به تابع <===
    update_daily_eod_from_brsapi(db_session=db_session) 
    logger.info("✅ مرحله 1/3: update_daily_eod_from_brsapi با موفقیت انجام شد.")

    # 2. اجرای تحلیل تکنیکال (نیاز به داده‌های به‌روز شده دارد)
    logger.info("➡️ مرحله 2/3: فراخوانی run_technical_analysis")
    # فرض می‌کنیم run_technical_analysis به db_session نیاز دارد
    run_technical_analysis(db_session=db_session) 
    logger.info("✅ مرحله 2/3: run_technical_analysis با موفقیت انجام شد.")

    # 3. اجرای تشخیص الگوهای کندل استیک (نیاز به داده‌های به‌روز و نتایج تحلیل تکنیکال دارد)
    logger.info("➡️ مرحله 3/3: فراخوانی run_candlestick_detection")
    # فرض می‌کنیم run_candlestick_detection به db_session نیاز دارد
    run_candlestick_detection(db_session=db_session)
    logger.info("✅ مرحله 3/3: run_candlestick_detection با موفقیت انجام شد.")
    
    logger.info("🎉 پایان موفقیت‌آمیز 'Daily Analysis Flow'.")

# ----------------- Job Definitions -----------------
JOBS = [
    # 🟢 وظایف روزانه  

    {"id": "daily_analysis_flow_job", "func": run_daily_analysis_flow, "trigger": "cron", "day_of_week": "sat, sun, mon, tue, wed", "hour": 22, "minute": 26}, # تابع از قبل دکوریت شده است

    {"id": "daily_sector_analysis_job", "func": run_daily_sector_analysis, "trigger": "cron", "day_of_week": "sat, sun, mon, tue, wed", "hour": 22, "minute": 29},
    {"id": "generate_daily_summary_job", "func": market_analysis_service.generate_market_summary, "trigger": "cron", "day_of_week": "sat, sun, mon, tue, wed", "hour": 22, "minute": 31},

    {"id": "potential_buy_queues_job", "func": run_potential_buy_queue_analysis_and_save, "trigger": "cron", "day_of_week": "sat, sun, mon, tue, wed", "hour": 7, "minute": 15},

    {"id": "update_ml_outcomes_job", "func": update_ml_prediction_outcomes, "trigger": "cron", "day_of_week": "sat, sun, mon, tue, wed", "hour": 8, "minute": 0},

    # 🟡 وظایف هفتگی

    {"id": "weekly_watchlist_performance_job", "func": evaluate_weekly_watchlist_performance, "trigger": "cron", "day_of_week": "wed", "hour": 22, "minute": 0},
    {"id": "weekly_watchlist_selection_job", "func": run_weekly_watchlist_selection, "trigger": "cron", "day_of_week": "wed", "hour": 22, "minute": 30},

    {"id": "run_golden_key_filters_job", "func": run_golden_key_analysis_and_save, "trigger": "cron", "day_of_week": "sun, tue, wed", "hour": 23, "minute": 30},

    {"id": "update_exit_prices_job", "func": market_analysis_service.update_evaluated_prices_job, "trigger": "cron", "day_of_week": "thu", "hour": 4, "minute": 0},
    {"id": "daily_light_update_job", "func": run_daily_update, "trigger": "cron", "day_of_week": "thu", "hour": 16, "minute": 0, "coalesce": True, "max_instances": 1}, 

    {"id": "generate_ml_predictions_job", "func": generate_and_save_predictions_for_watchlist, "trigger": "cron", "day_of_week": "thu", "hour": 2, "minute": 0, "coalesce": True, "max_instances": 1}, 
    
    # ⚪️ سایر وظایف   
    {"id": "run-maintenance-update", "func": run_full_data_update, "trigger": "cron", "day": 1, "hour": 16, "minute": 50, "coalesce": True, "max_instances": 1},
    
]

TIMEZONE = "Asia/Tehran"

# ----------------- Scheduler Runner -----------------
def run_scheduler_app():
    """Runs the APScheduler in a standalone process."""
    app.config["SCHEDULER_RUN"] = True
    
    scheduler.init_app(app)

    for job in JOBS:
        try:
            # 💡 این بخش از کد، دکوراتور with_context_and_error_handling را به تابع اعمال می‌کند، مگر اینکه قبلاً اعمال شده باشد.
            # به جای شرط پیچیده، می‌توانیم فرض کنیم که تمام توابع در لیست JOBS خام (بدون دکوراتور) هستند
            # به جز run_daily_analysis_flow که در تعریفش دکوریت شده است.
            func_to_schedule = job["func"]

            # اگر تابع قبلاً توسط دکوراتور ما پیچیده نشده باشد، آن را می‌پیچیم.
            # برای سادگی، run_daily_analysis_flow را از این اعمال دکوراتور در حلقه نهایی حذف می‌کنیم.
            if job["id"] != "daily_analysis_flow_job":
                func_to_schedule = with_context_and_error_handling(job["func"])
            
            scheduler.add_job(
                id=job["id"],
                func=func_to_schedule,
                trigger=job["trigger"],
                replace_existing=True,
                timezone=TIMEZONE,
                **{k: v for k, v in job.items() if k not in ["id", "func", "trigger"]}
            )
            logger.info(f"✅ Job registered: {job['id']}")
        except Exception as e:
            logger.error(f"❌ Failed to add job {job['id']}: {e}")

    scheduler.start()
    logger.info("🚀 APScheduler started in a separate process.")

    try:
        while True:
            time.sleep(60)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
        logger.info("🛑 Scheduler has been shut down.")

if __name__ == "__main__":
    run_scheduler_app()
