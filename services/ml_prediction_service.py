# -*- coding: utf-8 -*-
# services/ml_prediction_service.py - سرویس برای تولید و ذخیره پیش‌بینی‌های ML و به‌روزرسانی نتایج

import pandas as pd
from datetime import datetime, date, timedelta
import jdatetime
import logging
import os
import sys

# تنظیمات لاگ‌نویسی
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


# --- تنظیمات مسیردهی هوشمند ---
current_script_dir = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(current_script_dir, '..'))  # یک سطح بالاتر = ریشه پروژه

if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# تشخیص خودکار محیط: Docker یا Local
MODELS_DIR = '/app/models' if os.path.exists('/app') else os.path.join(PROJECT_ROOT, "models")

# ایجاد پوشه اگر وجود ندارد
os.makedirs(MODELS_DIR, exist_ok=True)

print(" محیط تشخیص داده شد:")
print(" مسیر پروژه:", PROJECT_ROOT)
print(" مسیر مدل‌ها:", MODELS_DIR)
print(" محیط:", "Docker" if os.path.exists('/app') else "Local Machine")

# --- مسیرهای دیگر پروژه ---
SERVICES_PATH = os.path.join(PROJECT_ROOT, 'services')
DATA_PATH = os.path.join(PROJECT_ROOT, 'data')

if SERVICES_PATH not in sys.path:
    sys.path.insert(0, SERVICES_PATH)

# تعریف مسیرهای خاص فایل‌های مدل
LATEST_MODEL_PATH = os.path.join(MODELS_DIR, 'latest_model.joblib')
LATEST_SCALER_PATH = os.path.join(MODELS_DIR, 'latest_scaler.joblib')
LATEST_FEATURE_NAMES_PATH = os.path.join(MODELS_DIR, 'latest_feature_names.joblib')
LATEST_CLASS_LABELS_PATH = os.path.join(MODELS_DIR, 'latest_class_labels.joblib')


try:
    from extensions import db
    from models import MLPrediction, ComprehensiveSymbolData, HistoricalData
    from ml_predictor import predict_trend_for_symbol
    from services.utils import convert_gregorian_to_jalali
except ImportError as e:
    logger.error(f"خطا در ایمپورت ماژول‌ها در ml_prediction_service.py: {e}")
    sys.exit(1)


def get_ml_predictions_for_symbol(symbol_id: str) -> dict | None:
    """
    Retrieves the latest ML prediction for a given symbol_id.
    """
    logger.info(f"Fetching latest ML prediction for symbol_id: {symbol_id}")
    try:
        prediction = MLPrediction.query.filter_by(symbol_id=symbol_id)\
                                       .order_by(MLPrediction.prediction_date.desc())\
                                       .first()

        if prediction:
            logger.info(f"Found ML prediction for {symbol_id} on {prediction.jprediction_date} with trend: {prediction.predicted_trend}")
            return prediction.to_dict()
        else:
            logger.warning(f"No ML prediction found for symbol_id: {symbol_id}")
            return None
    except Exception as e:
        logger.error(f"Error fetching ML prediction for symbol {symbol_id}: {e}", exc_info=True)
        return None

def get_all_ml_predictions() -> list[dict]:
    """
    Retrieves all ML predictions, ordered by prediction date descending.
    """
    logger.info("Fetching all ML predictions.")
    try:
        predictions = MLPrediction.query.order_by(MLPrediction.prediction_date.desc()).all()

        result = [p.to_dict() for p in predictions]
        logger.info(f"Retrieved {len(result)} ML predictions.")
        return result
    except Exception as e:
        logger.error(f"Error fetching all ML predictions: {e}", exc_info=True)
        return []

def generate_and_save_predictions_for_watchlist(prediction_date_greg=None, prediction_period_days=7):
    """
    تولید و ذخیره پیش‌بینی‌های ML برای نمادها.
    """
    if prediction_date_greg is None:
        prediction_date_greg = date.today()

    jprediction_date = convert_gregorian_to_jalali(prediction_date_greg)
    if jprediction_date is None:
        logger.error(f"خطا در تبدیل تاریخ میلادی {prediction_date_greg} به جلالی. نمی‌توان پیش‌بینی را ادامه داد.")
        return False, "تاریخ نامعتبر"

    logger.info(f"در حال تولید پیش‌بینی‌های ML برای تاریخ {jprediction_date} (میلادی: {prediction_date_greg})...")

    try:
        all_symbols = db.session.query(ComprehensiveSymbolData).all()
        logger.info(f"تعداد کل نمادهای یافت شده: {len(all_symbols)}")
    except Exception as e:
        logger.error(f"خطا در واکشی لیست نمادها از دیتابیس: {e}")
        return False, f"خطا در واکشی نمادها: {e}"

    processed_count = 0
    for symbol_data in all_symbols:
        symbol_id = symbol_data.symbol_id
        symbol_name = symbol_data.symbol_name

        existing_prediction = db.session.query(MLPrediction).filter_by(
            symbol_id=symbol_id,
            prediction_date=prediction_date_greg
        ).first()

        if existing_prediction:
            logger.info(f"پیش‌بینی برای نماد {symbol_name} ({symbol_id}) در تاریخ {jprediction_date} از قبل وجود دارد. پرش.")
            continue

        start_date_for_hist = prediction_date_greg - timedelta(days=200)

        try:
            historical_data_records = db.session.query(HistoricalData).filter(
                HistoricalData.symbol_id == symbol_id,
                HistoricalData.date >= start_date_for_hist,
                HistoricalData.date <= prediction_date_greg
            ).order_by(HistoricalData.date.asc()).all()

            if not historical_data_records:
                logger.warning(f"داده تاریخی کافی برای نماد {symbol_name} ({symbol_id}) برای پیش‌بینی یافت نشد.")
                continue

            historical_data_df = pd.DataFrame([r.to_dict() if hasattr(r, 'to_dict') else r.__dict__ for r in historical_data_records])
            historical_data_df['gregorian_date'] = pd.to_datetime(historical_data_df['date'])

            predicted_trend, prediction_probability = predict_trend_for_symbol(historical_data_df, symbol_id_for_logging=symbol_id)

            if predicted_trend is None:
                logger.warning(f"پیش‌بینی برای نماد {symbol_name} ({symbol_id}) انجام نشد (داده ناکافی یا خطا در ml_predictor).")
                continue


            model_version_str = "latest"

            new_prediction = MLPrediction(
                symbol_id=symbol_id,
                symbol_name=symbol_name,
                prediction_date=prediction_date_greg,
                jprediction_date=jprediction_date,
                prediction_period_days=prediction_period_days,
                predicted_trend=predicted_trend,
                prediction_probability=prediction_probability,
                signal_source='ML-Trend',
                model_version=model_version_str
            )
            db.session.add(new_prediction)
            processed_count += 1
            logger.info(f"پیش‌بینی برای {symbol_name} ({symbol_id}): روند {predicted_trend} با احتمال {prediction_probability:.2f} ذخیره شد.")

        except Exception as e:
            db.session.rollback()
            logger.error(f"خطا در پردازش نماد {symbol_name} ({symbol_id}): {e}", exc_info=True)
            continue

    try:
        db.session.commit()
        logger.info(f"فرآیند تولید پیش‌بینی‌های ML کامل شد. {processed_count} پیش‌بینی جدید ذخیره شد.")
        return True, f"فرآیند تولید پیش‌بینی‌های ML کامل شد. {processed_count} پیش‌بینی جدید ذخیره شد."
    except Exception as e:
        db.session.rollback()
        logger.error(f"خطا در commit کردن پیش‌بینی‌ها به دیتابیس: {e}", exc_info=True)
        return False, f"خطا در ذخیره پیش‌بینی‌ها: {e}"

def update_ml_prediction_outcomes():
    """
    به‌روزرسانی نتایج واقعی و دقت برای پیش‌بینی‌های ML گذشته.
    این تابع به صورت دوره‌ای (مثلاً روزانه) اجرا می‌شود تا بررسی کند آیا
    دوره پیش‌بینی برای هر پیش‌بینی 'فعال' به پایان رسیده است یا خیر.
    """
    logger.info("در حال شروع به‌روزرسانی نتایج پیش‌بینی‌های ML گذشته...")

    today_greg = date.today()
    updated_count = 0

    try:
        # 1. کوئری گرفتن برای پیش‌بینی‌های فعال که هنوز ارزیابی نشده‌اند
        # و تاریخ پایان دوره آن‌ها گذشته است.
        predictions_to_evaluate = MLPrediction.query.filter(
            MLPrediction.actual_trend_outcome == None, # هنوز ارزیابی نشده
            MLPrediction.prediction_date + MLPrediction.prediction_period_days <= today_greg # دوره پیش‌بینی به پایان رسیده
        ).all()

        logger.info(f"تعداد {len(predictions_to_evaluate)} پیش‌بینی برای ارزیابی یافت شد.")

        for prediction in predictions_to_evaluate:
            symbol_id = prediction.symbol_id
            symbol_name = prediction.symbol_name
            prediction_start_date = prediction.prediction_date
            prediction_period_days = prediction.prediction_period_days
            predicted_trend = prediction.predicted_trend

            # تاریخ پایان دوره پیش‌بینی
            evaluation_date = prediction_start_date + timedelta(days=prediction_period_days)

            # 2. واکشی قیمت واقعی در تاریخ پایان دوره
            # ما به قیمت بسته شدن (close) در تاریخ evaluation_date نیاز داریم.
            actual_data_record = db.session.query(HistoricalData).filter_by(
                symbol_id=symbol_id,
                date=evaluation_date
            ).first()

            if not actual_data_record:
                logger.warning(f"داده تاریخی برای نماد {symbol_name} ({symbol_id}) در تاریخ ارزیابی {evaluation_date} یافت نشد. نمی‌توان نتیجه را به‌روزرسانی کرد.")
                continue

            actual_price_at_period_end = actual_data_record.close # استفاده از قیمت بسته شدن

            # 3. تعیین روند واقعی (Uptrend, Downtrend, Sideways)
            # برای این کار، به قیمت بسته شدن در تاریخ شروع پیش‌بینی نیز نیاز داریم.
            start_price_record = db.session.query(HistoricalData).filter_by(
                symbol_id=symbol_id,
                date=prediction_start_date
            ).first()

            if not start_price_record:
                logger.warning(f"داده تاریخی برای نماد {symbol_name} ({symbol_id}) در تاریخ شروع پیش‌بینی {prediction_start_date} یافت نشد. نمی‌توان نتیجه را به‌روزرسانی کرد.")
                continue

            start_price = start_price_record.close # قیمت بسته شدن در تاریخ شروع پیش‌بینی

            # محاسبه درصد تغییر
            if start_price == 0: # جلوگیری از تقسیم بر صفر
                percentage_change = 0.0
            else:
                percentage_change = ((actual_price_at_period_end - start_price) / start_price) * 100

            # تعیین روند واقعی بر اساس آستانه‌هایی مشابه آنچه در train_model.py استفاده شد.
            DOWNTREND_THRESHOLD = -1.0 # مثلاً -1%
            UPTREND_THRESHOLD = 1.0    # مثلاً +1%

            actual_trend_outcome = 'Sideways'
            if percentage_change > UPTREND_THRESHOLD:
                actual_trend_outcome = 'Uptrend'
            elif percentage_change < DOWNTREND_THRESHOLD:
                actual_trend_outcome = 'Downtrend'

            # 4. مقایسه و به‌روزرسانی رکورد
            is_prediction_accurate = (predicted_trend == actual_trend_outcome)

            prediction.actual_price_at_period_end = actual_price_at_period_end
            prediction.actual_trend_outcome = actual_trend_outcome
            prediction.is_prediction_accurate = is_prediction_accurate
            prediction.updated_at = datetime.now() # به‌روزرسانی timestamp

            db.session.add(prediction) # اضافه کردن به سشن برای commit
            updated_count += 1
            logger.info(f"پیش‌بینی برای {symbol_name} ({symbol_id}) در تاریخ {prediction.jprediction_date} ارزیابی شد. روند پیش‌بینی شده: {predicted_trend}, روند واقعی: {actual_trend_outcome}, دقت: {is_prediction_accurate}")

        db.session.commit()
        logger.info(f"فرآیند به‌روزرسانی نتایج پیش‌بینی‌های ML کامل شد. {updated_count} پیش‌بینی به‌روزرسانی شد.")
        return True, f"فرآیند به‌روزرسانی نتایج پیش‌بینی‌های ML کامل شد. {updated_count} پیش‌بینی به‌روزرسانی شد."

    except Exception as e:
        db.session.rollback()
        logger.error(f"خطا در به‌روزرسانی نتایج پیش‌بینی‌های ML: {e}", exc_info=True)
        return False, f"خطا در به‌روزرسانی نتایج پیش‌بینی‌ها: {e}"