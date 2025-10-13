# -*- coding: utf-8 -*-
# ml_predictor.py - ماژول برای بارگذاری مدل ML و انجام پیش‌بینی‌ها
#
# این فایل طوری بازنویسی شده که با خروجی‌های فعلی پروژه (فایل‌های joblib)
# سازگار باشد و در عین حال تا جای ممکن مقاوم در برابر تغییرات ساختار فایل‌ها.
#
# ویژگی‌ها:
# - بارگذاری امن مدل و فایل‌های جانبی (Scaler, feature names, class labels)
# - مهندسی ویژگی دقیق مطابق با train_model.py
# - پیش‌بینی تک-نماد و دسته‌ای
# - توابع کمکی برای اعتبارسنجی آرایه‌ی ورودی، مدیریت فیچرهای گمشده، لاگینگ کامل
#
# نکته: این ماژول برای سازگاری با train_model.py طوری نوشته شده که انتظار دارد
# فایل‌های زیر در پوشه MODELS_DIR موجود باشند:
#   - latest_model.joblib
#   - latest_scaler.joblib
#   - latest_feature_names.joblib
#   - latest_class_labels.joblib
#
# اگر سیستم شما هنوز از نام‌های timestamped (.pkl) استفاده می‌کند، در پایین
# یک fallback هم پیاده‌سازی شده که فایل‌های timestamped را پیدا کند و در صورت
# نبودن فایل‌های latest_* از آن‌ها استفاده کند.
#
# حتماً قبل از اجرا: اطمینان حاصل کنید train_model.py مدل‌ها را در MODELS_DIR ذخیره کرده است.
#
# Author: بازنویسی شده توسط دستیار توسعه دهنده
# Date: 2025-09
#

from __future__ import annotations

import os
import sys
import logging
import joblib
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Tuple, Optional, List, Dict, Any

import jdatetime

# sklearn import only for type hints (predict_proba etc.)
from sklearn.base import BaseEstimator
from sklearn.preprocessing import StandardScaler

# توابع کمکی مهندسی ویژگی ممکن است در services.utils هم وجود داشته باشند.
# این ماژول سعی می‌کند ابتدا از آن فایل‌ها ایمپورت کند؛ در صورت ناموفق بودن
# پیاده‌سازی‌های محلی جایگزین استفاده خواهد شد (تا هماهنگ با train_model.py باشد).
#try:
    #from services.utils import calculate_rsi, calculate_macd, calculate_sma, calculate_volume_ma, calculate_atr
    #_USING_UTILS_IMPL = True
#except Exception:
    #_USING_UTILS_IMPL = False
    # پیاده‌سازی محلی احتیاطی (تا حد ممکن مطابق با train_model.py)
    
# --- محاسبات اندیکاتورها ---
def calculate_rsi(series, window=14):
    if not isinstance(series, pd.Series):
        series = pd.Series(series)
    delta = series.diff()
    gains = delta.where(delta > 0, 0)
    losses = -delta.where(delta < 0, 0)
    avg_gain = gains.ewm(com=window - 1, min_periods=window).mean()
    avg_loss = losses.ewm(com=window - 1, min_periods=window).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    rsi = 100 - (100 / (1 + rs))
    return rsi

def calculate_macd(series, fast_window=12, slow_window=26, signal_window=9):
    if not isinstance(series, pd.Series):
        series = pd.Series(series)
    ema_fast = series.ewm(span=fast_window, min_periods=fast_window).mean()
    ema_slow = series.ewm(span=slow_window, min_periods=slow_window).mean()
    macd_line = ema_fast - ema_slow
    signal_line = macd_line.ewm(span=signal_window, min_periods=signal_window).mean()
    macd_histogram = macd_line - signal_line
    return macd_line, signal_line, macd_histogram

def calculate_sma(series, window=20):
    if not isinstance(series, pd.Series):
        series = pd.Series(series)
    return series.rolling(window=window).mean()

def calculate_volume_ma(series, window=5):
    if not isinstance(series, pd.Series):
        series = pd.Series(series)
    return series.rolling(window=window).mean()

def calculate_atr(high, low, close, window=14):
    tr1 = high - low
    tr2 = abs(high - close.shift())
    tr3 = abs(low - close.shift())
    true_range = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    atr = true_range.ewm(com=window - 1, min_periods=window).mean()
    return atr


# logging تنظیم
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# مسیر پروژه و پوشه مدل‌ها
current_script_dir = os.path.dirname(os.path.abspath(__file__))
# فرض اینکه ما در ریشه پروژه اجرا می‌کنیم؛ اگر نه، کاربر باید PYTHONPATH را تنظیم کند.
PROJECT_ROOT = os.path.abspath(current_script_dir)

if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# تشخیص محیط Local یا Docker و تعیین مسیر مدل‌ها
MODELS_DIR = '/app/models' if os.path.exists('/app') else os.path.join(PROJECT_ROOT, "models")
os.makedirs(MODELS_DIR, exist_ok=True)

logger.info("محیط مدل‌ها: %s", MODELS_DIR)


# ---- توابع کمکی برای یافتن و بارگذاری مدل‌ها ----

def _list_files_with_prefix_suffix(directory: str, prefix: str, suffix: str) -> List[str]:
    """لیست فایل‌ها در دایرکتوری که با prefix و suffix مطابقت دارند."""
    try:
        return [f for f in os.listdir(directory) if f.startswith(prefix) and f.endswith(suffix)]
    except Exception as e:
        logger.debug("خطا در لیست کردن دایرکتوری %s: %s", directory, e)
        return []


def _find_timestamped_files(model_dir: str) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
    """
    جستجو برای فایل‌های قدیمی‌تر با قالب timestamped (trained_ml_model_{ts}.pkl).
    این تابع برای حالت fallback استفاده می‌شود اگر latest_*.joblib موجود نباشند.
    """
    # سازگار با train_model.py قدیمی (.pkl)
    candidate_models = _list_files_with_prefix_suffix(model_dir, 'trained_ml_model_', '.pkl')
    if not candidate_models:
        logger.debug("هیچ فایل timestamped .pkl در %s یافت نشد.", model_dir)
        return None, None, None, None

    # استخراج timestamp
    timestamps = []
    for f in candidate_models:
        ts_str = f[len('trained_ml_model_'):-len('.pkl')]
        try:
            ts = datetime.strptime(ts_str, "%Y%m%d_%H%M%S")
            timestamps.append((ts, ts_str))
        except Exception:
            continue

    if not timestamps:
        logger.debug("هیچ timestamp معتبر در نام فایل‌های مدل یافت نشد.")
        return None, None, None, None

    latest_ts_str = max(timestamps)[1]
    model_path = os.path.join(model_dir, f'trained_ml_model_{latest_ts_str}.pkl')
    feature_names_path = os.path.join(model_dir, f'feature_names_{latest_ts_str}.pkl')
    class_labels_map_path = os.path.join(model_dir, f'class_labels_map_{latest_ts_str}.pkl')
    scaler_path = os.path.join(model_dir, f'scaler_{latest_ts_str}.pkl')

    if all(os.path.exists(p) for p in [model_path, feature_names_path, class_labels_map_path, scaler_path]):
        logger.debug("فایل‌های timestamped یافت شد برای timestamp=%s", latest_ts_str)
        return model_path, feature_names_path, class_labels_map_path, scaler_path
    else:
        logger.debug("فایل‌های timestamped به طور کامل وجود ندارند.")
        return None, None, None, None


def _find_latest_joblib_files(model_dir: str) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
    """
    بررسی وجود latest_*.joblib — مسیرهای ثابت که train_model.py فعلی تولید می‌کند.
    """
    model_path = os.path.join(model_dir, 'latest_model.joblib')
    scaler_path = os.path.join(model_dir, 'latest_scaler.joblib')
    features_path = os.path.join(model_dir, 'latest_feature_names.joblib')
    labels_path = os.path.join(model_dir, 'latest_class_labels.joblib')

    if all(os.path.exists(p) for p in [model_path, scaler_path, features_path, labels_path]):
        return model_path, features_path, labels_path, scaler_path
    else:
        return None, None, None, None


# تلاش برای بارگذاری: ابتدا latest_* (joblib)، اگر نبود fallback به timestamped .pkl
_MODEL: Optional[BaseEstimator] = None
_SCALER: Optional[StandardScaler] = None
_FEATURE_NAMES: Optional[List[str]] = None
_CLASS_LABELS_MAP: Optional[List[str]] = None
_MODEL_SOURCE_INFO: Dict[str, Any] = {}

# بارگذاری مدل با منطق fallback
def _load_model_and_artifacts(model_dir: str = MODELS_DIR):
    global _MODEL, _SCALER, _FEATURE_NAMES, _CLASS_LABELS_MAP, _MODEL_SOURCE_INFO

    # 1) تلاش برای latest joblib
    model_path, features_path, labels_path, scaler_path = _find_latest_joblib_files(model_dir)
    if model_path:
        try:
            _MODEL = joblib.load(model_path)
            _SCALER = joblib.load(scaler_path)
            _FEATURE_NAMES = joblib.load(features_path)
            _CLASS_LABELS_MAP = joblib.load(labels_path)
            _MODEL_SOURCE_INFO = {"type": "joblib_latest", "model_path": model_path}
            logger.info("مدل و آثار (joblib latest) با موفقیت بارگذاری شدند. مسیر: %s", model_path)
            return
        except Exception as e:
            logger.error("خطا در بارگذاری فایل‌های latest_*.joblib: %s", e, exc_info=True)
            # ادامه می‌دهیم تا fallback را امتحان کنیم

    # 2) fallback: فایل‌های timestamped .pkl
    model_path, features_path, labels_path, scaler_path = _find_timestamped_files(model_dir)
    if model_path:
        try:
            _MODEL = joblib.load(model_path)
            _SCALER = joblib.load(scaler_path)
            _FEATURE_NAMES = joblib.load(features_path)
            _CLASS_LABELS_MAP = joblib.load(labels_path)
            _MODEL_SOURCE_INFO = {"type": "pkl_timestamped", "model_path": model_path}
            logger.info("مدل و آثار (timestamped .pkl) با موفقیت بارگذاری شدند. مسیر: %s", model_path)
            return
        except Exception as e:
            logger.error("خطا در بارگذاری فایل‌های timestamped (.pkl): %s", e, exc_info=True)

    # 3) هیچ چیزی پیدا نشد
    logger.error("هیچ فایل مدل آموزش‌دیده کامل (joblib latest یا timestamped .pkl) در %s یافت نشد.", model_dir)
    raise FileNotFoundError(f"مدل آموزش‌دیده کامل در {model_dir} یافت نشد. ابتدا train_model.py را اجرا کنید.")


# اجرای بارگذاری در زمان import
try:
    _load_model_and_artifacts()
except Exception as e:
    # بالا بودن سطح لاگ باعث می‌شود پیام واضح‌تر دیده شود؛ اما ما این exception را بزرگتر throw نمی‌کنیم
    # تا کاربر بتواند ماژول را import کند و خودش تصمیم بگیرد. در صورت نیاز می‌توانیم raise کنیم.
    logger.exception("بارگذاری مدل در import با خطا مواجه شد: %s", e)
    # اگر نیاز به رفتار سخت‌گیرانه دارید، uncomment کنید:
    # raise


# ---- مهندسی ویژگی مخصوص پیش‌بینی (باید مطابق train_model.py باشد) ----

def _perform_feature_engineering_for_prediction(df_symbol_hist: pd.DataFrame, symbol_id_for_logging: str = "N/A") -> pd.DataFrame:
    """
    انجام مهندسی ویژگی روی داده تاریخی یک نماد برای ورودی مدل.
    خروجی: DataFrame که ستون‌هایش حداقل شامل subset از _FEATURE_NAMES باشد؛
    در صورت نبودن فیچرهایی که مدل انتظار دارد، آن‌ها با صفر پر می‌شوند.
    """
    if df_symbol_hist is None or df_symbol_hist.empty:
        return pd.DataFrame()

    # اطمینان از اینکه gregorian_date وجود دارد
    if 'gregorian_date' not in df_symbol_hist.columns:
        # اگر ستون date وجود دارد سعی کنیم آنرا به gregorian_date تبدیل کنیم
        if 'date' in df_symbol_hist.columns:
            df_symbol_hist['gregorian_date'] = pd.to_datetime(df_symbol_hist['date'])
        else:
            logger.warning("برای نماد %s ستون gregorian_date یا date یافت نشد؛ تلاش برای ادامه...", symbol_id_for_logging)
            # سعی کنیم index را به عنوان تاریخ استفاده کنیم
            try:
                df_symbol_hist = df_symbol_hist.reset_index()
                df_symbol_hist['gregorian_date'] = pd.to_datetime(df_symbol_hist['gregorian_date'])
            except Exception:
                return pd.DataFrame()

    df_processed = df_symbol_hist.sort_values(by='gregorian_date').set_index('gregorian_date').copy()

    # محاسبه اندیکاتورها (از توابع import شده یا جایگزین محلی استفاده می‌شود)
    try:
        df_processed.loc[:, 'rsi'] = calculate_rsi(df_processed['close'])
    except Exception as e:
        logger.debug("خطا در محاسبه RSI برای %s: %s", symbol_id_for_logging, e)
        df_processed.loc[:, 'rsi'] = 0

    try:
        macd_line, signal_line, _ = calculate_macd(df_processed['close'])
        df_processed.loc[:, 'macd'] = macd_line
        df_processed.loc[:, 'signal_line'] = signal_line
    except Exception as e:
        logger.debug("خطا در محاسبه MACD برای %s: %s", symbol_id_for_logging, e)
        df_processed.loc[:, 'macd'] = 0
        df_processed.loc[:, 'signal_line'] = 0

    try:
        df_processed.loc[:, 'sma_20'] = calculate_sma(df_processed['close'], window=20)
        df_processed.loc[:, 'sma_50'] = calculate_sma(df_processed['close'], window=50)
        df_processed.loc[:, 'volume_ma_5_day'] = calculate_volume_ma(df_processed['volume'], window=5)
    except Exception as e:
        logger.debug("خطا در محاسبه SMA/volume MA برای %s: %s", symbol_id_for_logging, e)
        df_processed.loc[:, 'sma_20'] = 0
        df_processed.loc[:, 'sma_50'] = 0
        df_processed.loc[:, 'volume_ma_5_day'] = 0

    try:
        df_processed.loc[:, 'atr'] = calculate_atr(df_processed['high'], df_processed['low'], df_processed['close'])
    except Exception as e:
        logger.debug("خطا در محاسبه ATR برای %s: %s", symbol_id_for_logging, e)
        df_processed.loc[:, 'atr'] = 0

    # Stochastic
    window_stoch = 14
    try:
        df_processed.loc[:, 'lowest_low_stoch'] = df_processed['low'].rolling(window=window_stoch).min()
        df_processed.loc[:, 'highest_high_stoch'] = df_processed['high'].rolling(window=window_stoch).max()
        denominator_stoch = df_processed['highest_high_stoch'] - df_processed['lowest_low_stoch']
        df_processed.loc[:, '%K'] = ((df_processed['close'] - df_processed['lowest_low_stoch']) / denominator_stoch.replace(0, np.nan)) * 100
        df_processed.loc[:, '%D'] = df_processed['%K'].rolling(window=3).mean()
    except Exception as e:
        logger.debug("خطا در محاسبه Stochastic برای %s: %s", symbol_id_for_logging, e)
        df_processed.loc[:, '%K'] = 0
        df_processed.loc[:, '%D'] = 0

    # OBV
    try:
        close_shifted = df_processed['close'].shift(1)
        volume_numeric = pd.to_numeric(df_processed['volume'], errors='coerce').fillna(0)
        df_processed.loc[:, 'obv'] = (np.where(df_processed['close'] > close_shifted, volume_numeric,
                                               np.where(df_processed['close'] < close_shifted, -volume_numeric, 0))).cumsum()
    except Exception as e:
        logger.debug("خطا در محاسبه OBV برای %s: %s", symbol_id_for_logging, e)
        df_processed.loc[:, 'obv'] = 0

    # لگ‌ها
    try:
        df_processed.loc[:, 'price_change_1d'] = df_processed['close'].pct_change()
        df_processed.loc[:, 'volume_change_1d'] = df_processed['volume'].pct_change()
        df_processed.loc[:, 'price_change_3d'] = df_processed['close'].pct_change(periods=3)
        df_processed.loc[:, 'volume_change_3d'] = df_processed['volume'].pct_change(periods=3)
        df_processed.loc[:, 'price_change_5d'] = df_processed['close'].pct_change(periods=5)
        df_processed.loc[:, 'volume_change_5d'] = df_processed['volume'].pct_change(periods=5)
    except Exception as e:
        logger.debug("خطا در محاسبه لگ‌ها برای %s: %s", symbol_id_for_logging, e)
        df_processed.loc[:, 'price_change_1d'] = 0
        df_processed.loc[:, 'volume_change_1d'] = 0
        df_processed.loc[:, 'price_change_3d'] = 0
        df_processed.loc[:, 'volume_change_3d'] = 0
        df_processed.loc[:, 'price_change_5d'] = 0
        df_processed.loc[:, 'volume_change_5d'] = 0

    # نسبت قدرت خریدار حقیقی
    try:
        buy_i_vol = pd.to_numeric(df_processed['buy_i_volume'], errors='coerce').fillna(0)
        sell_i_vol = pd.to_numeric(df_processed['sell_i_volume'], errors='coerce').fillna(0)
        buy_count_i = pd.to_numeric(df_processed['buy_count_i'], errors='coerce').fillna(0)
        sell_count_i = pd.to_numeric(df_processed['sell_count_i'], errors='coerce').fillna(0)
        denominator_buy_power = (sell_i_vol * sell_count_i)
        df_processed.loc[:, 'individual_buy_power_ratio'] = (buy_i_vol * buy_count_i) / denominator_buy_power.replace(0, np.nan)
    except Exception as e:
        logger.debug("خطا در محاسبه individual_buy_power_ratio برای %s: %s", symbol_id_for_logging, e)
        df_processed.loc[:, 'individual_buy_power_ratio'] = 0

    # پاکسازی NaN و Inf
    df_processed.replace([np.inf, -np.inf], np.nan, inplace=True)
    df_processed = df_processed.ffill().bfill()
    df_processed.fillna(0, inplace=True)

    # اطمینان از اینکه ترتیب ستون‌ها مطابق با مدل است و ویژگی‌های گمشده را پر می‌کنیم
    if _FEATURE_NAMES is None:
        # اگر هنوز مدل بارگذاری نشده، فقط تمام ستون‌های مجاز را برگردان
        return df_processed

    # انتخاب ستون‌های موجود در _FEATURE_NAMES به همان ترتیب
    available_features = [c for c in _FEATURE_NAMES if c in df_processed.columns]
    missing_features = [c for c in _FEATURE_NAMES if c not in df_processed.columns]

    features_df = df_processed[available_features].copy()
    if missing_features:
        logger.warning("برای نماد %s %d ویژگی گم شده یافت شد. آنها را با 0 پر می‌کنیم. missing=%s",
                         symbol_id_for_logging, len(missing_features), missing_features)
        for f in missing_features:
            features_df[f] = 0
        # ترتیب را تصحیح کن
        features_df = features_df[_FEATURE_NAMES]

    # اطمینان از اینکه هیچ NaN یا Inf باقی نمانده
    features_df.replace([np.inf, -np.inf], np.nan, inplace=True)
    features_df.fillna(0, inplace=True)

    return features_df


# ---- تابع اصلی پیش‌بینی تک‌نماد ----

def predict_trend_for_symbol(historical_data_df: pd.DataFrame, symbol_id_for_logging: str = "N/A") -> Tuple[Optional[str], Optional[float]]:
    """
    پیش‌بینی روند برای یک نماد بر اساس داده‌های تاریخی آن.
    خروجی: (predicted_trend_label, predicted_probability) یا (None, None) در صورت خطا/داده ناکافی.
    """
    if historical_data_df is None or historical_data_df.empty:
        logger.warning("هیچ داده‌ای برای پیش‌بینی برای نماد %s ارائه نشده.", symbol_id_for_logging)
        return None, None

    # حداقل تعداد روز مورد نیاز (مطابق train_model.py) 60 است
    if len(historical_data_df) < 60:
        logger.warning("داده تاریخی کافی برای نماد %s برای پیش‌بینی وجود ندارد (حداقل 60 روز نیاز است).", symbol_id_for_logging)
        return None, None

    # اطمینان از اینکه مدل بارگذاری شده
    if _MODEL is None or _SCALER is None or _FEATURE_NAMES is None:
        logger.error("مدل یا scaler یا نام فیچرها بارگذاری نشده‌اند. لطفاً ابتدا train_model.py را اجرا کنید یا فایل‌های latest_* را قرار دهید.")
        return None, None

    try:
        features_for_prediction = _perform_feature_engineering_for_prediction(historical_data_df.copy(), symbol_id_for_logging)
        if features_for_prediction is None or features_for_prediction.empty:
            logger.warning("برای نماد %s پس از مهندسی ویژگی داده‌ای برای پیش‌بینی باقی نماند.", symbol_id_for_logging)
            return None, None

        # برداشتن آخرین سطر (آخرین روز) برای پیش‌بینی
        latest_features = features_for_prediction.iloc[[-1]]

        # اطمینان از ترتیب ستون‌ها مطابق _FEATURE_NAMES
        try:
            latest_features = latest_features[_FEATURE_NAMES]
        except Exception:
            # اگر ترتیب فرق داشت، سعی کنیم ستون‌ها را مرتب کنیم یا پر کنیم
            for col in _FEATURE_NAMES:
                if col not in latest_features.columns:
                    latest_features[col] = 0
            latest_features = latest_features[_FEATURE_NAMES]

        # اعمال scaler
        latest_features_scaled = _SCALER.transform(latest_features)

        # پیش‌بینی احتمالات
        if hasattr(_MODEL, "predict_proba"):
            probabilities = _MODEL.predict_proba(latest_features_scaled)[0]
        else:
            # اگر مدل توانایی پیش‌بینی احتمال را ندارد (مثلاً بعضی از مدل‌ها)
            pred = _MODEL.predict(latest_features_scaled)
            probabilities = np.zeros(len(_MODEL.classes_))
            try:
                idx = list(_MODEL.classes_).index(pred[0])
                probabilities[idx] = 1.0
            except Exception:
                probabilities[0] = 1.0

        predicted_class_idx = int(np.argmax(probabilities))
        predicted_probability = float(probabilities[predicted_class_idx])
        predicted_trend_label = _MODEL.classes_[predicted_class_idx]

        return predicted_trend_label, predicted_probability
    except Exception as e:
        logger.error("خطا هنگام پیش‌بینی برای نماد %s: %s", symbol_id_for_logging, e, exc_info=True)
        return None, None


# ---- توابع دسته‌ای و کمکی ----

def predict_trends_for_dataframe(df_all_hist: pd.DataFrame, group_by_symbol_column: str = "symbol_id",
                                 symbol_name_col: Optional[str] = None) -> pd.DataFrame:
    """
    ورودی: DataFrame شامل داده‌ تاریخی برای چند نماد (ستون symbol_id ضروری است).
    خروجی: DataFrame شامل ستون‌های symbol_id, symbol_name (در صورت وجود), predicted_trend, probability, model_version
    """
    if df_all_hist is None or df_all_hist.empty:
        return pd.DataFrame()

    if group_by_symbol_column not in df_all_hist.columns:
        raise ValueError(f"ستونِ گروه‌بندی ({group_by_symbol_column}) در DataFrame وجود ندارد.")

    results = []
    for symbol_id, df_symbol in df_all_hist.groupby(group_by_symbol_column):
        try:
            trend, prob = predict_trend_for_symbol(df_symbol.copy(), symbol_id_for_logging=str(symbol_id))
            symbol_name = None
            if symbol_name_col and symbol_name_col in df_symbol.columns:
                symbol_name = df_symbol.iloc[-1].get(symbol_name_col)
            results.append({
                "symbol_id": symbol_id,
                "symbol_name": symbol_name,
                "predicted_trend": trend,
                "prediction_probability": prob,
                "model_version": _MODEL_SOURCE_INFO.get("model_path") if _MODEL_SOURCE_INFO else None
            })
        except Exception as e:
            logger.error("خطا در پردازش نماد %s در predict_trends_for_dataframe: %s", symbol_id, e, exc_info=True)
            results.append({
                "symbol_id": symbol_id,
                "symbol_name": None,
                "predicted_trend": None,
                "prediction_probability": None,
                "model_version": _MODEL_SOURCE_INFO.get("model_path") if _MODEL_SOURCE_INFO else None
            })

    return pd.DataFrame(results)


def get_model_info() -> Dict[str, Any]:
    """اطلاعاتی درباره مدل لود شده (مسیر، نوع، تعداد فیچرها، کلاس‌ها و غیره) برمی‌گرداند."""
    return {
        "loaded": _MODEL is not None,
        "model_path": _MODEL_SOURCE_INFO.get("model_path") if _MODEL_SOURCE_INFO else None,
        "model_type": type(_MODEL).__name__ if _MODEL else None,
        "n_features": len(_FEATURE_NAMES) if _FEATURE_NAMES else None,
        "feature_names": list(_FEATURE_NAMES)[:50] if _FEATURE_NAMES else None,
        "class_labels": list(_CLASS_LABELS_MAP) if _CLASS_LABELS_MAP else None
    }


# ---- اگر این فایل مستقیماً اجرا شد ----
if __name__ == "__main__":
    logger.info("ماژول ml_predictor.py به عنوان یک اسکریپت اصلی اجرا شد. برای استفاده باید import شود.")
    logger.info("فانکشن‌های پیش‌بینی (predict_trend_for_symbol و predict_trends_for_dataframe) اکنون آماده استفاده هستند.")
    logger.info("اطلاعات مدل بارگذاری شده: %s", get_model_info())