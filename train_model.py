# -*- coding: utf-8 -*-
# train_model.py - اسکریپت آموزش و ارزیابی مدل یادگیری ماشین برای پیش‌بینی روند سهام

import pandas as pd
import numpy as np
import jdatetime
from datetime import datetime
import logging
import os
import joblib
import sys
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, accuracy_score
from sklearn.preprocessing import StandardScaler
from sqlalchemy.orm import sessionmaker
from config import Config
from sqlalchemy import create_engine, text

# --- تنظیمات لاگ‌نویسی ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- تنظیمات مسیردهی ---
current_script_dir = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(current_script_dir)

if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# تشخیص محیط
MODELS_DIR = '/app/models' if os.path.exists('/app') else os.path.join(PROJECT_ROOT, "models")
os.makedirs(MODELS_DIR, exist_ok=True)

print("✅ محیط تشخیص داده شد:")
print("📁 مسیر پروژه:", PROJECT_ROOT)
print("🤖 مسیر مدل‌ها:", MODELS_DIR)
print("🏠 محیط:", "Docker" if os.path.exists('/app') else "Local Machine")

# مسیرهای دیگر
SERVICES_PATH = os.path.join(PROJECT_ROOT, 'services')
DATA_PATH = os.path.join(PROJECT_ROOT, 'data')

if SERVICES_PATH not in sys.path:
    sys.path.insert(0, SERVICES_PATH)

# ایمپورت مدل‌ها
try:
    from models import HistoricalData, ComprehensiveSymbolData
except ImportError as e:
    logger.error(f"خطا در ایمپورت ماژول‌ها: {e}")
    sys.exit(1)

# دیتابیس
DATABASE_URL = f"sqlite:///{os.path.join(PROJECT_ROOT, 'app.db')}"
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)

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

# --- مهندسی ویژگی ---
def _perform_feature_engineering(df_symbol_hist, symbol_id_for_logging="N/A"):
    df_processed = df_symbol_hist.sort_values(by='gregorian_date').set_index('gregorian_date').copy()

    # اندیکاتورها
    df_processed['rsi'] = calculate_rsi(df_processed['close'])
    macd_line, signal_line, _ = calculate_macd(df_processed['close'])
    df_processed['macd'] = macd_line
    df_processed['signal_line'] = signal_line
    df_processed['sma_20'] = calculate_sma(df_processed['close'], window=20)
    df_processed['sma_50'] = calculate_sma(df_processed['close'], window=50)
    df_processed['volume_ma_5_day'] = calculate_volume_ma(df_processed['volume'], window=5)
    df_processed['atr'] = calculate_atr(df_processed['high'], df_processed['low'], df_processed['close'])

    # Stochastic Oscillator
    window_stoch = 14
    df_processed['lowest_low_stoch'] = df_processed['low'].rolling(window=window_stoch).min()
    df_processed['highest_high_stoch'] = df_processed['high'].rolling(window=window_stoch).max()
    denominator_stoch = df_processed['highest_high_stoch'] - df_processed['lowest_low_stoch']
    df_processed['%K'] = ((df_processed['close'] - df_processed['lowest_low_stoch']) / denominator_stoch.replace(0, np.nan)) * 100
    df_processed['%D'] = df_processed['%K'].rolling(window=3).mean()

    # OBV
    close_shifted = df_processed['close'].shift(1)
    volume_numeric = pd.to_numeric(df_processed['volume'], errors='coerce').fillna(0)
    df_processed['obv'] = (np.where(df_processed['close'] > close_shifted, volume_numeric,
                                   np.where(df_processed['close'] < close_shifted, -volume_numeric, 0))).cumsum()

    # تغییرات قیمت و حجم
    df_processed['price_change_1d'] = df_processed['close'].pct_change()
    df_processed['volume_change_1d'] = df_processed['volume'].pct_change()
    df_processed['price_change_3d'] = df_processed['close'].pct_change(periods=3)
    df_processed['volume_change_3d'] = df_processed['volume'].pct_change(periods=3)
    df_processed['price_change_5d'] = df_processed['close'].pct_change(periods=5)
    df_processed['volume_change_5d'] = df_processed['volume'].pct_change(periods=5)

    # نسبت قدرت خریدار حقیقی
    buy_i_vol = pd.to_numeric(df_processed['buy_i_volume'], errors='coerce').fillna(0)
    sell_i_vol = pd.to_numeric(df_processed['sell_i_volume'], errors='coerce').fillna(0)
    buy_count_i = pd.to_numeric(df_processed['buy_count_i'], errors='coerce').fillna(0)
    sell_count_i = pd.to_numeric(df_processed['sell_count_i'], errors='coerce').fillna(0)
    denominator_buy_power = (sell_i_vol * sell_count_i)
    df_processed['individual_buy_power_ratio'] = (buy_i_vol * buy_count_i) / denominator_buy_power.replace(0, np.nan)

    df_processed.replace([np.inf, -np.inf], np.nan, inplace=True)
    df_processed = df_processed.ffill().bfill().fillna(0)

    # انتخاب ویژگی‌ها
    feature_columns = [
        'open', 'high', 'low', 'close', 'volume', 'num_trades',
        'rsi', 'macd', 'signal_line', 'sma_20', 'sma_50', 'volume_ma_5_day', 'atr',
        '%K', '%D', 'obv',
        'price_change_1d', 'volume_change_1d',
        'price_change_3d', 'volume_change_3d',
        'price_change_5d', 'volume_change_5d',
        'individual_buy_power_ratio',
        'buy_count_i', 'sell_count_i', 'buy_i_volume', 'sell_i_volume',
        'zd1', 'qd1', 'pd1', 'zo1', 'qo1', 'po1',
        'zd2', 'qd2', 'pd2', 'zo2', 'qo2', 'po2',
        'zd3', 'qd3', 'pd3', 'zo3', 'qo3', 'po3',
        'zd4', 'qd4', 'pd4', 'zo4', 'qo4', 'po4',
        'zd5', 'qd5', 'pd5', 'zo5', 'qo5', 'po5'
    ]
    features_df = df_processed[feature_columns].copy()
    features_df.fillna(0, inplace=True)
    features_df.replace([np.inf, -np.inf], 0, inplace=True)
    return features_df

# --- آموزش مدل ---
def train_model():
    logger.info("در حال اتصال به دیتابیس و بارگذاری داده‌ها ...")
    CHUNK_SIZE = 10000
    all_chunks_df = []
    try:
        query = "SELECT * FROM stock_data ORDER BY symbol_id, date"
        with engine.connect() as conn:
            result_proxy = conn.execute(text(query))
            while True:
                chunk_records = result_proxy.fetchmany(CHUNK_SIZE)
                if not chunk_records:
                    break
                chunk_df = pd.DataFrame(chunk_records, columns=result_proxy.keys())
                logger.info(f"دسته جدید با {len(chunk_df)} ردیف بارگذاری شد.")
                all_chunks_df.append(chunk_df)

        if not all_chunks_df:
            logger.error("هیچ داده‌ای در دیتابیس نیست.")
            return

        df_hist = pd.concat(all_chunks_df, ignore_index=True)
        logger.info(f"تعداد کل نقاط داده: {len(df_hist)}")
        df_hist['gregorian_date'] = pd.to_datetime(df_hist['date'])
        df_hist.drop(columns=['_sa_instance_state'], errors='ignore', inplace=True)

        numeric_cols = ['open', 'high', 'low', 'close', 'final', 'yesterday_price', 'volume', 'value', 'num_trades',
                         'plc', 'plp', 'pcc', 'pcp', 'mv',
                         'buy_count_i', 'buy_count_n', 'sell_count_i', 'sell_count_n',
                         'buy_i_volume', 'buy_n_volume', 'sell_i_volume', 'sell_n_volume',
                         'zd1', 'qd1', 'pd1', 'zo1', 'qo1', 'po1',
                         'zd2', 'qd2', 'pd2', 'zo2', 'qo2', 'po2',
                         'zd3', 'qd3', 'pd3', 'zo3', 'qo3', 'po3',
                         'zd4', 'qd4', 'pd4', 'zo4', 'qo4', 'po4',
                         'zd5', 'qd5', 'pd5', 'zo5', 'qo5', 'po5']
        for col in numeric_cols:
            if col in df_hist.columns:
                df_hist[col] = pd.to_numeric(df_hist[col], errors='coerce')

        df_hist.dropna(subset=['close', 'volume', 'high', 'low', 'open'], inplace=True)

        # مهندسی ویژگی
        all_features_df = pd.DataFrame()
        for symbol_id in df_hist['symbol_id'].unique():
            df_symbol = df_hist[df_hist['symbol_id'] == symbol_id].copy()
            # اصلاح: بررسی تعداد کافی داده قبل از پردازش
            if len(df_symbol) < 60:
                logger.warning(f"پرش از نماد {symbol_id}: داده کافی ({len(df_symbol)} روز) برای محاسبه ویژگی‌ها وجود ندارد.")
                continue
            features_df = _perform_feature_engineering(df_symbol, symbol_id)
            if features_df.empty:
                continue
            features_df['symbol_id'] = symbol_id
            features_df['jdate'] = df_symbol.set_index('gregorian_date').loc[features_df.index, 'jdate']
            features_df['close_hist'] = df_symbol.set_index('gregorian_date').loc[features_df.index, 'close']
            all_features_df = pd.concat([all_features_df, features_df], ignore_index=False)

        if all_features_df.empty:
            logger.error("داده‌ای برای آموزش باقی نمانده.")
            return

        all_features_df.sort_values(by=['symbol_id', 'gregorian_date'], inplace=True)
        all_features_df['future_close'] = all_features_df.groupby('symbol_id')['close_hist'].shift(-7)
        all_features_df['percentage_change'] = ((all_features_df['future_close'] - all_features_df['close_hist']) / all_features_df['close_hist']) * 100
        
        # اصلاح: حذف ردیف‌های دارای NaN پس از مهندسی ویژگی و برچسب‌گذاری
        initial_count = len(all_features_df)
        all_features_df.dropna(subset=['percentage_change'], inplace=True)
        dropped_count = initial_count - len(all_features_df)
        logger.info(f"تعداد کل نقاط داده آموزشی پس از تعریف برچسب: {len(all_features_df)} (حذف شده: {dropped_count})")

        if all_features_df.empty:
            logger.error("داده‌ای برای آموزش باقی نمانده.")
            return

        lower_bound = all_features_df['percentage_change'].quantile(0.33)
        upper_bound = all_features_df['percentage_change'].quantile(0.66)
        def get_trend(change):
            if change > upper_bound:
                return 'Uptrend'
            elif change < lower_bound:
                return 'Downtrend'
            else:
                return 'Sideways'
        all_features_df['trend'] = all_features_df['percentage_change'].apply(get_trend)

        logger.info(f"آستانه نزولی (Quantile 33%): {lower_bound:.2f}%")
        logger.info(f"آستانه صعودی (Quantile 66%): {upper_bound:.2f}%")
        logger.info("توزیع کلاس‌ها در داده‌های آموزشی (پس از برچسب‌گذاری با کوانتایل):")
        logger.info(all_features_df['trend'].value_counts(normalize=True))

        X = all_features_df.drop(columns=['symbol_id', 'jdate', 'close_hist', 'future_close', 'percentage_change', 'trend'])
        y = all_features_df['trend']

        # ترکیب X و y
        # اصلاح: نام دیتافریم را به df_combined تغییر دادم تا با لاگ‌های قبلی مطابقت نداشته باشد و سردرگمی ایجاد نشود.
        df_combined = X.copy()
        df_combined["target"] = y
        unique_dates = df_combined.index.unique().sort_values()

        initial_train_window_days = getattr(Config, "ML_INITIAL_TRAIN_DAYS", 252)
        test_window_days = getattr(Config, "ML_TEST_DAYS", 21)
        step_window_days = getattr(Config, "ML_STEP_DAYS", 21)

        fold_reports, fold_accuracies = [], []
        latest_model_path = os.path.join(MODELS_DIR, 'latest_model.joblib')
        latest_scaler_path = os.path.join(MODELS_DIR, 'latest_scaler.joblib')
        latest_feature_names_path = os.path.join(MODELS_DIR, 'latest_feature_names.joblib')
        latest_class_labels_path = os.path.join(MODELS_DIR, 'latest_class_labels.joblib')

        logger.info("در حال شروع آموزش مدل ML با اعتبارسنجی Walk-Forward...")

        if len(unique_dates) < initial_train_window_days + test_window_days:
            logger.info("داده کافی برای اعتبارسنجی Walk-Forward وجود ندارد. آموزش بر روی کل مجموعه داده...")
            scaler = StandardScaler()
            X_scaled = scaler.fit_transform(X)
            model = RandomForestClassifier(n_estimators=200, random_state=42, class_weight='balanced', n_jobs=-1)
            model.fit(X_scaled, y)
            final_model, final_scaler = model, scaler
        else:
            start_idx_for_test_window = initial_train_window_days
            while start_idx_for_test_window + test_window_days <= len(unique_dates):
                train_end_date = unique_dates[start_idx_for_test_window - 1]
                test_start_date = unique_dates[start_idx_for_test_window]
                test_end_date = unique_dates[min(start_idx_for_test_window + test_window_days - 1, len(unique_dates) - 1)]

                # اصلاح برای رفع خطای ناهمگامی داده‌ها
                # استفاده از loc برای انتخاب داده‌ها بر اساس تاریخ
                train_data = df_combined.loc[df_combined.index <= train_end_date].copy()
                test_data = df_combined.loc[(df_combined.index >= test_start_date) & (df_combined.index <= test_end_date)].copy()
                
                # اصلاح: مطمئن می‌شویم که داده‌های آموزشی و تست خالی نباشند.
                if train_data.empty or test_data.empty:
                    logger.warning(f"داده آموزشی یا تست برای بازه {train_end_date} تا {test_end_date} خالی است. پرش به مرحله بعد.")
                    start_idx_for_test_window += step_window_days
                    continue

                X_train_fold = train_data.drop(columns=["target"])
                y_train_fold = train_data["target"]
                X_test_fold = test_data.drop(columns=["target"])
                y_test_fold = test_data["target"]

                # اصلاح برای رفع خطای ناهمگامی داده‌ها:
                # اطمینان از اینکه X و y در هر fold همگام هستند
                combined_train_fold = pd.concat([X_train_fold, y_train_fold], axis=1).dropna()
                X_train_fold_clean = combined_train_fold.drop(columns=["target"])
                y_train_fold_clean = combined_train_fold["target"]

                combined_test_fold = pd.concat([X_test_fold, y_test_fold], axis=1).dropna()
                X_test_fold_clean = combined_test_fold.drop(columns=["target"])
                y_test_fold_clean = combined_test_fold["target"]

                # اطمینان از اینکه داده‌های تمیز شده خالی نیستند
                if X_train_fold_clean.empty or X_test_fold_clean.empty:
                    logger.warning(f"داده تمیز شده برای بازه {train_end_date} تا {test_end_date} خالی است. پرش به مرحله بعد.")
                    start_idx_for_test_window += step_window_days
                    continue

                scaler = StandardScaler()
                X_train_scaled = scaler.fit_transform(X_train_fold_clean)
                X_test_scaled = scaler.transform(X_test_fold_clean)

                model = RandomForestClassifier(n_estimators=200, random_state=42, class_weight='balanced', n_jobs=-1)
                
                # اصلاح: استفاده از داده‌های تمیز شده برای fit
                model.fit(X_train_scaled, y_train_fold_clean)
                
                # اصلاح: استفاده از داده‌های تمیز شده برای predict
                y_pred_fold = model.predict(X_test_scaled)
                
                fold_reports.append(classification_report(y_test_fold_clean, y_pred_fold, output_dict=True, zero_division=0))
                fold_accuracies.append(accuracy_score(y_test_fold_clean, y_pred_fold))
                
                logger.info(f"✅ آموزش و ارزیابی برای بازه تست {test_start_date.strftime('%Y-%m-%d')} تا {test_end_date.strftime('%Y-%m-%d')} با موفقیت انجام شد.")
                logger.info(f"دقت: {fold_accuracies[-1]:.2%}")
                
                start_idx_for_test_window += step_window_days
            
            # مدل نهایی از آخرین Fold
            final_model, final_scaler = model, scaler

        # ذخیره‌سازی
        joblib.dump(final_model, latest_model_path)
        joblib.dump(X.columns.tolist(), latest_feature_names_path)
        joblib.dump(final_model.classes_.tolist(), latest_class_labels_path)
        joblib.dump(final_scaler, latest_scaler_path)
        logger.info("✅ مدل و فایل‌های جانبی ذخیره شدند.")

        # لاگ فیچر ایمپورتنس
        try:
            importances = pd.Series(final_model.feature_importances_, index=X.columns)
            logger.info("ویژگی‌های مهم مدل:")
            logger.info(importances.sort_values(ascending=False).head(20))
        except Exception:
            pass

    except Exception as e:
        logger.error(f"خطای کلی در فرآیند آموزش: {e}", exc_info=True)

if __name__ == "__main__":
    train_model()