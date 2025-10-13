# services/golden_key_service.py

from extensions import db
from models import (
    ComprehensiveSymbolData,
    FundamentalData,
    HistoricalData,
    GoldenKeyResult,
)
import pandas as pd
import logging
from datetime import datetime
import jdatetime
import numpy as np
import json
from sqlalchemy import func

# تنظیمات لاگینگ برای این ماژول
logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

# وارد کردن توابع کاربردی ضروری
from services.utils import get_today_jdate_str
# وارد کردن توابع کاربردی اختیاری
try:
    from services.utils import calculate_smart_money_flow, check_candlestick_patterns
except Exception:
    calculate_smart_money_flow = None
    check_candlestick_patterns = None
    logger.debug("Optional utility functions (calculate_smart_money_flow, check_candlestick_patterns) not found.")

# ---------------------------------------------------------
# پیاده‌سازی محلی اندیکاتورها برای پایداری بیشتر
# ---------------------------------------------------------

def _to_series(x):
    if isinstance(x, pd.Series):
        return x.astype(float)
    return pd.Series(x).astype(float)

def compute_sma(series, window=20):
    s = _to_series(series)
    if s.empty or len(s) < window:
        return pd.Series([np.nan] * len(s), index=s.index)
    return s.rolling(window=window, min_periods=1).mean()

def compute_rsi(series, window=14):
    s = _to_series(series)
    if s.empty:
        return pd.Series([], dtype=float)
    delta = s.diff()
    up = delta.clip(lower=0)
    down = -delta.clip(upper=0)
    roll_up = up.ewm(span=window, adjust=False).mean()
    roll_down = down.ewm(span=window, adjust=False).mean()
    rs = roll_up / roll_down.replace(0, np.nan)
    rsi = 100 - (100 / (1 + rs))
    rsi.iloc[:window] = np.nan
    return rsi

def compute_macd(series, fast=12, slow=26, signal=9):
    s = _to_series(series)
    if s.empty:
        return pd.Series([]), pd.Series([]), pd.Series([])
    ema_fast = s.ewm(span=fast, adjust=False).mean()
    ema_slow = s.ewm(span=slow, adjust=False).mean()
    macd_line = ema_fast - ema_slow
    signal_line = macd_line.ewm(span=signal, adjust=False).mean()
    histogram = macd_line - signal_line
    return macd_line, signal_line, histogram

def compute_atr(high, low, close, window=14):
    h, l, c = _to_series(high), _to_series(low), _to_series(close)
    if h.empty or l.empty or c.empty:
        return pd.Series([], dtype=float)
    tr1 = h - l
    tr2 = (h - c.shift()).abs()
    tr3 = (l - c.shift()).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    atr = tr.ewm(span=window, adjust=False).mean()
    return atr

# ---------------------------------------------------------
# توابع کمکی برای فیلترها
# ---------------------------------------------------------

def is_resistance_breakout(df_high, current_close, days_window=20):
    if len(df_high) < days_window + 1: return False
    recent_highs = df_high.iloc[-days_window-1:-1].max()
    return pd.notna(current_close) and pd.notna(recent_highs) and current_close > recent_highs

def is_high_volume(current_volume, avg_volume, multiplier=1.5):
    return pd.notna(current_volume) and pd.notna(avg_volume) and current_volume > (avg_volume * multiplier)

def is_rsi_oversold(rsi_value, threshold=30):
    return pd.notna(rsi_value) and rsi_value < threshold

def is_macd_buy_signal(macd_line, signal_line):
    if len(macd_line) < 2 or len(signal_line) < 2: return False
    current_macd, current_signal = macd_line.iloc[-1], signal_line.iloc[-1]
    prev_macd, prev_signal = macd_line.iloc[-2], signal_line.iloc[-2]
    if pd.isna(current_macd) or pd.isna(current_signal) or pd.isna(prev_macd) or pd.isna(prev_signal): return False
    return (current_macd > current_signal) and (prev_macd <= prev_signal)

# ---------------------------------------------------------
# تابع اصلی تحلیل و ذخیره‌سازی کلید طلایی
# ---------------------------------------------------------

def run_golden_key_analysis_and_save(top_n_symbols=8):
    logger.info("Starting Golden Key analysis and saving process.")
    today_jdate_str = get_today_jdate_str()
    all_symbols = ComprehensiveSymbolData.query.all()

    if not all_symbols:
        logger.warning("No symbols found in ComprehensiveSymbolData. Cannot run Golden Key analysis.")
        return False, "No symbols found to analyze."

    # --- گام ۱: حذف تمام رکوردهای قبلی از جدول GoldenKeyResult ---
    try:
        deleted_count = db.session.query(GoldenKeyResult).delete()
        db.session.commit()
        logger.info(f"Successfully deleted {deleted_count} old records from GoldenKeyResult.")
    except Exception as e:
        db.session.rollback()
        logger.error(f"Error deleting all previous Golden Key results: {e}", exc_info=True)
        return False, f"DB cleanup failed: {str(e)}"
    # ---------------------------------------------------------------
    
    # منطق حذف صندوق‌ها در اینجا کاربردی ندارد چون کل جدول پاک شده است.
    fund_keywords = ["صندوق", "سرمایه گذاری", "اعتبار", "آتیه", "یکتا", "دارایی", "اختصاصی","کمند", "پاداش", "ارمغان"]
    fund_symbol_ids_to_delete = [s.symbol_id for s in all_symbols if any(keyword in s.symbol_name for keyword in fund_keywords)]
    
    current_day_results = []

    for symbol_data in all_symbols:
        symbol_id = symbol_data.symbol_id
        symbol_name = symbol_data.symbol_name

        if symbol_data.symbol_id in fund_symbol_ids_to_delete:
            continue

        # --- حذف واکشی P/E گروه که دیگر استفاده نمی‌شود ---
        # fundamental_record = FundamentalData.query.filter_by(symbol_id=symbol_data.symbol_id).first()
        # current_group_pe = fundamental_record.group_pe_ratio if fundamental_record and pd.notna(fundamental_record.group_pe_ratio) else np.nan
        # -------------------------------------------------

        historical_records = HistoricalData.query.filter_by(symbol_id=symbol_data.symbol_id).order_by(HistoricalData.jdate.asc()).all()
        if not historical_records or len(historical_records) < 60:
            continue

        df = pd.DataFrame([r.__dict__ for r in historical_records])
        numeric_cols = ['open', 'high', 'low', 'close', 'volume', 'buy_i_volume', 'buy_count_i', 'sell_i_volume', 'sell_count_i']
        
        existing_numeric_cols = [col for col in numeric_cols if col in df.columns]
        
        for col in existing_numeric_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            
        df.dropna(subset=['open', 'high', 'low', 'close', 'volume'], inplace=True)
        if len(df) < 60: continue

        # --- محاسبه اندیکاتورها ---
        rsi_series = compute_rsi(df['close'])
        macd_line, signal_line, _ = compute_macd(df['close'])
        sma_20 = compute_sma(df['close'], window=20)
        sma_50 = compute_sma(df['close'], window=50)
        volume_ma_20 = compute_sma(df['volume'], window=20)
        atr_series = compute_atr(df['high'], df['low'], df['close'], window=14)
        
        # --- استخراج آخرین مقادیر ---
        last_row = df.iloc[-1]
        current_close, current_volume = last_row['close'], last_row['volume']
        latest_rsi = rsi_series.iloc[-1] if not rsi_series.empty else np.nan
        latest_sma_20, latest_sma_50 = sma_20.iloc[-1], sma_50.iloc[-1]
        latest_vol_ma_20 = volume_ma_20.iloc[-1]
        
        smf_df = calculate_smart_money_flow(df) if calculate_smart_money_flow else pd.DataFrame()
        latest_ibp = smf_df['individual_buy_power'].iloc[-1] if not smf_df.empty else np.nan

        satisfied_filters, total_score, reason_phrases = [], 0, []

        # --- تعریف فیلترهای جدید و بهبودیافته (فیلتر P/E حذف شده است) ---
        filter_definitions = {
            "ورود پول هوشمند در کف قیمتی": {
                "func": lambda: is_rsi_oversold(latest_rsi, 35) and pd.notna(latest_ibp) and latest_ibp > 2.0,
                "score": 20, "reason": "ورود پول هوشمند سنگین در ناحیه اشباع فروش"
            },
            "کندل قدرتمند صعودی با حجم بالا": {
                "func": lambda: (last_row['close'] - last_row['open']) > 0.6 * (last_row['high'] - last_row['low']) and \
                                 current_close > last_row['open'] and \
                                 is_high_volume(current_volume, latest_vol_ma_20, 2.0),
                "score": 18, "reason": "کندل قدرتمند صعودی با افزایش چشمگیر حجم"
            },
            "تثبیت تقاطع طلایی": {
                "func": lambda: latest_sma_20 > latest_sma_50 and (sma_20.iloc[-2] <= sma_50.iloc[-2]) and \
                                 current_close > latest_sma_20,
                "score": 15, "reason": "تثبیت تقاطع طلایی و قرارگیری قیمت بالای میانگین‌ها"
            },
            "شکست مقاومت با قدرت خریدار": {
                "func": lambda: is_resistance_breakout(df['high'], current_close, 40) and pd.notna(latest_ibp) and latest_ibp > 1.5,
                "score": 15, "reason": "شکست مقاومت مهم ۴۰ روزه با حمایت پول هوشمند"
            },
            "واگرایی مثبت RSI + افزایش حجم": {
                "func": lambda: is_rsi_oversold(latest_rsi, 40) and is_high_volume(current_volume, latest_vol_ma_20, 1.8) and \
                                 (df['low'].iloc[-10:].min() < df['low'].iloc[-20:-10].min()) and \
                                 (rsi_series.iloc[-10:].min() > rsi_series.iloc[-20:-10].min()),
                "score": 12, "reason": "واگرایی مثبت RSI در روند نزولی با افزایش حجم"
            },
            "فشردگی نوسان (مقدمه حرکت انفجاری)": {
                "func": lambda: (atr_series.iloc[-1] < atr_series.iloc[-20:].min() * 1.1) and (latest_rsi > 50),
                "score": 10, "reason": "کاهش شدید نوسانات و قرارگیری در آستانه یک حرکت قوی"
            },
            "عبور MACD از خط سیگنال": {
                "func": lambda: is_macd_buy_signal(macd_line, signal_line) and macd_line.iloc[-1] < 0,
                "score": 8, "reason": "تقاطع صعودی MACD در محدوده منفی (سیگنال اولیه بازگشت)"
            }
        }

        for fname, finfo in filter_definitions.items():
            try:
                if finfo['func']():
                    satisfied_filters.append(fname)
                    total_score += finfo['score']
                    reason_phrases.append(finfo['reason'])
            except Exception:
                continue

        if total_score <= 0: continue

        reason_str = ", ".join(reason_phrases)
        
        result_payload = {
            "symbol_id": symbol_id, "symbol_name": symbol_name, "jdate": today_jdate_str,
            "score": int(total_score), "satisfied_filters": json.dumps(satisfied_filters, ensure_ascii=False),
            "reason": reason_str, "recommendation_price": current_close,
            "recommendation_jdate": today_jdate_str, "timestamp": datetime.now(),
        }
        current_day_results.append(result_payload)

    # --- گام ۲: منطق ذخیره‌سازی ساده شده (بدون نیاز به آپدیت یا بررسی موجودیت) ---
    current_day_results.sort(key=lambda x: x['score'], reverse=True)

    for idx, r in enumerate(current_day_results):
        r['is_golden_key'] = (idx < top_n_symbols and r['score'] > 10)
        if r['score'] >= 30: r['signal_status'] = "📈 سیگنال قوی خرید"
        elif r['score'] >= 15: r['signal_status'] = "⚠️ احتمال رشد"
        else: r['signal_status'] = "❌ سیگنال ضعیف"
        
        final_reason_str = f"وضعیت سیگنال: {r['signal_status']}, دلایل: {r['reason']}"

        new_obj = GoldenKeyResult(
            symbol_id=r['symbol_id'], symbol_name=r['symbol_name'], jdate=r['jdate'],
            score=r['score'], satisfied_filters=r['satisfied_filters'], reason=final_reason_str,
            recommendation_price=r['recommendation_price'], recommendation_jdate=r['jdate'],
            is_golden_key=r['is_golden_key'], status=r['signal_status'],
            # فیلدهای حذف شده دیگر نیازی به مقداردهی اولیه ندارند
            timestamp=r['timestamp']
        )
        db.session.add(new_obj)

    try:
        db.session.commit()
        logger.info(f"Golden Key analysis completed. Total {len(current_day_results)} new results saved.")
        return True, f"Golden Key analysis completed. Total {len(current_day_results)} new results saved."
    except Exception as e:
        db.session.rollback()
        logger.error(f"Error committing Golden Key results: {e}", exc_info=True)
        return False, f"DB commit failed: {str(e)}"

# ---------------------------------------------------------
# API برای دریافت نتایج (بدون تغییر)
# ---------------------------------------------------------
def get_golden_key_results(filters=None, top_n=8):
    # از آنجایی که ما فقط داده‌های امروز را داریم، نیازی به max(jdate) نیست.
    # اما برای سازگاری با API می‌توان آن را نگه داشت.
    latest_date_result = db.session.query(func.max(GoldenKeyResult.jdate)).scalar() 
    if not latest_date_result:
        return {"top_stocks": [], "technical_filters": get_golden_key_filter_definitions(), "last_updated": "نامشخص"}

    query = GoldenKeyResult.query.filter_by(jdate=latest_date_result)
    
    # ... (بقیه منطق واکشی داده‌ها)

    if filters:
        wanted = [f.strip() for f in filters.split(',') if f.strip()]
        all_rows = query.all()
        matched = [r for r in all_rows if all(w in json.loads(r.satisfied_filters or '[]') for w in wanted)]
        rows = matched
    else:
        rows = query.filter_by(is_golden_key=True).order_by(GoldenKeyResult.score.desc()).all()
        if not rows:
            rows = query.order_by(GoldenKeyResult.score.desc()).limit(top_n).all()

    output = [{
        "symbol": r.symbol_name, "symbol_id": r.symbol_id, "jdate": r.jdate,
        "satisfied_filters_list": json.loads(r.satisfied_filters or '[]'),
        "total_score": r.score, "reason": r.reason, "entry_price": r.recommendation_price,
        "status": r.status, "timestamp": r.timestamp.isoformat() if r.timestamp else None
    } for r in rows]
    
    return {"top_stocks": output, "technical_filters": get_golden_key_filter_definitions(), "last_updated": latest_date_result}


def get_golden_key_filter_definitions():
    return [
        {"name": "ورود پول هوشمند در کف قیمتی", "category": "جریان وجوه", "description": "قدرت خریدار حقیقی بیش از ۲ برابر فروشنده و RSI در ناحیه اشباع فروش."},
        {"name": "کندل قدرتمند صعودی با حجم بالا", "category": "الگوهای کندلی", "description": "بدنه کندل بیش از ۶۰٪ کل کندل را تشکیل داده و حجم معاملات ۲ برابر میانگین ۲۰ روزه است."},
        {"name": "تثبیت تقاطع طلایی", "category": "میانگین‌ها", "description": "میانگین متحرک ۲۰ روزه، ۵۰ روزه را به بالا قطع کرده و قیمت بالای هر دو تثبیت شده."},
        {"name": "شکست مقاومت با قدرت خریدار", "category": "روند قیمت", "description": "قیمت بالاترین سقف ۴۰ روزه خود را شکسته و قدرت خریدار حقیقی بیش از ۱.۵ است."},
        {"name": "واگرایی مثبت RSI + افزایش حجم", "category": "واگرایی", "description": "قیمت کف پایین‌تر ساخته ولی RSI کف بالاتر زده و حجم معاملات افزایش یافته."},
        {"name": "فشردگی نوسان (مقدمه حرکت انفجاری)", "category": "نوسان", "description": "نوسانات سهم (ATR) به کمترین حد خود در ۲۰ روز اخیر رسیده و RSI بالای ۵۰ است."},
        {"name": "عبور MACD از خط سیگنال", "category": "واگرایی", "description": "خط MACD خط سیگنال را در ناحیه منفی به سمت بالا قطع کرده است."},
    ]