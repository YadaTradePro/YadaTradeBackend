# -*- coding: utf-8 -*-
# services/weekly_watchlist_service.py
from extensions import db
from models import HistoricalData, ComprehensiveSymbolData, TechnicalIndicatorData, FundamentalData, WeeklyWatchlistResult, SignalsPerformance, AggregatedPerformance, GoldenKeyResult, CandlestickPatternDetection, MLPrediction, FinancialRatiosData
from flask import current_app
import pandas as pd
from datetime import datetime, timedelta, date
import jdatetime
import uuid
from sqlalchemy import func, text
import logging
import json
import numpy as np
from types import SimpleNamespace
from models import DailySectorPerformance


# NEW: Import for market sentiment analysis
from services.iran_market_data import fetch_iran_market_indices

# Import utility functions
from services.utils import get_today_jdate_str, normalize_value, calculate_rsi, calculate_macd, calculate_sma, calculate_bollinger_bands, calculate_volume_ma, calculate_atr, calculate_smart_money_flow, check_candlestick_patterns, check_tsetmc_filters, check_financial_ratios, convert_gregorian_to_jalali, calculate_z_score

# Import analysis_service for aggregated performance calculation
from services import analysis_service

# تنظیمات لاگینگ برای این ماژول
logger = logging.getLogger(__name__)

# IMPROVEMENT: Lookback period and minimum history days adjusted for better indicator quality
TECHNICAL_DATA_LOOKBACK_DAYS = 120
MIN_REQUIRED_HISTORY_DAYS = 50

# REVISED: New filter weights dictionary with descriptions for clarity
FILTER_WEIGHTS = {
    # --- High-Impact Leading & Breakout Signals ---
    "Power_Thrust_Signal": {
        "weight": 5,
        "description": "سیگنال قدرت: ترکیبی از حجم معاملات انفجاری، ورود پول هوشمند سنگین در یک روز مثبت."
    },
    "RSI_Positive_Divergence": {
        "weight": 5,
        "description": "واگرایی مثبت در RSI، یک سیگنال پیشرو قوی برای احتمال بازگشت روند نزولی به صعودی."
    },
    "Resistance_Broken": {
        "weight": 5,
        "description": "شکست یک سطح مقاومت کلیدی، نشانه‌ای از قدرت خریداران و پتانسیل شروع یک حرکت صعودی جدید."
    },
    "Static_Resistance_Broken": {
        "weight": 5,
        "description": "شکست یک مقاومت مهم استاتیک (کلاسیک)، سیگنالی بسیار معتبر برای ادامه رشد."
    },
    "Squeeze_Momentum_Fired_Long": {
        "weight": 4,
        "description": "اندیکاتور Squeeze Momentum سیگنال خرید (خروج از فشردگی) صادر کرده است که نشان‌دهنده احتمال یک حرکت انفجاری است."
    },
    "Stochastic_Bullish_Cross_Oversold": {
        "weight": 4,
        "description": "تقاطع صعودی در اندیکاتور استوکاستیک در ناحیه اشباع فروش، یک سیگنال خرید کلاسیک و معتبر."
    },
    "Consolidation_Breakout_Candidate": {
        "weight": 3,
        "description": "سهم در فاز تراکم و نوسان کم قرار دارد و آماده یک حرکت قوی (شکست) است."
    },
    "Near_Static_Support": {
        "weight": 3,
        "description": "قیمت در نزدیکی یک سطح حمایتی استاتیک معتبر قرار دارد که ریسک به ریوارد مناسبی برای ورود فراهم می‌کند."
    },
    "Bollinger_Lower_Band_Touch": {
        "weight": 2,
        "description": "قیمت به باند پایین بولینگر برخورد کرده است که می‌تواند نشانه بازگشت کوتاه‌مدت قیمت باشد."
    },

    # --- Trend Confirmation & Strength Signals ---
    "IsInLeadingSector": {
        "weight": 4,
        "description": "سهم متعلق به یکی از صنایع پیشرو و مورد توجه بازار است که شانس موفقیت را افزایش می‌دهد."
    },
    "Positive_Real_Money_Flow_Trend_10D": {
        "weight": 3,
        "description": "برآیند ورود پول هوشمند (حقیقی) در ۱۰ روز گذشته مثبت بوده است."
    },
    "Heavy_Individual_Buy_Pressure": {
        "weight": 3,
        "description": "سرانه خرید حقیقی‌ها در روز آخر به طور قابل توجهی بالاتر از سرانه فروش بوده است (ورود پول سنگین)."
    },
    "MACD_Bullish_Cross_Confirmed": {
        "weight": 2,
        "description": "خط MACD خط سیگنال خود را به سمت بالا قطع کرده که تاییدی بر شروع روند صعودی است."
    },
    "HalfTrend_Buy_Signal": {
        "weight": 2,
        "description": "اندیکاتور HalfTrend سیگنال خرید صادر کرده است."
    },
    "High_Volume_On_Up_Day": {
        "weight": 2,
        "description": "حجم معاملات در یک روز مثبت به طور معناداری افزایش یافته که نشان از حمایت قوی از رشد قیمت دارد."
    },
    "Price_Above_SMA50": {
        "weight": 1,
        "description": "قیمت بالاتر از میانگین متحرک ۵۰ روزه خود قرار دارد که نشانه روند صعودی میان‌مدت است."
    },

    # --- Fundamental Quality Filters ---
    "Reasonable_PE": {
        "weight": 1, # Increased from 0.5
        "description": "نسبت P/E سهم در محدوده منطقی قرار دارد و سهم از نظر بنیادی ارزنده است."
    },
    "Reasonable_PS": {
        "weight": 1, # Increased from 0.5
        "description": "نسبت P/S سهم در محدوده منطقی قرار دارد."
    },
    "Positive_EPS": {
        "weight": 0.5,
        "description": "سود به ازای هر سهم (EPS) شرکت مثبت است."
    },

    # --- Candlestick Filters ---
    "Bullish_Engulfing_Detected": {
        "weight": 3,
        "description": "الگوی کندلی پوشاننده صعودی (Bullish Engulfing) مشاهده شده است."
    },
    "Hammer_Detected": {
        "weight": 3,
        "description": "الگوی کندلی چکش (Hammer) که یک الگوی بازگشتی صعودی است، مشاهده شده است."
    },
    "Morning_Star_Detected": {
        "weight": 4,
        "description": "الگوی کندلی ستاره صبحگاهی (Morning Star)، یک الگوی بازگشتی بسیار معتبر، مشاهده شده است."
    },
    
    # --- ML Prediction Filter ---
    "ML_Predicts_Uptrend": {
        "weight": 2,
        "description": "مدل یادگیری ماشین، احتمال بالایی برای یک روند صعودی پیش‌بینی کرده است."
    },

    # --- Penalties & Negative Scores (Crucial for avoiding peaks) ---
    "RSI_Is_Overbought": {
        "weight": -4,
        "description": "جریمه منفی: RSI در ناحیه اشباع خرید قرار دارد که ریسک اصلاح قیمت را افزایش می‌دهد."
    },
    "Price_Too_Stretched_From_SMA50": {
        "weight": -3,
        "description": "جریمه منفی: قیمت فاصله زیادی از میانگین متحرک ۵۰ روزه گرفته که احتمال بازگشت به میانگین را بالا می‌برد."
    },
    "Negative_Real_Money_Flow_Trend_10D": {
        "weight": -2,
        "description": "جریمه منفی: برآیند ورود پول هوشمند در ۱۰ روز گذشته منفی بوده است (خروج پول)."
    }
}


# کمک‌کننده: بازگرداندن یک سری close قابل‌اعتماد از historical DF
def _get_close_series_from_hist_df(hist_df):
    """
    Accepts a historical dataframe and returns a numeric pandas Series of close prices.
    Tries common column names: 'close_price', 'close', 'final'
    """
    if hist_df is None or hist_df.empty:
        return pd.Series(dtype=float)

    for col in ['close_price', 'close', 'final']:
        if col in hist_df.columns:
            ser = pd.to_numeric(hist_df[col], errors='coerce').dropna()
            if not ser.empty:
                return ser
    return pd.Series(dtype=float)



# --- NEW: Market Sentiment Analysis Function ---
def _get_market_sentiment() -> str:
    """
    Determines short-term market sentiment based on the daily change of major indices.
    Returns: 'Bullish', 'Neutral', or 'Bearish'.
    """
    try:
        indices_data = fetch_iran_market_indices()
        total_index = indices_data.get("Total_Index", {})
        equal_weighted_index = indices_data.get("Equal_Weighted_Index", {})

        total_percent = total_index.get("percent", 0) or 0
        equal_percent = equal_weighted_index.get("percent", 0) or 0

        if total_percent > 0.3 and equal_percent > 0.3:
            logger.info(f"Market Sentiment: Bullish (Total: {total_percent}%, Equal: {equal_percent}%)")
            return "Bullish"
        elif total_percent < -0.3 and equal_percent < -0.3:
            logger.info(f"Market Sentiment: Bearish (Total: {total_percent}%, Equal: {equal_percent}%)")
            return "Bearish"
        else:
            logger.info(f"Market Sentiment: Neutral (Total: {total_percent}%, Equal: {equal_percent}%)")
            return "Neutral"
    except Exception as e:
        logger.error(f"Could not fetch market sentiment, defaulting to Neutral: {e}")
        return "Neutral"

# --- REVISED: Filter Functions ---

def _check_market_condition_filters(hist_df, tech_df):
    """
    Checks for individual stock conditions like overbought state or consolidation.
    """
    satisfied_filters, reason_parts = [], {"market_condition": []}
    if tech_df is None or tech_df.empty or hist_df is None or len(hist_df) < 50:
        return satisfied_filters, reason_parts

    last_tech = tech_df.iloc[-1]
    close_ser = _get_close_series_from_hist_df(hist_df)
    if close_ser.empty:
        return satisfied_filters, reason_parts
    last_close = close_ser.iloc[-1]
    last_hist = hist_df.iloc[-1]

    # --- Check 1: RSI Overbought Condition (Penalize only if weak) ---
    if hasattr(last_tech, 'RSI') and last_tech.RSI is not None and last_tech.RSI > 70:
        is_negative_divergence = (
            hasattr(last_tech, 'RSI_Divergence') and last_tech.RSI_Divergence == "Negative"
        )
        historical_volume_series = hist_df.tail(10)['volume']
        average_volume = historical_volume_series.mean() if not historical_volume_series.empty else 0
        is_high_volume = last_hist['volume'] > average_volume * 1.5 if average_volume > 0 else False

        if is_negative_divergence or not is_high_volume:
            satisfied_filters.append("RSI_Is_Overbought")
            reason_parts["market_condition"].append(
                f"RSI ({last_tech.RSI:.2f}) overbought with weakness."
            )
        else:
            reason_parts["market_condition"].append(
                f"RSI ({last_tech.RSI:.2f}) overbought but supported by strong volume (no penalty)."
            )

    # --- Check 2: Price is too far from its SMA50 (Penalize) ---
    if hasattr(last_tech, 'SMA_50') and last_tech.SMA_50 is not None and last_tech.SMA_50 > 0:
        stretch_percent = ((last_close - last_tech.SMA_50) / last_tech.SMA_50) * 100
        if stretch_percent > 20:
            satisfied_filters.append("Price_Too_Stretched_From_SMA50")
            reason_parts["market_condition"].append(
                f"Price is overextended ({stretch_percent:.1f}%) from SMA50."
            )

    # --- Check 3: Consolidation Pattern (Reward) ---
    if hasattr(last_tech, 'ATR'):
        atr_series = pd.to_numeric(tech_df['ATR'].dropna())
        if len(atr_series) > 30:
            recent_atr_avg = atr_series.tail(10).mean()
            historical_atr_avg = atr_series.tail(30).mean()
            if recent_atr_avg < (historical_atr_avg * 0.7):
                satisfied_filters.append("Consolidation_Breakout_Candidate")
                reason_parts["market_condition"].append(
                    "Stock is in a low-volatility consolidation phase."
                )

    return satisfied_filters, reason_parts

# تغییر در is_data_sufficient: انعطاف‌پذیرتر و مقاوم‌تر
def is_data_sufficient(data_df, min_len):
    """
    Checks if the provided DataFrame is not empty and has at least min_len records.
    """
    if data_df is None or data_df.empty:
        return False
    return len(data_df) >= min_len

def convert_jalali_to_gregorian_timestamp(jdate_str):
    """
    Converts a Jalali date string (YYYY-MM-DD) to a pandas Timestamp (Gregorian).
    """
    if pd.notna(jdate_str) and isinstance(jdate_str, str):
        try:
            jy, jm, jd = map(int, jdate_str.split('-'))
            gregorian_date = jdatetime.date(jy, jm, jd).togregorian()
            return pd.Timestamp(gregorian_date)
        except ValueError:
            return pd.NaT
    return pd.NaT

# Helper function to safely get a value
def _get_attr_safe(rec, attr, default=None):
    val = getattr(rec, attr, default)
    if isinstance(val, (pd.Series, pd.DataFrame)):
        return val.iloc[0] if not val.empty else default
    return val

# --- REFACTORED: Technical filters are broken down into smaller functions ---

def _check_oscillator_signals(technical_rec, prev_tech_rec, close_ser):
    """Checks oscillator-based signals like RSI and Stochastic."""
    satisfied_filters, reason_parts = [], {"technical": []}
    
    # RSI Positive Divergence
    current_rsi = _get_attr_safe(technical_rec, 'RSI')
    prev_rsi = _get_attr_safe(prev_tech_rec, 'RSI')
    if current_rsi is not None and prev_rsi is not None and len(close_ser) > 1:
        if current_rsi > prev_rsi and close_ser.iloc[-1] < close_ser.iloc[-2]:
            satisfied_filters.append("RSI_Positive_Divergence")
            reason_parts["technical"].append(f"Positive divergence on RSI ({current_rsi:.2f}).")

    # Stochastic Oscillator
    current_stoch_k = _get_attr_safe(technical_rec, 'Stochastic_K')
    current_stoch_d = _get_attr_safe(technical_rec, 'Stochastic_D')
    prev_stoch_k = _get_attr_safe(prev_tech_rec, 'Stochastic_K')
    prev_stoch_d = _get_attr_safe(prev_tech_rec, 'Stochastic_D')
    if all(x is not None for x in [current_stoch_k, current_stoch_d, prev_stoch_k, prev_stoch_d]):
        if current_stoch_k > current_stoch_d and prev_stoch_k <= prev_stoch_d and current_stoch_d < 25:
            satisfied_filters.append("Stochastic_Bullish_Cross_Oversold")
            reason_parts["technical"].append("Stochastic bullish cross in oversold area.")
            
    return satisfied_filters, reason_parts

def _check_trend_signals(technical_rec, prev_tech_rec, last_close_val):
    """Checks trend-following signals like MACD, SMA, HalfTrend, and Resistance breaks."""
    satisfied_filters, reason_parts = [], {"technical": []}

    # MACD Cross
    current_macd = _get_attr_safe(technical_rec, 'MACD')
    current_macd_signal = _get_attr_safe(technical_rec, 'MACD_Signal')
    prev_macd = _get_attr_safe(prev_tech_rec, 'MACD')
    prev_macd_signal = _get_attr_safe(prev_tech_rec, 'MACD_Signal')
    if all(x is not None for x in [current_macd, current_macd_signal, prev_macd, prev_macd_signal]):
        if current_macd > current_macd_signal and prev_macd <= prev_macd_signal:
            satisfied_filters.append("MACD_Bullish_Cross_Confirmed")

    # Price vs SMA50
    sma50 = _get_attr_safe(technical_rec, 'SMA_50')
    if sma50 is not None and last_close_val > sma50:
        satisfied_filters.append("Price_Above_SMA50")
        
    # HalfTrend
    current_halftrend = _get_attr_safe(technical_rec, 'halftrend_signal')
    prev_halftrend = _get_attr_safe(prev_tech_rec, 'halftrend_signal')
    if current_halftrend == 1 and prev_halftrend != 1:
        satisfied_filters.append("HalfTrend_Buy_Signal")
        
    # Dynamic Resistance Break
    resistance_broken = _get_attr_safe(technical_rec, 'resistance_broken')
    if resistance_broken:
        satisfied_filters.append("Resistance_Broken")
        res_level = _get_attr_safe(technical_rec, 'resistance_level_50d', 'N/A')
        reason_parts["technical"].append(f"Broke a key dynamic resistance level around {res_level}.")

    return satisfied_filters, reason_parts

def _check_volatility_signals(hist_df, technical_rec, last_close_val):
    """Checks volatility-based signals like Bollinger Bands and Squeeze Momentum."""
    satisfied_filters, reason_parts = [], {"technical": []}

    # Bollinger Lower Band
    bollinger_low = _get_attr_safe(technical_rec, 'Bollinger_Low')
    if bollinger_low is not None and last_close_val < bollinger_low:
        satisfied_filters.append("Bollinger_Lower_Band_Touch")

    # Squeeze Momentum
    prev_tech_rec = hist_df.iloc[-2] if len(hist_df) > 1 else technical_rec
    current_squeeze_on = _get_attr_safe(technical_rec, 'squeeze_on')
    prev_squeeze_on = _get_attr_safe(prev_tech_rec, 'squeeze_on')
    if current_squeeze_on == False and prev_squeeze_on == True:
        satisfied_filters.append("Squeeze_Momentum_Fired_Long")
        reason_parts["technical"].append("Squeeze Momentum indicator fired long.")
        
    return satisfied_filters, reason_parts

def _check_volume_signals(hist_df, close_ser):
    """Checks for volume-based signals."""
    satisfied_filters, reason_parts = [], {"technical": []}
    
    if 'volume' in hist_df.columns and len(hist_df) >= 20 and len(close_ser) > 1:
        volume_z_score = calculate_z_score(pd.to_numeric(hist_df['volume'], errors='coerce').dropna().iloc[-20:])
        if volume_z_score is not None and volume_z_score > 1.5 and close_ser.iloc[-1] > close_ser.iloc[-2]:
            satisfied_filters.append("High_Volume_On_Up_Day")
            reason_parts["technical"].append(f"High volume (Z-Score: {volume_z_score:.2f}) on a positive day.")
            
    return satisfied_filters, reason_parts

def _check_technical_filters(hist_df, tech_df):
    """
    Checks all technical indicators by calling specialized sub-functions. (Coordinator function)
    """
    all_satisfied_filters, all_reason_parts = [], {"technical": []}
    if tech_df is None or tech_df.empty or len(tech_df) < 2:
        return all_satisfied_filters, all_reason_parts

    technical_rec = tech_df.iloc[-1]
    prev_tech_rec = tech_df.iloc[-2]
    
    close_ser = _get_close_series_from_hist_df(hist_df)
    last_close_val = close_ser.iloc[-1] if not close_ser.empty else None
    if last_close_val is None:
        return all_satisfied_filters, all_reason_parts

    # Call sub-functions and aggregate results
    for func in [_check_oscillator_signals, _check_trend_signals, _check_volatility_signals, _check_volume_signals]:
        # Adjust arguments as needed per function signature
        if func in [_check_oscillator_signals, _check_trend_signals]:
             satisfied, reasons = func(technical_rec, prev_tech_rec, close_ser if func == _check_oscillator_signals else last_close_val)
        elif func == _check_volatility_signals:
            satisfied, reasons = func(tech_df, technical_rec, last_close_val)
        else: # _check_volume_signals
            satisfied, reasons = func(hist_df, close_ser)

        all_satisfied_filters.extend(satisfied)
        if "technical" in reasons:
            all_reason_parts["technical"].extend(reasons["technical"])

    return all_satisfied_filters, all_reason_parts

# --- END REFACTORED SECTION ---

def _check_fundamental_filters(fundamental_rec):
    satisfied_filters = []
    reason_parts = {"fundamental": []}
    if fundamental_rec:
        if fundamental_rec.pe is not None and 0 < fundamental_rec.pe < 20: satisfied_filters.append("Reasonable_PE")
        if fundamental_rec.p_s_ratio is not None and 0 < fundamental_rec.p_s_ratio < 5: satisfied_filters.append("Reasonable_PS")
        if fundamental_rec.eps is not None and fundamental_rec.eps > 0: satisfied_filters.append("Positive_EPS")
    return satisfied_filters, reason_parts

def _check_smart_money_filters(hist_df):
    """REVISED: Now also checks for heavy individual buy pressure on the last day."""
    satisfied_filters = []
    reason_parts = {"smart_money": []}
    trend_lookback = 10
    if hist_df is None or hist_df.empty or 'buy_i_volume' not in hist_df.columns:
        return satisfied_filters, reason_parts

    # Check 1: 10-Day Real Money Flow Trend
    if len(hist_df) >= trend_lookback:
        smart_money_flow_df = calculate_smart_money_flow(hist_df)
        if not smart_money_flow_df.empty and len(smart_money_flow_df) >= trend_lookback:
            trend_net_flow = smart_money_flow_df['individual_net_flow'].iloc[-trend_lookback:].sum()
            if trend_net_flow > 0:
                satisfied_filters.append("Positive_Real_Money_Flow_Trend_10D")
                reason_parts["smart_money"].append(f"Positive real money inflow over the last {trend_lookback} days.")
            elif trend_net_flow < 0:
                satisfied_filters.append("Negative_Real_Money_Flow_Trend_10D")
    
    # NEW Check 2: Heavy Individual Buy Pressure on the last day
    last_day = hist_df.iloc[-1]
    required_cols = ['buy_i_volume', 'buy_i_count', 'sell_i_volume', 'sell_i_count']
    if all(col in last_day and pd.notna(last_day[col]) for col in required_cols):
        if last_day['buy_i_count'] > 0 and last_day['sell_i_count'] > 0:
            per_capita_buy = last_day['buy_i_volume'] / last_day['buy_i_count']
            per_capita_sell = last_day['sell_i_volume'] / last_day['sell_i_count']
            
            # Check if per capita buy is 2.5x greater than sell
            if per_capita_buy > (per_capita_sell * 2.5):
                satisfied_filters.append("Heavy_Individual_Buy_Pressure")
                reason_parts["smart_money"].append(f"Significant buy pressure detected (Per capita buy: {per_capita_buy:,.0f} vs sell: {per_capita_sell:,.0f}).")

    return satisfied_filters, reason_parts


def _check_power_thrust_signal(hist_df, close_ser):
    """
    Checks for a Power Thrust Signal: a combination of high volume,
    heavy individual buy pressure, and a positive closing day.
    """
    satisfied_filters, reason_parts = [], {"power_thrust": []}
    
    # اطمینان از وجود داده کافی برای تحلیل
    if hist_df is None or len(hist_df) < 20 or close_ser.empty or len(close_ser) < 2:
        return satisfied_filters, reason_parts

    last_day = hist_df.iloc[-1]
    
    # --- شرط ۱: روز معاملاتی مثبت ---
    is_up_day = close_ser.iloc[-1] > close_ser.iloc[-2]
    if not is_up_day:
        return satisfied_filters, reason_parts # اگر روز مثبت نیست، ادامه نده

    # --- شرط ۲: حجم معاملات بسیار بالا (استفاده از Z-Score) ---
    volume_series = pd.to_numeric(hist_df['volume'], errors='coerce').dropna().tail(20)
    volume_z_score = calculate_z_score(volume_series)
    # آستانه بالا برای حجم، مثلا Z-Score بیشتر از 1.8
    is_high_volume = volume_z_score is not None and volume_z_score > 1.8 
    
    # --- شرط ۳: فشار خرید سنگین حقیقی‌ها ---
    is_heavy_buy_pressure = False
    required_cols = ['buy_i_volume', 'buy_i_count', 'sell_i_volume', 'sell_i_count']
    if all(col in last_day and pd.notna(last_day[col]) for col in required_cols):
        if last_day['buy_i_count'] > 0 and last_day['sell_i_count'] > 0:
            per_capita_buy = last_day['buy_i_volume'] / last_day['buy_i_count']
            per_capita_sell = last_day['sell_i_volume'] / last_day['sell_i_count']
            # آستانه بالا برای سرانه خرید، مثلا ۲.۵ برابر سرانه فروش
            if per_capita_buy > (per_capita_sell * 2.5):
                is_heavy_buy_pressure = True

    # --- ترکیب نهایی ---
    if is_high_volume and is_heavy_buy_pressure:
        satisfied_filters.append("Power_Thrust_Signal")
        reason_parts["power_thrust"].append(
            f"Power signal detected! High Volume (Z-Score: {volume_z_score:.2f}) and Heavy Buy Pressure."
        )
            
    return satisfied_filters, reason_parts


def _check_candlestick_filters(pattern_rec):
    satisfied_filters, reason_parts = [], {"candlestick": []}
    if not pattern_rec:
        return satisfied_filters, reason_parts

    pattern_name = pattern_rec.pattern_name
    if "Bullish Engulfing" in pattern_name:
        satisfied_filters.append("Bullish_Engulfing_Detected")
        reason_parts["candlestick"].append(f"Detected: {pattern_name}")
    if "Hammer" in pattern_name:
        satisfied_filters.append("Hammer_Detected")
        reason_parts["candlestick"].append(f"Detected: {pattern_name}")
    if "Morning Star" in pattern_name:
        satisfied_filters.append("Morning_Star_Detected")
        reason_parts["candlestick"].append(f"Detected: {pattern_name}")

    return satisfied_filters, reason_parts

def _check_advanced_fundamental_filters(ratios_df):
    satisfied_filters, reason_parts = [], {"advanced_fundamental": []}
    if ratios_df is None or ratios_df.empty:
        return satisfied_filters, reason_parts

    debt_ratios = ratios_df[ratios_df['ratio_name'] == 'Debt to Equity'].sort_values('fiscal_year')
    if len(debt_ratios) >= 2:
        last_ratio = debt_ratios['ratio_value'].iloc[-1]
        prev_ratio = debt_ratios['ratio_value'].iloc[-2]
        if last_ratio < prev_ratio:
            satisfied_filters.append("Debt_To_Equity_Decreasing")
            reason_parts["advanced_fundamental"].append(f"Debt to Equity is decreasing (from {prev_ratio:.2f} to {last_ratio:.2f}).")
            
    return satisfied_filters, reason_parts

# --- NEW: Functions for new strategic filters ---

def _get_leading_sectors():
    """
    با کوئری به جدول DailySectorPerformance، لیست صنایع پیشرو (مثلاً 4 صنعت برتر)
    در آخرین روز تحلیل شده را برمی‌گرداند.
    """
    try:
        latest_date_query = db.session.query(func.max(DailySectorPerformance.jdate)).scalar()
        if not latest_date_query:
            logger.warning("هیچ داده‌ای در جدول تحلیل صنایع یافت نشد. از لیست پیش‌فرض استفاده می‌شود.")
            return {"خودرو و ساخت قطعات"} # Fallback

        # دریافت 4 صنعت برتر در آخرین روز
        leading_sectors_query = db.session.query(DailySectorPerformance.sector_name)\
                                          .filter(DailySectorPerformance.jdate == latest_date_query)\
                                          .order_by(DailySectorPerformance.rank.asc())\
                                          .limit(4).all()
        
        leading_sectors = {row[0] for row in leading_sectors_query}
        logger.info(f"صنایع پیشرو شناسایی شده از دیتابیس: {leading_sectors}")
        return leading_sectors

    except Exception as e:
        logger.error(f"خطا در دریافت صنایع پیشرو از دیتابیس: {e}")
        return {"خودرو و ساخت قطعات"} # Fallback in case of error

def _check_sector_strength_filter(symbol_sector, leading_sectors):
    """Checks if the symbol belongs to a leading sector."""
    satisfied_filters, reason_parts = [], {"sector_strength": []}
    if symbol_sector in leading_sectors:
        satisfied_filters.append("IsInLeadingSector")
        reason_parts["sector_strength"].append(f"Symbol is in a leading sector: {symbol_sector}.")
    return satisfied_filters, reason_parts

def _check_static_levels_filters(technical_rec, last_close_val):
    """
    Checks for proximity to static support or breakout of static resistance.
    This assumes 'static_support_level' and 'static_resistance_level' fields
    are pre-calculated and available in the TechnicalIndicatorData record.
    """
    satisfied_filters, reason_parts = [], {"static_levels": []}

    # --- اصلاح: رفع خطای Pandas ValueError (خط 526 در لاگ شما) ---
    # technical_rec می‌تواند None یا یک Pandas Series باشد.
    is_rec_valid = technical_rec is not None and (
        not isinstance(technical_rec, pd.Series) or not technical_rec.empty
    )
    # last_close_val باید یک مقدار عددی باشد، پس بررسی None کافی است.
    if not is_rec_valid or last_close_val is None or last_close_val <= 0:
        return satisfied_filters, reason_parts

    # 1. Check for proximity to static support
    # (استفاده از _get_attr_safe تضمین می‌کند که مقدار عددی از Series یا شیء ORM استخراج شود)
    support_level = _get_attr_safe(technical_rec, 'static_support_level')
    
    # اگر سطح حمایت معتبر باشد و قیمت به آن نزدیک باشد
    if support_level and support_level > 0:
        # فاصله‌ی قیمت از سطح حمایت (مثبت برای بالای حمایت، منفی برای پایین)
        distance = last_close_val - support_level
        # میزان نوسان (درصد) نسبت به قیمت حمایت
        proximity_percent = abs(distance) / support_level 
        
        # اگر در محدوده ۲٪ حول سطح حمایت باشد (ریسک به ریوارد مناسب)
        if proximity_percent <= 0.02 and distance >= -0.005 * support_level: # نزدیک یا کمی بالاتر از سطح (حداکثر 0.5% زیر سطح)
            satisfied_filters.append("Near_Static_Support")
            reason_parts["static_levels"].append(
                f"Price is near a major static support level at {support_level:,.0f} (Proximity: {proximity_percent*100:.1f}%)."
            )

    # 2. Check for breakout of static resistance
    resistance_level = _get_attr_safe(technical_rec, 'static_resistance_level')
    
    # اگر سطح مقاومت معتبر باشد
    if resistance_level and resistance_level > 0:
        # قیمت بسته شدن بالاتر از سطح مقاومت باشد.
        # نکته: برای یک چک قوی باید قیمت روز قبل را هم چک کنیم که زیر مقاومت بوده باشد.
        # اما با فرض سادگی و موجود نبودن قیمت روز قبل:
        if last_close_val > resistance_level and last_close_val < 1.03 * resistance_level: 
            # شرط دوم: قیمت بیش از حد از مقاومت دور نشده باشد (شکست تازه و معتبر)
            satisfied_filters.append("Static_Resistance_Broken")
            reason_parts["static_levels"].append(
                f"Price broke a major static resistance level at {resistance_level:,.0f}."
            )

    return satisfied_filters, reason_parts

# --- END NEW STRATEGIC FILTERS ---

def run_weekly_watchlist_selection():
    """
    Selects symbols for the weekly watchlist using a bulk data fetching and processing approach,
    incorporating a dynamic score threshold and advanced strategic filters.
    """
    logger.info("Starting Weekly Watchlist selection process.")

    # Step 1: Determine market sentiment and leading sectors
    market_sentiment = _get_market_sentiment()
    leading_sectors = _get_leading_sectors() # NEW
    
    if market_sentiment == "Bullish":
        score_threshold = 7
    elif market_sentiment == "Neutral":
        score_threshold = 8
    else:  # Bearish
        score_threshold = 10
    logger.info(f"Market sentiment is '{market_sentiment}'. Score threshold set to >= {score_threshold}.")
    logger.info(f"Identified leading sectors: {leading_sectors}")

    # Step 2: Bulk Data Fetching
    allowed_market_types = ['بورس', 'فرابورس', 'پایه فرابورس', 'بورس کالا', 'بورس انرژی']
    symbols_to_analyze = ComprehensiveSymbolData.query.filter(
        ComprehensiveSymbolData.market_type.in_(allowed_market_types)
    ).all()

    if not symbols_to_analyze:
        logger.warning("No symbols found for watchlist analysis. Skipping.")
        return False, "No symbols found for watchlist analysis."
    
    symbol_ids = [s.symbol_id for s in symbols_to_analyze]
    today_jdate = get_today_jdate_str()
    cutoff_date_j = (jdatetime.date.today() - jdatetime.timedelta(days=TECHNICAL_DATA_LOOKBACK_DAYS + 10)).strftime('%Y-%m-%d')
    
    logger.info(f"Fetching bulk data for {len(symbol_ids)} symbols...")
    
    try:
        # Fetch all required data in bulk
        hist_df = pd.DataFrame([rec.__dict__ for rec in HistoricalData.query.filter(HistoricalData.symbol_id.in_(symbol_ids), HistoricalData.jdate >= cutoff_date_j).all()]).drop(columns=['_sa_instance_state'], errors='ignore')
        tech_df = pd.DataFrame([rec.__dict__ for rec in TechnicalIndicatorData.query.filter(TechnicalIndicatorData.symbol_id.in_(symbol_ids), TechnicalIndicatorData.jdate >= cutoff_date_j).all()]).drop(columns=['_sa_instance_state'], errors='ignore')
        fundamental_records = FundamentalData.query.filter(FundamentalData.symbol_id.in_(symbol_ids)).all()
        candlestick_records = CandlestickPatternDetection.query.filter(CandlestickPatternDetection.symbol_id.in_(symbol_ids), CandlestickPatternDetection.jdate == today_jdate).all()
        ml_predictions = MLPrediction.query.filter(MLPrediction.symbol_id.in_(symbol_ids), MLPrediction.jprediction_date == today_jdate, MLPrediction.predicted_trend == 'Uptrend').all()
        financial_ratio_records = FinancialRatiosData.query.filter(FinancialRatiosData.symbol_id.in_(symbol_ids)).all()
        financial_ratios_df = pd.DataFrame([rec.__dict__ for rec in financial_ratio_records]).drop(columns=['_sa_instance_state'], errors='ignore') if financial_ratio_records else pd.DataFrame()

    except Exception as e:
         logger.error(f"❌ Error during bulk data fetching: {e}", exc_info=True)
         return False, "Data fetching failed."
    
    logger.info(f"Fetched data for {len(hist_df)} historical records and other related tables.")
    
    # Step 3: Group data for efficient processing
    hist_groups = {k: v.sort_values(by='jdate') for k, v in hist_df.groupby("symbol_id")} if not hist_df.empty else {}
    tech_groups = {k: v.sort_values(by='jdate') for k, v in tech_df.groupby("symbol_id")} if not tech_df.empty else {}
    fundamental_map = {rec.symbol_id: rec for rec in fundamental_records}
    candlestick_map = {rec.symbol_id: rec for rec in candlestick_records}
    ml_prediction_set = {rec.symbol_id for rec in ml_predictions}
    financial_ratios_groups = {k: v for k, v in financial_ratios_df.groupby("symbol_id")} if not financial_ratios_df.empty else {}
    
    # Step 4: Process each symbol and score it
    watchlist_candidates = []
    for symbol in symbols_to_analyze:
        symbol_hist_df = hist_groups.get(symbol.symbol_id, pd.DataFrame()).copy()
        symbol_tech_df = tech_groups.get(symbol.symbol_id, pd.DataFrame()).copy()

        if len(symbol_hist_df) < MIN_REQUIRED_HISTORY_DAYS:
            continue
        
        last_close_series = _get_close_series_from_hist_df(symbol_hist_df)
        entry_price = float(last_close_series.iloc[-1]) if not last_close_series.empty else None
        
        if entry_price is None or pd.isna(entry_price):
            logger.warning(f"Skipping {symbol.symbol_name} due to missing entry price.")
            continue
            
        all_satisfied_filters = []
        all_reason_parts = {}

        # Run all filter checks
        def run_check(check_func, *args):
            filters, reasons = check_func(*args)
            all_satisfied_filters.extend(filters)
            all_reason_parts.update(reasons)

        technical_rec = symbol_tech_df.iloc[-1] if not symbol_tech_df.empty else None
        fundamental_rec = fundamental_map.get(symbol.symbol_id)
        pattern_rec = candlestick_map.get(symbol.symbol_id)
        symbol_ratios_df = financial_ratios_groups.get(symbol.symbol_id)
        
        run_check(_check_technical_filters, symbol_hist_df, symbol_tech_df)
        run_check(_check_fundamental_filters, fundamental_rec)
        run_check(_check_smart_money_filters, symbol_hist_df)
        run_check(_check_power_thrust_signal, symbol_hist_df, last_close_series)
        run_check(_check_market_condition_filters, symbol_hist_df, symbol_tech_df)
        run_check(_check_candlestick_filters, pattern_rec)
        run_check(_check_advanced_fundamental_filters, symbol_ratios_df)
        
        # --- NEW: Run new strategic filter checks ---
        run_check(_check_sector_strength_filter, getattr(symbol, 'sector_name', ''), leading_sectors)
        run_check(_check_static_levels_filters, technical_rec, entry_price)

        if symbol.symbol_id in ml_prediction_set:
            all_satisfied_filters.append("ML_Predicts_Uptrend")
            all_reason_parts.setdefault("ml_signal", []).append("ML model predicts a high-probability uptrend.")
            
        # REVISED: Calculate score using the new weights structure
        score = sum(FILTER_WEIGHTS.get(f, {}).get('weight', 0) for f in all_satisfied_filters)

        if score >= score_threshold:
            watchlist_candidates.append({
                "symbol_id": symbol.symbol_id,
                "symbol_name": symbol.symbol_name,
                "entry_price": entry_price,
                "entry_date": date.today(),
                "jentry_date": get_today_jdate_str(),
                "outlook": "Bullish",
                "reason_json": json.dumps(all_reason_parts, ensure_ascii=False),
                "satisfied_filters": json.dumps(list(set(all_satisfied_filters)), ensure_ascii=False),
                "score": score
            })

    # Step 5: Sort candidates and save the top N to the database
    logger.info(f"Found {len(watchlist_candidates)} candidates meeting the threshold. Sorting and saving top 8.")
    watchlist_candidates.sort(key=lambda x: x['score'], reverse=True)
    final_watchlist = watchlist_candidates[:8]

    saved_count = 0
    for candidate in final_watchlist:
        # Upsert logic
        existing_result = WeeklyWatchlistResult.query.filter_by(
            symbol_id=candidate['symbol_id'], jentry_date=candidate['jentry_date']
        ).first()

        if existing_result:
             # Update existing record
            existing_result.entry_price = candidate['entry_price']
            existing_result.outlook = candidate['outlook']
            existing_result.reason = candidate['satisfied_filters']
            existing_result.probability_percent = min(100, candidate['score'] * 5)
            existing_result.created_at = datetime.now()
        else:
            # Create new record
            existing_result = WeeklyWatchlistResult(
                signal_unique_id=str(uuid.uuid4()),
                symbol_id=candidate['symbol_id'],
                symbol_name=candidate['symbol_name'],
                entry_price=candidate['entry_price'],
                entry_date=candidate['entry_date'],
                jentry_date=candidate['jentry_date'],
                outlook=candidate['outlook'],
                reason=candidate['satisfied_filters'],
                probability_percent=min(100, candidate['score'] * 5),
                status='active',
            )
        db.session.add(existing_result)
        saved_count += 1

    try:
        db.session.commit()
        message = f"Weekly Watchlist selection completed. Saved top {saved_count} symbols."
        logger.info(message)
        return True, message
    except Exception as e:
        db.session.rollback()
        logger.error(f"Error during DB commit: {e}", exc_info=True)
        return False, "Database commit failed."


# =================================================================================
# Performance Evaluation Logic (No changes needed here, but kept for completeness)
# =================================================================================

def _update_weekly_watchlist_performance(active_entries):
    logger.info(f"Updating performance for {len(active_entries)} active watchlist signals.")
    updated_count = 0
    today_jdate_str = get_today_jdate_str()
    current_greg_date = datetime.now().date()

    for entry in active_entries:
        try:
            latest_historical = HistoricalData.query.filter_by(symbol_id=entry.symbol_id).order_by(HistoricalData.jdate.desc()).first()
            if not latest_historical:
                logger.warning(f"Skipping update for {entry.symbol_name} due to missing historical data.")
                continue

            current_price = normalize_value(latest_historical.close)
            if current_price is None or current_price <= 0:
                continue
            
            # برای سادگی، منطق حد سود و ضرر با ATR حذف شد تا بر اساس زمان ارزیابی شود
            entry_jdate = jdatetime.date(*map(int, entry.jentry_date.split('-')))
            days_passed = (jdatetime.date.today() - entry_jdate).days

            new_status = 'active'
            evaluation_reason = ""

            # سیگنال پس از 6 روز کاری منقضی و بسته می‌شود
            if days_passed >= 6:
                new_status = 'closed_expired'
                evaluation_reason = f"Expired after {days_passed} days."

            if new_status != 'active':
                profit_loss_percent = ((current_price - entry.entry_price) / entry.entry_price) * 100
                
                # تعیین وضعیت نهایی بر اساس سود یا زیان
                if new_status == 'closed_expired':
                    if profit_loss_percent > 0:
                        new_status = 'closed_win'
                    elif profit_loss_percent < 0:
                        new_status = 'closed_loss'
                    else:
                        new_status = 'closed_neutral'

                entry.status = new_status
                entry.exit_price = current_price
                entry.exit_date = current_greg_date
                entry.jexit_date = today_jdate_str
                entry.profit_loss_percentage = profit_loss_percent
                entry.updated_at = datetime.now()
            
                performance_record = SignalsPerformance(
                    symbol_id=entry.symbol_id,
                    symbol_name=entry.symbol_name,
                    # مقدار ثابت و صحیح برای منبع سیگنال
                    signal_source='WeeklyWatchlistService', 
                    entry_date=entry.entry_date,
                    entry_price=entry.entry_price,
                    jentry_date=entry.jentry_date,
                    exit_date=current_greg_date,
                    jexit_date=today_jdate_str,
                    exit_price=current_price,
                    profit_loss_percent=profit_loss_percent,
                    status=new_status,
                    reason=json.dumps({"original_reason": entry.reason, "evaluation_reason": evaluation_reason}),
                    outlook=entry.outlook
                )
                db.session.add(performance_record)
                db.session.add(entry)
                updated_count += 1
                logger.info(f"Closed signal for {entry.symbol_name}. Status: {new_status}, P/L: {profit_loss_percent:.2f}%.")
        
        except Exception as e:
            logger.error(f"Error updating performance for {entry.symbol_name}: {e}", exc_info=True)

    try:
        if updated_count > 0:
            db.session.commit()
            logger.info(f"Successfully updated and closed {updated_count} watchlist signals.")
    except Exception as e:
        db.session.rollback()
        logger.error(f"DB commit error during watchlist performance update: {e}", exc_info=True)


# --- این دو تابع به طور کامل حذف شدند ---
# def _calculate_watchlist_performance_metrics(all_results, start_date):
# def _save_watchlist_performance_metrics(...):


def evaluate_weekly_watchlist_performance():
    """
    فقط سیگنال‌های فعال را بررسی و در صورت لزوم می‌بندد و نتیجه را در SignalsPerformance ثبت می‌کند.
    دیگر محاسبات تجمعی را انجام نمی‌دهد.
    """
    logger.info("Starting Weekly Watchlist status update process.")
    
    active_entries = WeeklyWatchlistResult.query.filter_by(status='active').all()
    if not active_entries:
        logger.warning("No active watchlist signals found to update.")
        return True, "No active signals to evaluate."
    
    _update_weekly_watchlist_performance(active_entries)
    
    logger.info("Weekly Watchlist status update process completed.")
    # توجه: این تابع دیگر نیازی به اجرای روزانه برای محاسبه win-rate ندارد.
    # این کار توسط performance_service انجام خواهد شد.
    return True, "Weekly Watchlist signals' status updated successfully."

def get_weekly_watchlist_results():
    logger.info("Retrieving latest weekly watchlist results.")
    
    latest_jdate_record_obj = WeeklyWatchlistResult.query.order_by(WeeklyWatchlistResult.jentry_date.desc()).first()
    
    if not latest_jdate_record_obj or not latest_jdate_record_obj.jentry_date:
        logger.warning("No weekly watchlist results found.")
        return {"top_watchlist_stocks": [], "last_updated": "نامشخص"}

    latest_jdate_str = latest_jdate_record_obj.jentry_date
    logger.info(f"Latest Weekly Watchlist results date: {latest_jdate_str}")

    results = WeeklyWatchlistResult.query.filter_by(jentry_date=latest_jdate_str)\
                                         .order_by(WeeklyWatchlistResult.created_at.desc()).all() 

    symbol_ids_in_watchlist = [r.symbol_id for r in results]
    company_name_map = {}
    if symbol_ids_in_watchlist:
        company_name_records = ComprehensiveSymbolData.query.filter(
            ComprehensiveSymbolData.symbol_id.in_(symbol_ids_in_watchlist)
        ).with_entities(ComprehensiveSymbolData.symbol_id, ComprehensiveSymbolData.company_name).all()
        company_name_map = {symbol_id: company_name for symbol_id, company_name in company_name_records}

    output_stocks = []
    for r in results:
        output_stocks.append({
            'signal_unique_id': r.signal_unique_id, 
            'symbol_id': r.symbol_id,
            'symbol_name': r.symbol_name,
            'company_name': company_name_map.get(r.symbol_id, r.symbol_name),
            'outlook': r.outlook,
            'reason': r.reason,
            'entry_price': r.entry_price,
            'jentry_date': r.jentry_date,
            'exit_price': r.exit_price,
            'jexit_date': r.jexit_date,
            'profit_loss_percentage': r.profit_loss_percentage,
            'status': r.status,
            'probability_percent': r.probability_percent
        })
    
    logger.info(f"Retrieved and enriched {len(output_stocks)} weekly watchlist results.")
    
    return {
        "top_watchlist_stocks": output_stocks,
        "last_updated": latest_jdate_str
    }