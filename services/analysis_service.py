# services/analysis_service.py
from extensions import db
from models import (
    HistoricalData, ComprehensiveSymbolData, SignalsPerformance,
    FundamentalData, TechnicalIndicatorData, GoldenKeyResult,
    WeeklyWatchlistResult, AggregatedPerformance # AggregatedPerformance is still needed for GoldenKeyResult updates
)
from datetime import datetime, timedelta, date
import jdatetime
import pandas as pd
import numpy as np
from flask import current_app
from sqlalchemy import func, and_, cast, Date
import uuid 
import pytse_client as tse 
import json 

# Import utility functions
from services.utils import get_today_jdate_str, normalize_value, calculate_rsi, calculate_macd, calculate_sma, calculate_bollinger_bands, calculate_volume_ma, calculate_atr, calculate_smart_money_flow, check_candlestick_patterns, check_tsetmc_filters, check_financial_ratios

# تنظیمات لاگینگ برای این ماژول
import logging
logger = logging.getLogger(__name__)

# Define the lookback period for technical data (e.g., 200 days for Golden Key)
GOLDEN_KEY_LOOKBACK_DAYS = 200

# Helper function to convert Jalali date string to Gregorian date object
def convert_jalali_to_gregorian_date(jdate_str):
    """
    Converts a Jalali date string (YYYY-MM-DD) to a Gregorian date object.
    Handles NaN/None values gracefully.
    """
    if pd.notna(jdate_str) and isinstance(jdate_str, str):
        try:
            jy, jm, jd = map(int, jdate_str.split('-'))
            return jdatetime.date(jy, jm, jd).togregorian()
        except ValueError:
            return None # Return None for invalid date strings
    return None # Return None for NaN or None

# Helper function to get the most reliable price
def get_reliable_price(data_row):
    """
    Attempts to get the 'final' price, falls back to 'close' price if 'final' is invalid.
    Assumes data_row is a pandas Series or dict with 'final' and 'close' keys.
    """
    final_price = data_row.get('final')
    close_price = data_row.get('close')

    if pd.notna(final_price) and final_price > 0:
        return float(final_price)
    elif pd.notna(close_price) and close_price > 0:
        return float(close_price)
    return 0.0


# --- بخش ۱: منطق "کلید طلایی" (Golden Key) ---
def run_golden_key_analysis_and_save(): 
    current_app.logger.info("Starting Golden Key filter and scoring process.")
    
    allowed_market_types = [
        'بورس', 'فرابورس', 'بورس کالا', 'صندوق سرمایه گذاری', 'اوراق با درآمد ثابت',
        'مشتقه', 'عمومی', 'پایه فرابورس', 'بورس انرژی', 'اوراق تامین مالی'
    ]

    symbols = ComprehensiveSymbolData.query.filter(
        ComprehensiveSymbolData.market_type.in_(allowed_market_types)
    ).all()
    
    if not symbols:
        current_app.logger.warning("No symbols found in ComprehensiveSymbolData for Golden Key analysis based on allowed market types. Please ensure initial data population is complete.")
        return 0, "No symbols found for Golden Key analysis."

    results = []
    
    today_jdate_str = get_today_jdate_str()

    for symbol_data in symbols:
        symbol_id = symbol_data.symbol_id
        symbol_name = symbol_data.symbol_name
        
        current_app.logger.info(f"Applying Golden Key filters for symbol: {symbol_name} ({symbol_id})")

        historical_records = HistoricalData.query.filter_by(symbol_id=symbol_id)\
                                                .order_by(HistoricalData.jdate.desc())\
                                                .limit(GOLDEN_KEY_LOOKBACK_DAYS).all()
        
        tech_records = TechnicalIndicatorData.query.filter_by(symbol_id=symbol_id)\
                                                .order_by(TechnicalIndicatorData.jdate.desc())\
                                                .limit(GOLDEN_KEY_LOOKBACK_DAYS).all()
        
        fundamental_record = FundamentalData.query.filter_by(symbol_id=symbol_id).first()


        if not historical_records or len(historical_records) < GOLDEN_KEY_LOOKBACK_DAYS:
            current_app.logger.warning(f"Not enough historical data found for {symbol_name} ({len(historical_records)} days). Skipping Golden Key filters.")
            continue
        
        if not tech_records or len(tech_records) < GOLDEN_KEY_LOOKBACK_DAYS:
            current_app.logger.warning(f"Not enough technical data found for {symbol_name} ({len(tech_records)} days). Skipping Golden Key filters.")
            continue

        hist_df = pd.DataFrame([rec.__dict__ for rec in historical_records]).drop(columns=['_sa_instance_state'], errors='ignore')
        tech_df = pd.DataFrame([rec.__dict__ for rec in tech_records]).drop(columns=['_sa_instance_state'], errors='ignore')
        
        if 'jdate' not in hist_df.columns or 'jdate' not in tech_df.columns:
            current_app.logger.warning(f"Jdate column missing in data for {symbol_name}. Skipping Golden Key filters.")
            continue
            
        merged_df = pd.merge(hist_df, tech_df, on='jdate', how='inner')
        
        # FIX: Convert jdate to Gregorian date objects for pandas operations (MODIFIED)
        merged_df['greg_date'] = merged_df['jdate'].apply(convert_jalali_to_gregorian_date)
        merged_df = merged_df.dropna(subset=['greg_date']) # Drop rows where conversion failed
        merged_df = merged_df.sort_values(by='greg_date', ascending=True).reset_index(drop=True)
        # END FIX

        if merged_df.empty:
            current_app.logger.warning(f"Merged data is empty for {symbol_name} ({symbol_id}). Skipping Golden Key filters.")
            continue

        latest_data = merged_df.iloc[-1]
        
        # --- Use reliable price for entry price ---
        entry_price_for_signal = get_reliable_price(latest_data)
        if entry_price_for_signal <= 0:
            current_app.logger.warning(f"Reliable entry price for {symbol_name} is 0 or invalid. Skipping Golden Key signal creation.")
            continue


        total_score = 0
        satisfied_filters = [] 
        reasons = [] 

        # --- پیاده‌سازی ۱۰ فیلتر تکنیکال (مثال‌ها) ---

        # Filter 1: شکست مقاومت + عبور از MA50
        if 'SMA_50' in latest_data and pd.notna(latest_data['SMA_50']) and len(merged_df) >= 50:
            max_20_day_high = merged_df['high'].iloc[-20:-1].max() 
            if pd.notna(max_20_day_high) and latest_data['close'] > max_20_day_high and latest_data['close'] > latest_data['SMA_50']:
                total_score += 15
                satisfied_filters.append("Resistance_Breakout_and_SMA50_Cross")
                reasons.append("Price broke resistance and crossed above SMA-50.")
                current_app.logger.debug(f"Filter 1 matched for {symbol_name}")

        # Filter 2: واگرایی مثبت RSI + افزایش حجم
        if 'RSI' in latest_data and 'volume' in latest_data and len(merged_df) >= 5: 
            avg_volume_5_days = merged_df['volume'].iloc[-5:-1].mean()
            if pd.notna(latest_data['RSI']) and pd.notna(avg_volume_5_days) and \
               latest_data['RSI'] < 40 and latest_data['volume'] > (2 * avg_volume_5_days):
                total_score += 12
                satisfied_filters.append("RSI_Low_Volume_Spike")
                reasons.append("RSI is low with significant volume increase.")
                current_app.logger.debug(f"Filter 2 (simplified) matched for {symbol_name}")

        # Filter 3: کندل پوشا صعودی (Bullish Engulfing) یا چکش (Hammer)
        if len(merged_df) >= 2:
            today_candle = merged_df.iloc[-1]
            yesterday_candle = merged_df.iloc[-2]
            
            if all(pd.notna(val) for val in [today_candle['open'], today_candle['close'], today_candle['high'], today_candle['low'],
                                             yesterday_candle['open'], yesterday_candle['close']]):
                body_yesterday = abs(yesterday_candle['close'] - yesterday_candle['open'])
                body_today = abs(today_candle['close'] - today_candle['open'])

                if (yesterday_candle['close'] < yesterday_candle['open'] and 
                    today_candle['close'] > today_candle['open'] and 
                    today_candle['open'] < yesterday_candle['close'] and 
                    today_candle['close'] > yesterday_candle['open'] and 
                    body_today > body_yesterday): 
                    total_score += 20
                    satisfied_filters.append("Bullish_Engulfing")
                    reasons.append("Detected a Bullish Engulfing pattern.")
                    current_app.logger.debug(f"Filter 3 (Bullish Engulfing) matched for {symbol_name}")

                body = abs(today_candle['close'] - today_candle['open'])
                lower_shadow = min(today_candle['open'], today_candle['close']) - today_candle['low']
                upper_shadow = today_candle['high'] - max(today_candle['open'], today_candle['close'])
                
                if body > 0 and lower_shadow >= 2 * body and upper_shadow < body/2 and today_candle['close'] > today_candle['open']: 
                    total_score += 18
                    satisfied_filters.append("Bullish_Hammer")
                    reasons.append("Detected a Bullish Hammer pattern.")
                    current_app.logger.debug(f"Filter 3 (Hammer) matched for {symbol_name}")


        # Filter 4: عبور MACD از خط سیگنال به سمت بالا
        if 'MACD' in latest_data and 'MACD_Signal' in latest_data and \
           pd.notna(latest_data['MACD']) and pd.notna(latest_data['MACD_Signal']) and len(merged_df) >= 2:
            if latest_data['MACD'] > latest_data['MACD_Signal'] and \
               merged_df.iloc[-2]['MACD'] <= merged_df.iloc[-2]['MACD_Signal']: 
                total_score += 10
                satisfied_filters.append("MACD_Bullish_Cross")
                reasons.append("MACD crossed above its signal line.")
                current_app.logger.debug(f"Filter 4 matched for {symbol_name}")

        # Filter 5: افزایش قدرت خریدار حقیقی + ورود پول
        if 'buy_count_i' in latest_data and 'sell_count_i' in latest_data and \
           pd.notna(latest_data['buy_count_i']) and pd.notna(latest_data['sell_count_i']) and \
           latest_data['buy_count_i'] > 0 and latest_data['sell_count_i'] > 0: 
            
            avg_buy_i_per_trade = latest_data['buy_i_volume'] / latest_data['buy_count_i']
            avg_sell_i_per_trade = latest_data['sell_i_volume'] / latest_data['sell_count_i']

            if avg_buy_i_per_trade > avg_sell_i_per_trade * 1.5: 
                total_score += 15
                satisfied_filters.append("Strong_Real_Buyer_Power")
                reasons.append("Real buyers' average volume per trade is significantly higher than sellers'.")
                current_app.logger.debug(f"Filter 5 (Real Buyer Power) matched for {symbol_name}")

        smart_money_flow_df = calculate_smart_money_flow(hist_df)
        if not smart_money_flow_df.empty:
            latest_smart_money = smart_money_flow_df.iloc[-1]
            if pd.notna(latest_smart_money['individual_net_flow']) and latest_smart_money['individual_net_flow'] > 0 and \
               pd.notna(latest_data['value']) and latest_data['value'] > 0 and \
               latest_smart_money['individual_net_flow'] > (latest_data['value'] * 0.05): 
                total_score += 10
                satisfied_filters.append("Positive_Real_Money_Flow")
                reasons.append("Significant positive real money flow detected.")
                current_app.logger.debug(f"Filter 5 (Money Flow) matched for {symbol_name}")


        # Filter 6: حجم مشکوک (بیش از 3 برابر میانگین 21 روزه)
        if 'volume' in latest_data and pd.notna(latest_data['volume']) and len(merged_df) >= 21:
            avg_volume_21_days = merged_df['volume'].iloc[-21:-1].mean()
            if pd.notna(avg_volume_21_days) and latest_data['volume'] > (3 * avg_volume_21_days):
                total_score += 10
                satisfied_filters.append("Suspicious_Volume_Spike")
                reasons.append("Volume is more than 3 times the 21-day average.")
                current_app.logger.debug(f"Filter 6 matched for {symbol_name}")

        # Filter 7: قیمت آخرین معامله (final) نزدیک به سقف روزانه (high)
        # This filter will now also use get_reliable_price for comparison
        reliable_final_price_for_filter = get_reliable_price(latest_data)
        if reliable_final_price_for_filter > 0 and pd.notna(latest_data['high']) and latest_data['high'] > 0:
            if (latest_data['high'] - reliable_final_price_for_filter) / latest_data['high'] < 0.01: 
                total_score += 8
                satisfied_filters.append("Final_Price_Near_High")
                reasons.append("Final price is very close to daily high.")
                current_app.logger.debug(f"Filter 7 (Final Price near High) matched for {symbol_name}")
            elif reliable_final_price_for_filter > latest_data['close']: 
                total_score += 5
                satisfied_filters.append("Final_Price_Above_Close")
                reasons.append("Final price is higher than closing price (strong demand).")
                current_app.logger.debug(f"Filter 7 (Final > Close) matched for {symbol_name}")


        # Filter 8: P/E پایین تر از P/E گروه
        if fundamental_record and fundamental_record.pe is not None and fundamental_record.pe > 0: 
            group_pe_avg = db.session.query(func.avg(FundamentalData.pe))\
                                     .join(ComprehensiveSymbolData, FundamentalData.symbol_id == ComprehensiveSymbolData.symbol_id)\
                                     .filter(ComprehensiveSymbolData.group_name == symbol_data.group_name).scalar()
            if group_pe_avg and fundamental_record.pe < group_pe_avg: 
                total_score += 8
                satisfied_filters.append("PE_Lower_Than_Group_PE")
                reasons.append(f"P/E ({fundamental_record.pe:.2f}) is lower than group average ({group_pe_avg:.2f}).") 
                current_app.logger.debug(f"Filter 8 matched for {symbol_name}")

        # Filter 9: RSI در حال صعود از محدوده زیر 50
        if 'RSI' in latest_data and pd.notna(latest_data['RSI']) and len(merged_df) >= 2:
           if latest_data['RSI'] > merged_df.iloc[-2]['RSI'] and 30 < latest_data['RSI'] < 50:
                total_score += 7
                satisfied_filters.append("RSI_Rising_From_Below_50")
                reasons.append("RSI is rising from below 50, indicating momentum shift.")
                current_app.logger.debug(f"Filter 9 matched for {symbol_name}")

        # Filter 10: ATR (Average True Range) - for volatility (if needed for Golden Key)
        if 'ATR' in latest_data and pd.notna(latest_data['ATR']) and latest_data['ATR'] > 0:
            # Use reliable price for ATR calculation base if needed, currently uses close
            if latest_data['close'] is not None and latest_data['close'] > 0:
                volatility_percent = (latest_data['ATR'] / latest_data['close']) * 100
                if volatility_percent > 2: 
                    total_score += 5
                    satisfied_filters.append("Moderate_Volatility_ATR")
                    reasons.append(f"ATR ({latest_data['ATR']:.2f}) indicates moderate volatility ({volatility_percent:.2f}% of price).")
                    current_app.logger.debug(f"Filter 10 (ATR) matched for {symbol_name}")
            
        weekly_growth = 0.0
        if len(merged_df) >= 7 and 'close' in merged_df.columns:
            price_7_days_ago = merged_df.iloc[-7]['close']
            if pd.notna(price_7_days_ago) and price_7_days_ago > 0:
                weekly_growth = ((latest_data['close'] - price_7_days_ago) / price_7_days_ago) * 100
        
        if total_score > 0: 
            results.append({
                'symbol_id': symbol_id, 
                'symbol_name': symbol_name, 
                'weekly_growth': weekly_growth,
                'satisfied_filters': json.dumps(satisfied_filters), 
                'reason': " | ".join(reasons) if reasons else "Met Golden Key criteria.",
                'score': total_score, 
                'jdate': today_jdate_str,
                'entry_price': entry_price_for_signal # Use the reliable entry price
            })
            current_app.logger.info(f"Symbol {symbol_name} ({symbol_id}) added as a Golden Key candidate with score {total_score}.")
            
    sorted_results = sorted(results, key=lambda x: x['score'], reverse=True)
    top_5_golden_key = sorted_results[:5]
    
    saved_count = 0
    for item in top_5_golden_key:
        golden_key_entry = GoldenKeyResult.query.filter_by(
            symbol_id=item['symbol_id'], 
            jdate=item['jdate'] 
        ).first()

        if golden_key_entry:
            golden_key_entry.symbol_name = item['symbol_name']
            golden_key_entry.weekly_growth = item['weekly_growth']
            golden_key_entry.satisfied_filters = item['satisfied_filters']
            golden_key_entry.reason = item['reason']
            golden_key_entry.score = item['score']
            golden_key_entry.timestamp = datetime.now()
            # FIX: Always update recommendation_price and recommendation_jdate for existing entries
            golden_key_entry.recommendation_price = item['entry_price']
            golden_key_entry.recommendation_jdate = item['jdate']
            # golden_key_entry.final_price and profit_loss_percentage are updated in evaluate_golden_key_performance
            db.session.add(golden_key_entry)
            current_app.logger.info(f"Updated existing GoldenKeyResult for {item['symbol_name']} on {item['jdate']}.")
        else:
            new_entry = GoldenKeyResult(
                symbol_id=item['symbol_id'],
                symbol_name=item['symbol_name'],
                weekly_growth=item['weekly_growth'],
                satisfied_filters=item['satisfied_filters'],
                reason=item['reason'],
                score=item['score'],
                jdate=item['jdate'],
                timestamp=datetime.now(),
                recommendation_price=item['entry_price'], 
                recommendation_jdate=item['jdate']
            )
            db.session.add(new_entry)
            current_app.logger.info(f"Added new GoldenKeyResult for {item['symbol_name']} on {item['jdate']}.")
        saved_count += 1
        
        # --- Create/Update SignalsPerformance entry for Golden Key ---
        greg_entry_date = jdatetime.date(*map(int, item['jdate'].split('-'))).togregorian()

        signal_performance_entry = SignalsPerformance.query.filter_by(
            symbol_id=item['symbol_id'],
            jentry_date=item['jdate'],
            signal_source='Golden Key'
        ).first()

        if signal_performance_entry:
            signal_performance_entry.entry_price = item['entry_price']
            signal_performance_entry.outlook = "Bullish" # Golden Key is generally bullish
            signal_performance_entry.reason = item['reason']
            signal_performance_entry.probability_percent = item['score'] # Using score as probability
            signal_performance_entry.status = 'active' # Remains active until evaluated
            signal_performance_entry.evaluated_at = datetime.now()
            db.session.add(signal_performance_entry)
            logger.info(f"Updated SignalsPerformance for Golden Key: {item['symbol_name']} on {item['jdate']}.")
        else:
            new_signal_performance = SignalsPerformance(
                signal_id=str(uuid.uuid4()), # Generate unique ID for this signal
                symbol_id=item['symbol_id'],
                symbol_name=item['symbol_name'],
                signal_source='Golden Key',
                entry_date=greg_entry_date,
                jentry_date=item['jdate'],
                entry_price=item['entry_price'],
                outlook="Bullish", # Golden Key signals are typically bullish
                reason=item['reason'],
                probability_percent=item['score'], # Using score as probability
                status='active', # Initially active
                created_at=datetime.now(),
                evaluated_at=datetime.now()
            )
            db.session.add(new_signal_performance)
            logger.info(f"Added new SignalsPerformance entry for Golden Key: {item['symbol_name']} on {item['jdate']}.")
        # --- END NEW SECTION ---

    try:
        db.session.commit()
        message = f"Golden Key filter process completed. Found {len(results)} candidates, saved top {saved_count} Golden Key symbols."
        current_app.logger.info(message)
        return saved_count, message
    except Exception as e:
        db.session.rollback()
        error_message = f"Error saving Golden Key results: {e}"
        current_app.logger.error(error_message, exc_info=True)
        return 0, error_message

def get_golden_key_results(filters=None):
    """
    Retrieves Golden Key results from the database.
    If filters are provided, it filters the results.
    Also returns the list of all available technical filters for the frontend.
    """
    logger.info(f"Retrieving Golden Key results with filters: {filters}")
    
    all_technical_filters_definitions = [
        {"name": "Resistance_Breakout_and_SMA50_Cross", "category": "روند قیمت", "description": "Price broke resistance and crossed above SMA-50."},
        {"name": "RSI_Low_Volume_Spike", "category": "واگرایی", "description": "RSI is low with significant volume increase."},
        {"name": "Bullish_Engulfing", "category": "الگوهای کلاسیک", "description": "Detected a Bullish Engulfing pattern."},
        {"name": "Bullish_Hammer", "category": "الگوهای کلاسیک", "description": "Detected a Bullish Hammer pattern."},
        {"name": "MACD_Bullish_Cross", "category": "میانگین‌ها", "description": "MACD crossed above its signal line."},
        {"name": "Strong_Real_Buyer_Power", "category": "جریان وجوه", "description": "Real buyers' average volume per trade is significantly higher than sellers'."},
        {"name": "Positive_Real_Money_Flow", "category": "جریان وجوه", "description": "Significant positive real money flow detected."},
        {"name": "Suspicious_Volume_Spike", "category": "حجم", "description": "Volume is more than 3 times the 21-day average."},
        {"name": "Final_Price_Near_High", "category": "روند قیمت", "description": "Final price is very close to daily high."},
        {"name": "Final_Price_Above_Close", "category": "روند قیمت", "description": "Final price is higher than closing price (strong demand)."},
        {"name": "PE_Lower_Than_Group_PE", "category": "بنیادی", "description": "P/E is lower than group average."},
        {"name": "RSI_Rising_From_Below_50", "category": "واگرایی", "description": "RSI is rising from below 50, indicating momentum shift."},
        {"name": "Moderate_Volatility_ATR", "category": "روند قیمت", "description": "ATR indicates moderate volatility."},
    ]

    today_jdate_str = get_today_jdate_str()
    query = GoldenKeyResult.query.filter_by(jdate=today_jdate_str).order_by(GoldenKeyResult.score.desc())

    if filters:
        filter_names = [f.strip() for f in filters.split(',') if f.strip()]
        if filter_names:
            all_results_today = query.all()
            filtered_results = []
            for result in all_results_today:
                try:
                    satisfied_filters_list = json.loads(result.satisfied_filters)
                    if all(f in satisfied_filters_list for f in filter_names):
                        filtered_results.append(result)
                except json.JSONDecodeError:
                    logger.warning(f"Failed to decode satisfied_filters for symbol {result.symbol_id}: {result.satisfied_filters}")
                    continue
            results = filtered_results
            logger.info(f"Filtered Golden Key results by: {filters}. Found {len(results)} matches.")
        else:
            results = query.all() 
            logger.info("No valid filters provided, returning all Golden Key results for today.")
    else:
        results = query.all() 
        logger.info("No filters provided, returning all Golden Key results for today.")

    output_stocks = []
    for r in results:
        latest_historical_data = HistoricalData.query.filter_by(symbol_id=r.symbol_id)\
                                                    .order_by(HistoricalData.jdate.desc())\
                                                    .first()
        
        # --- Use reliable price for live display ---
        final_price_live = 0.0
        if latest_historical_data:
            final_price_live = get_reliable_price(latest_historical_data.__dict__) # Pass dict for get_reliable_price

        profit_loss_percentage_live = 0.0
        rec_price = r.recommendation_price # Use the stored recommendation price from GoldenKeyResult

        if rec_price and rec_price > 0: # Ensure rec_price is not None or 0
            profit_loss_percentage_live = ((final_price_live - rec_price) / rec_price) * 100
        
        output_stocks.append({
            'symbol_id': r.symbol_id, 
            'symbol_name': r.symbol_name,
            'total_score': r.score,
            'matched_filters': len(json.loads(r.satisfied_filters)) if r.satisfied_filters else 0,
            'reason': r.reason,
            'weekly_growth': r.weekly_growth,
            'recommendation_price': rec_price, 
            'recommendation_jdate': r.jdate, 
            'final_price': final_price_live, # This is the live final_price for display
            'profit_loss_percentage': profit_loss_percentage_live, # This is the live P/L for display
            'stored_final_price': r.final_price, # Stored final price from GoldenKeyResult
            'stored_profit_loss_percentage': r.profit_loss_percentage # Stored P/L from GoldenKeyResult
        })
    
    output_stocks_sorted = sorted(output_stocks, key=lambda x: x['total_score'], reverse=True)

    return {
        "top_stocks": output_stocks_sorted,
        "technical_filters": all_technical_filters_definitions,
        "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S") 
    }

# --- NEW: Golden Key Performance Evaluation ---
def evaluate_golden_key_performance():
    """
    Evaluates the performance of active Golden Key signals.
    Calculates profit/loss and updates status in SignalsPerformance.
    Intended to be run periodically (e.g., weekly).
    """
    logger.info("Starting Golden Key performance evaluation.")
    
    today_jdate_str = get_today_jdate_str()
    current_greg_date = datetime.now().date()

    # Fetch all active Golden Key signals from SignalsPerformance
    active_golden_key_signals = SignalsPerformance.query.filter(
        SignalsPerformance.status == 'active',
        SignalsPerformance.signal_source == 'Golden Key'
    ).all()

    if not active_golden_key_signals:
        logger.warning("No active Golden Key signals found for evaluation.")
        return False, "No active Golden Key signals to evaluate."

    evaluated_count = 0
    for signal_entry in active_golden_key_signals:
        logger.info(f"Evaluating performance for Golden Key signal: {signal_entry.symbol_name} (ID: {signal_entry.symbol_id}).")

        # Fetch the latest historical data (final price) for the symbol
        latest_historical_data = HistoricalData.query.filter_by(symbol_id=signal_entry.symbol_id)\
                                                    .order_by(HistoricalData.jdate.desc())\
                                                    .first()

        if not latest_historical_data:
            logger.warning(f"No HistoricalData record found for symbol_id: {signal_entry.symbol_id} (Name: {signal_entry.symbol_name}). Cannot evaluate Golden Key signal.")
            continue 
        
        # --- Use reliable price for current price ---
        current_price = get_reliable_price(latest_historical_data.__dict__) # Pass dict for get_reliable_price
        if current_price <= 0:
            logger.warning(f"Reliable current price for symbol_id: {signal_entry.symbol_id} (Name: {signal_entry.symbol_name}) is invalid (Value: {current_price}). Cannot evaluate Golden Key signal.")
            continue 

        # Calculate profit/loss percentage
        profit_loss_percent = 0.0
        if signal_entry.entry_price and signal_entry.entry_price > 0:
            profit_loss_percent = ((current_price - signal_entry.entry_price) / signal_entry.entry_price) * 100
        else:
            logger.warning(f"Entry price for Golden Key signal {signal_entry.symbol_name} is zero or invalid. Cannot calculate profit/loss.")

        # Determine status (e.g., close if profit > X% or loss > Y%)
        status = 'closed_win' if profit_loss_percent > 0 else 'closed_loss' if profit_loss_percent < 0 else 'closed_neutral'
        
        # Update the SignalsPerformance entry
        signal_entry.exit_price = current_price
        signal_entry.jexit_date = today_jdate_str
        signal_entry.exit_date = current_greg_date
        signal_entry.profit_loss_percent = profit_loss_percent
        signal_entry.status = status # Mark as closed
        signal_entry.evaluated_at = datetime.now()
        db.session.add(signal_entry)
        
        logger.info(f"Updated SignalsPerformance for Golden Key: {signal_entry.symbol_name}: Status={status}, P/L={profit_loss_percent:.2f}%")

        # --- Update corresponding GoldenKeyResult entry ---
        golden_key_result_entry = GoldenKeyResult.query.filter_by(
            symbol_id=signal_entry.symbol_id,
            jdate=signal_entry.jentry_date # Use the entry date of the signal
        ).first()

        if golden_key_result_entry:
            golden_key_result_entry.final_price = current_price
            golden_key_result_entry.profit_loss_percentage = profit_loss_percent
            db.session.add(golden_key_result_entry)
            logger.info(f"Updated GoldenKeyResult for {golden_key_result_entry.symbol_name} on {golden_key_result_entry.jdate} with final_price={current_price:.2f} and P/L={profit_loss_percent:.2f}%.")
        else:
            logger.warning(f"Could not find corresponding GoldenKeyResult for SignalsPerformance entry: {signal_entry.symbol_name} on {signal_entry.jentry_date}. Final price not updated in GoldenKeyResult.")
        # --- END NEW SECTION ---

        evaluated_count += 1

    try:
        db.session.commit()
        logger.info(f"Golden Key performance evaluation completed. Evaluated {evaluated_count} signals.")
        
        # After evaluating individual signals, trigger aggregated performance calculation for Golden Key
        # This function is now in performance_service.py
        from services.performance_service import calculate_aggregated_performance as calculate_agg_perf 
        success_agg, msg_agg = calculate_agg_perf(
            period_type='weekly', 
            signal_source='Golden Key'
        )
        logger.info(f"Aggregated performance for Golden Key: {msg_agg}")

        # Also trigger an overall aggregation for the app's performance
        success_overall_agg, msg_overall_agg = calculate_agg_perf(
            period_type='weekly', 
            signal_source='overall' 
        )
        logger.info(f"Aggregated overall performance (weekly): {msg_overall_agg}")
        
        return True, f"Golden Key performance evaluation completed for {evaluated_count} signals."
    except Exception as e:
        db.session.rollback()
        error_message = f"Error during Golden Key performance evaluation: {e}"
        logger.error(error_message, exc_info=True)
        return False, error_message

# The following functions are being moved to performance_service.py:
# get_app_performance_summary
# get_signals_performance_details
# calculate_aggregated_performance
