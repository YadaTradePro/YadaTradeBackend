# -*- coding: utf-8 -*-
# services/market_analysis_service.py

import logging
from datetime import datetime, timedelta, date
import jdatetime
from sqlalchemy.exc import SQLAlchemyError
import json
from typing import Dict, List, Any, Optional
import pandas as pd
import numpy as np


from models import (
    ComprehensiveSymbolData,
    HistoricalData,
    TechnicalIndicatorData,
    GoldenKeyResult,
    AggregatedPerformance,
    WeeklyWatchlistResult,
    DailySectorPerformance, # 👈 تغییر ۱: ایمپورت مدل DailySectorPerformance
)


# Import Jinja2 for templating
from jinja2 import Environment, FileSystemLoader, Template

# Import necessary modules and models from the Flask application structure
from extensions import db
from services.iran_market_data import fetch_iran_market_indices
from services.utils import calculate_smart_money_flow

# تنظیمات لاگینگ
logger = logging.getLogger(__name__)

# -----------------------------------------------------------------------------
# تنظیمات Jinja2: استفاده از قالب‌های جداگانه برای خوانایی بهتر
# -----------------------------------------------------------------------------

# تعریف متغیرهای قالب در سطح ماژول
daily_template = None
weekly_template = None

try:
    template_loader = FileSystemLoader('services/templates')
    template_env = Environment(loader=template_loader)
    daily_template = template_env.get_template('daily_summary.j2')
    weekly_template = template_env.get_template('weekly_summary.j2')
    logger.info("✅ قالب‌های Jinja2 با موفقیت از فایل‌ها بارگذاری شدند.")
except Exception as e:
    logger.error(f"❌ خطای بارگذاری قالب‌های Jinja2 از فایل: {e}. استفاده از قالب‌های درون‌حافظه‌ای.", exc_info=True)

    # Fallback to in-memory templates for robustness
    DAILY_TEMPLATE_STRING = """
**تحلیل روزانه بازار بورس تهران - {{ jdate }}**

## نمای کلی بازار
**شاخص کل:** با تغییر **{{ '%.2f'|format(indices_data.Total_Index.percent|default(0)) }}%**، امروز روندی {{ indices_data.Total_Index.status }} را تجربه کرد.
**شاخص هم‌وزن:** عملکرد {{ indices_data.Equal_Weighted_Index.status }} آن با تغییر **{{ '%.2f'|format(indices_data.Equal_Weighted_Index.percent|default(0)) }}%** نشان‌دهنده وضعیت سهام کوچک و متوسط بود.
{{ smart_money_flow_text }}

{{ sector_summary }} 👈 تغییر ۳: اضافه شدن خلاصه صنایع به قالب روزانه

## تحلیل نمادهای منتخب
{% if all_symbols %}
در ادامه سیگنال‌های جدیدی که امروز شناسایی شده‌اند، آمده است:
{{ symbols_text }}
{% else %}
امروز نماد جدیدی در لیست‌های انتخابی سیگنال‌دهی نشده است.
{% endif %}
"""
    WEEKLY_TEMPLATE_STRING = """
**تحلیل هفتگی بازار بورس تهران - {{ jdate }}**

## نمای کلی بازار
{% if indices_data %}
**عملکرد شاخص‌ها:** شاخص کل در این هفته **{{ '%.2f'|format(indices_data.total_profit_percent|default(0)) }}%** و شاخص هم‌وزن (بر اساس عملکرد GoldenKey) **{{ '%.2f'|format(indices_data.win_rate|default(0)) }}%** نرخ برد داشته است.
{% else %}
خلاصه روند شاخص‌ها در دسترس نیست.
{% endif %}
{{ smart_money_flow_text }}

{{ sector_summary }} 👈 تغییر ۳: اضافه شدن خلاصه صنایع به قالب هفتگی

## عملکرد نمادهای منتخب هفته
{% if all_symbols %}
در ادامه عملکرد سیگنال‌های ارائه شده در طول هفته ارزیابی شده است:
{{ symbols_text }}
{% else %}
در این هفته هیچ نماد جدیدی در لیست‌های انتخابی سیگنال‌دهی نشده است.
{% endif %}
"""
    daily_template = Template(DAILY_TEMPLATE_STRING)
    weekly_template = Template(WEEKLY_TEMPLATE_STRING)
    logger.info("✅ قالب‌های Jinja2 با موفقیت از رشته‌های درون‌حافظه‌ای بارگذاری شدند.")

# -----------------------------------------------------------------------------
# توابع کمکی جدید و بهینه شده
# -----------------------------------------------------------------------------

def _safe_dataframe_from_orm(rows: List[Any], cols: List[str]) -> pd.DataFrame:
    """
    DataFrame ایمن از لیست اشیاء ORM می‌سازد و فیلدهای داخلی SQLAlchemy را حذف می‌کند.
    """
    if not rows:
        return pd.DataFrame(columns=cols)
    # استخراج فقط ستون‌های مورد نیاز
    data = [{c: getattr(r, c, None) for c in cols} for r in rows]
    return pd.DataFrame(data)

def _choose_price_col(df: pd.DataFrame) -> str:
    """
    ستون مناسب قیمت را برای تبدیل حجم به ارزش انتخاب می‌کند: close > close_price > pclosing.
    """
    for c in ('close', 'close_price', 'pclosing'):
        if c in df.columns and df[c].mean() > 0:
            return c
    # اگر هیچ ستون قیمتی مناسب نبود
    df['dummy_price'] = 1000 
    return 'dummy_price'

def _get_day_type() -> str:
    """
    روز هفته را برای تعیین نوع تحلیل (روزانه، هفتگی یا بدون تحلیل) مشخص می‌کند.
    در ایران: شنبه تا چهارشنبه (تحلیل روزانه) | پنجشنبه (بدون تحلیل) | جمعه (تحلیل هفتگی).
    """
    # jdatetime.date.today().weekday() -> Monday=0, ..., Sunday=6.
    # برای خوانایی بیشتر، از نام روز فارسی یا انگلیسی استفاده می‌کنیم.
    j_today = jdatetime.date.today()
    day_name = j_today.strftime('%A') 

    # Sat: 5, Sun: 6, Mon: 0, Tue: 1, Wed: 2, Thu: 3, Fri: 4
    if day_name in ('Saturday', 'Sunday', 'Monday', 'Tuesday', 'Wednesday'):
        return 'daily'
    if day_name == 'Friday':
        return 'weekly'
    if day_name == 'Thursday':
        return 'no_analysis_day'
        
    return 'daily' # Fallback

def _calculate_pnl(entry_price: float, exit_price: Optional[float]) -> Optional[float]:
    """
    درصد سود یا زیان را محاسبه می‌کند.
    """
    if not entry_price or entry_price == 0 or exit_price is None:
        return None
    return round(((exit_price - entry_price) / entry_price) * 100, 2)

def _get_formatted_smart_money_flow_text(net_flow: float, is_weekly: bool) -> str:
    """متن فرمت‌شده برای نمایش وضعیت ورود و خروج پول هوشمند را تولید می‌کند."""
    period = "امروز" if not is_weekly else "در مجموع این هفته"
    # 1e10 = 10,000,000,000 ریال = 1 میلیارد تومان
    if net_flow > 0:
        return f"{period} شاهد ورود پول حقیقی به ارزش تقریبی **{net_flow / 1e10:.2f} میلیارد تومان** به بازار بودیم."
    elif net_flow < 0:
        return f"{period} خروج پول حقیقی به ارزش تقریبی **{abs(net_flow) / 1e10:.2f} میلیارد تومان** از بازار صورت گرفت."
    else:
        return f"{period} جریان پول حقیقی در بازار تقریباً خنثی بود."

def _get_formatted_symbols_text(symbols: List[Any], is_weekly: bool) -> str:
    """متن فرمت‌شده برای نمایش تحلیل نمادهای منتخب را تولید می‌کند."""
    text_parts = []
    for symbol_data in symbols:
        symbol_name = symbol_data.symbol_name
        # فرض می‌کنیم symbol_data از نوع WeeklyWatchlistResult است
        signal_source = getattr(symbol_data, 'signal_source', 'WeeklyWatchlist')
        reasons = getattr(symbol_data, 'reasons', '{}')
        if not isinstance(reasons, str):
            reasons = json.dumps(reasons, ensure_ascii=False)
        entry_price = symbol_data.entry_price

        if not is_weekly:
            daily_change = getattr(symbol_data, 'daily_change_percent', None)
            
            status_text = ""
            if daily_change is not None:
                if daily_change > 0:
                    status_text = f"با رشد **{daily_change:.2f}%** همراه بود."
                elif daily_change < 0:
                    status_text = f"با کاهش **{abs(daily_change):.2f}%** همراه بود."
                else:
                    status_text = "بدون تغییر قیمت بسته شد."
            else:
                status_text = "تغییرات روزانه آن در دسترس نیست."

            text_parts.append(f"**- نماد {symbol_name} ({signal_source}):** {status_text} (دلیل سیگنال: {reasons})")
        else:
            pnl_percent = getattr(symbol_data, 'profit_loss_percentage', None)
            
            status_text = ""
            if pnl_percent is not None:
                if pnl_percent > 0:
                    status_text = f"این هفته **{pnl_percent:.2f}%** سوددهی داشته است."
                elif pnl_percent < 0:
                    status_text = f"این هفته با **{abs(pnl_percent):.2f}%** زیان بسته شد."
                else:
                    status_text = "این هفته بدون تغییر قیمت بسته شد."
            else:
                status_text = "هنوز در وضعیت فعال قرار دارد و ارزیابی نهایی نشده است."
            
            text_parts.append(f"**- نماد {symbol_name}:** {status_text} (دلیل سیگنال: {reasons})")
            
    return "\n".join(text_parts)

def _prepare_indices_data(indices_data: Dict) -> Dict:
    """داده‌های شاخص‌ها را برای استفاده در قالب آماده‌سازی می‌کند."""
    processed_data = {}
    for key, value in indices_data.items():
        percent = value.get('percent', 0) or 0
        status = 'صعودی' if percent > 0 else 'نزولی' if percent < 0 else 'بدون تغییر'
        processed_data[key] = {'percent': percent, 'status': status}
    return processed_data

def _get_top_sectors_summary(db_session: db.session, limit: int = 5) -> str: # 👈 تغییر ۲: تابع جدید
    """
    خلاصه‌ای از {{ limit }} صنعت برتر را بر اساس آخرین رتبه‌بندی تولید می‌کند.
    """
    try:
        # 1. پیدا کردن آخرین تاریخ تحلیل موجود
        latest_date_record = db_session.query(DailySectorPerformance.jdate).order_by(DailySectorPerformance.jdate.desc()).first()
        if not latest_date_record:
            return "\n## رتبه‌بندی صنایع\n**داده‌ای از رتبه‌بندی صنایع برای تحلیل موجود نیست.**"

        latest_jdate_str = latest_date_record[0]
        
        # 2. واکشی صنایع برتر برای آن تاریخ
        top_sectors = DailySectorPerformance.query.filter_by(
            jdate=latest_jdate_str
        ).order_by(DailySectorPerformance.rank.asc()).limit(limit).all()
        
        if not top_sectors:
            return "\n## رتبه‌بندی صنایع\n**رتبه‌بندی صنایع امروز تکمیل نشده است.**"
            
        text_parts = []
        for sector in top_sectors:
            # رتبه‌بندی: 1, 2, 3, ...
            # جابجایی پول: از میلیارد تومان به 2 رقم اعشار
            flow_billion_toman = sector.net_money_flow / 1e10 if sector.net_money_flow else 0
            
            flow_status = "ورود پول" if flow_billion_toman > 0 else "خروج پول"
            flow_value = f"{abs(flow_billion_toman):.2f}"
            
            text_parts.append(
                f"- **{sector.rank}.** {sector.sector_name}: {flow_status} ({flow_value} م.تومان)"
            )

        header = f"\n## رتبه‌بندی {limit} صنعت برتر (تاریخ تحلیل: {latest_jdate_str})\n"
        return header + "\n".join(text_parts)
    
    except Exception as e:
        logger.error(f"❌ خطا در تولید خلاصه صنایع برتر: {e}")
        return "\n## رتبه‌بندی صنایع\n**خطای فنی در استخراج اطلاعات صنایع.**"

# -----------------------------------------------------------------------------
# توابع اصلی تحلیل
# -----------------------------------------------------------------------------

def _generate_daily_summary() -> str:
    """
    یک تحلیل روزانه مختصر از بازار با استفاده از قالب تولید می‌کند.
    این تابع سیگنال‌های *جدید* همان روز را نمایش می‌دهد.
    """
    logger.info("شروع فرآیند تولید تحلیل روزانه بازار...")
    
    try:
        # 1. دریافت داده‌های شاخص از منبع آنلاین
        raw_indices_data = fetch_iran_market_indices()
        indices_data = _prepare_indices_data(raw_indices_data)
        
        # 1.1. پیدا کردن آخرین تاریخ معاملاتی موجود در دیتابیس
        last_trading_day_data = HistoricalData.query.filter(
            HistoricalData.symbol_name.isnot(None)
        ).order_by(HistoricalData.jdate.desc()).first()
        
        if not last_trading_day_data:
            logger.error("❌ هیچ داده تاریخی در دیتابیس یافت نشد.")
            return "❌ هیچ داده‌ای برای تحلیل روزانه موجود نیست."
            
        analysis_date_jdate_str = last_trading_day_data.jdate
        
        current_jdate_str = jdatetime.date.today().strftime('%Y-%m-%d')

        if current_jdate_str != analysis_date_jdate_str:
            logger.info("بازار امروز (%s) ممکن است بسته باشد یا داده‌ای دریافت نشده است. تحلیل بر اساس آخرین روز معاملاتی (%s) تولید می‌شود.", 
                         current_jdate_str, analysis_date_jdate_str)
        
        # 2. تنظیم تاریخ برای کوئری‌های اصلی و تاریخ دیروز
        yesterday_data = HistoricalData.query.filter(
            HistoricalData.jdate < analysis_date_jdate_str
        ).order_by(HistoricalData.jdate.desc()).first()
        
        yesterday_jdate_str = yesterday_data.jdate if yesterday_data else None

        # 2. دریافت داده‌های تاریخی مورد نیاز برای محاسبه جریان پول
        historical_data_for_df_cols = ['symbol_id', 'symbol_name', 'jdate', 'close', 'close_price', 'pclosing',
                                        'buy_i_volume', 'sell_i_volume', 'buy_count_i', 'sell_count_i', 'value']
        
        historical_data_for_df_rows = HistoricalData.query.with_entities(
            *[getattr(HistoricalData, col) for col in historical_data_for_df_cols if hasattr(HistoricalData, col)]
        ).filter(
            HistoricalData.jdate == analysis_date_jdate_str, 
            HistoricalData.symbol_name.isnot(None)
        ).all()
        
        # **استفاده از helper برای ساخت ایمن DataFrame**
        df = _safe_dataframe_from_orm(historical_data_for_df_rows, historical_data_for_df_cols)
        
        if df.empty:
            logger.warning("❌ DataFrame داده‌های تاریخی برای روز %s خالی است.", analysis_date_jdate_str)
            total_net_real_money_flow = 0
        else:
            # 1. آماده‌سازی ستون‌ها
            for col in ['buy_i_volume', 'sell_i_volume', 'close', 'close_price', 'pclosing', 'value', 'buy_count_i', 'sell_count_i']:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            
            # فراخوانی calculate_smart_money_flow
            smart_money_flow_df = calculate_smart_money_flow(df)

            # محاسبه جریان خالص حجمی (robust logic)
            if smart_money_flow_df is not None and 'individual_net_flow' in smart_money_flow_df.columns and len(smart_money_flow_df) == len(df):
                net_volume_flow = smart_money_flow_df['individual_net_flow']
            else:
                net_volume_flow = df['buy_i_volume'] - df['sell_i_volume']

            # یافتن ستون قیمت و محاسبه ارزش ریالی
            price_col = _choose_price_col(df)
            df['net_real_value_flow'] = net_volume_flow * df[price_col]
            total_net_real_money_flow = df['net_real_value_flow'].sum()
        
        # 3. دریافت سیگنال‌های جدید همان روز از پایگاه داده
        # **GoldenKeyResults حذف شد - تمرکز فقط بر WeeklyWatchlist**
        weekly_watchlist_results = WeeklyWatchlistResult.query.filter(WeeklyWatchlistResult.jentry_date == analysis_date_jdate_str).all() 
        
        all_new_symbols = weekly_watchlist_results

        # 4. محاسبه تغییرات روزانه برای هر نماد با استفاده از _calculate_pnl
        for symbol in all_new_symbols:
            # استفاده از 'close' به عنوان قیمت مرجع ورود/خروج
            today_data = HistoricalData.query.filter_by(symbol_id=symbol.symbol_id, jdate=analysis_date_jdate_str).first() 
            
            if yesterday_jdate_str:
                yesterday_data = HistoricalData.query.filter_by(symbol_id=symbol.symbol_id, jdate=yesterday_jdate_str).first()

                if today_data and yesterday_data and yesterday_data.close is not None and today_data.close is not None:
                    # **استفاده از _calculate_pnl**
                    daily_change = _calculate_pnl(yesterday_data.close, today_data.close)
                    
                    setattr(symbol, 'daily_change_percent', daily_change)
                else:
                    setattr(symbol, 'daily_change_percent', None)
            else:
                setattr(symbol, 'daily_change_percent', None)

        
        # 5. **اضافه کردن تحلیل صنایع** 👈 تغییر ۴: فراخوانی تابع جدید
        sector_summary_text = _get_top_sectors_summary(db.session, limit=5)

        # 6. آماده‌سازی داده‌ها برای ارسال به قالب
        data_for_template = {
            'jdate': analysis_date_jdate_str, 
            'indices_data': indices_data,
            'smart_money_flow_text': _get_formatted_smart_money_flow_text(total_net_real_money_flow, is_weekly=False),
            'sector_summary': sector_summary_text, # 👈 متغیر جدید
            'all_symbols': all_new_symbols,
            'symbols_text': _get_formatted_symbols_text(all_new_symbols, is_weekly=False)
        }
        
        # **اصلاح: استفاده از **kwargs برای render**
        return daily_template.render(**data_for_template)

    except SQLAlchemyError as e:
        logger.error(f"❌ خطای پایگاه داده در تولید تحلیل روزانه: {e}", exc_info=True)
        return "❌ متأسفانه به دلیل خطای پایگاه داده، امکان تولید تحلیل روزانه وجود ندارد."
    except Exception as e:
        logger.error(f"❌ خطای ناشناخته در تولید تحلیل روزانه: {e}", exc_info=True)
        return "❌ متأسفانه به دلیل خطای فنی، امکان تولید تحلیل روزانه وجود ندارد."

def _generate_weekly_summary() -> str:
    """
    یک تحلیل هفتگی جامع از عملکرد بازار و نمادها تولید می‌کند.
    """
    logger.info("شروع فرآیند تولید تحلیل هفتگی بازار...")
    
    try:
        week_ago_greg = datetime.now().date() - timedelta(days=7)
        week_ago_jdate_str = jdatetime.date.fromgregorian(date=week_ago_greg).strftime('%Y-%m-%d')
        
        # 1. دریافت داده‌های تجمیعی عملکرد
        aggregated_data = AggregatedPerformance.query.filter(
            AggregatedPerformance.period_type == 'weekly'
        ).order_by(AggregatedPerformance.created_at.desc()).first()
        
        # **تبدیل شی ORM به dict برای استفاده در قالب**
        if aggregated_data:
            indices_for_template = {
                'total_profit_percent': getattr(aggregated_data, 'total_profit_percent', 0),
                'win_rate': getattr(aggregated_data, 'win_rate', 0),
            }
        else:
            indices_for_template = {}
        
        # 2. دریافت داده‌های HistoricalData برای محاسبه جریان پول
        historical_data_for_df_cols = ['symbol_id', 'symbol_name', 'jdate', 'close', 'close_price', 'pclosing',
                                        'buy_i_volume', 'sell_i_volume', 'buy_count_i', 'sell_count_i', 'value']
        
        historical_data_for_df_rows = HistoricalData.query.with_entities(
            *[getattr(HistoricalData, col) for col in historical_data_for_df_cols if hasattr(HistoricalData, col)]
        ).filter(
            HistoricalData.jdate >= week_ago_jdate_str,
            HistoricalData.symbol_name.isnot(None)
        ).all()
        
        # **استفاده از helper برای ساخت ایمن DataFrame**
        df = _safe_dataframe_from_orm(historical_data_for_df_rows, historical_data_for_df_cols)

        if df.empty:
            logger.warning("❌ DataFrame داده‌های تاریخی برای دوره هفتگی خالی است.")
            total_net_real_money_flow = 0
        else:
            # 1. آماده‌سازی ستون‌ها
            for col in ['buy_i_volume', 'sell_i_volume', 'close', 'close_price', 'pclosing', 'value', 'buy_count_i', 'sell_count_i']:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            
            # فراخوانی calculate_smart_money_flow
            weekly_smart_money_flow_df = calculate_smart_money_flow(df)
            
            # محاسبه جریان خالص حجمی (robust logic)
            if weekly_smart_money_flow_df is not None and 'individual_net_flow' in weekly_smart_money_flow_df.columns and len(weekly_smart_money_flow_df) == len(df):
                net_volume_flow = weekly_smart_money_flow_df['individual_net_flow']
            else:
                net_volume_flow = df['buy_i_volume'] - df['sell_i_volume']

            # یافتن ستون قیمت و محاسبه ارزش ریالی
            price_col = _choose_price_col(df)
            df['net_real_value_flow'] = net_volume_flow * df[price_col]
            total_net_real_money_flow = df['net_real_value_flow'].sum()

        # 3. دریافت رکوردهای سیگنال‌های هفته 
        # **GoldenKeyRecords حذف شد - تمرکز فقط بر WeeklyWatchlist**
        weekly_watchlist_records = WeeklyWatchlistResult.query.filter(WeeklyWatchlistResult.jentry_date >= week_ago_jdate_str).all()
        
        all_week_symbols = weekly_watchlist_records
        
        # 4. **اضافه کردن تحلیل صنایع** 👈 تغییر ۴: فراخوانی تابع جدید
        sector_summary_text = _get_top_sectors_summary(db.session, limit=5)
        
        # 5. آماده‌سازی داده‌ها برای ارسال به قالب
        data_for_template = {
            'jdate': jdatetime.date.today().strftime('%Y-%m-%d'),
            'indices_data': indices_for_template, # استفاده از دیکشنری تبدیل‌شده
            'smart_money_flow_text': _get_formatted_smart_money_flow_text(total_net_real_money_flow, is_weekly=True),
            'sector_summary': sector_summary_text, # 👈 متغیر جدید
            'all_symbols': all_week_symbols,
            'symbols_text': _get_formatted_symbols_text(all_week_symbols, is_weekly=True)
        }
        
        # **اصلاح: استفاده از **kwargs برای render**
        return weekly_template.render(**data_for_template)

    except SQLAlchemyError as e:
        logger.error(f"❌ خطای پایگاه داده در تولید تحلیل هفتگی: {e}", exc_info=True)
        return "❌ متأسفانه به دلیل خطای پایگاه داده، امکان تولید تحلیل هفتگی وجود ندارد."
    except Exception as e:
        logger.error(f"❌ خطای ناشناخته در تولید تحلیل هفتگی: {e}", exc_info=True)
        return "❌ متأسفانه به دلیل خطای فنی، امکان تولید تحلیل هفتگی وجود ندارد."

# -----------------------------------------------------------------------------
# تابع اصلی سرویس
# -----------------------------------------------------------------------------

def generate_market_summary() -> str:
    """
    تابع اصلی سرویس که بسته به روز هفته، تحلیل روزانه یا هفتگی را برمی‌گرداند.
    """
    logger.info("سرویس تحلیل بازار فراخوانی شد.")
    day_type = _get_day_type()
    
    if day_type == 'daily':
        return _generate_daily_summary()
    elif day_type == 'weekly':
        return _generate_weekly_summary()
    elif day_type == 'no_analysis_day':
        logger.info("امروز پنجشنبه است؛ تحلیل بازار منتشر نمی‌شود.")
        return "در روز پنجشنبه، بازار سرمایه فعال نیست و تحلیل روزانه منتشر نمی‌شود."
    
    return "نوع تحلیل برای روز جاری قابل تشخیص نیست."

# -----------------------------------------------------------------------------
# شبه‌کد برای فرآیند پس‌زمینه (جهت مستندسازی)
# -----------------------------------------------------------------------------

def update_evaluated_prices_job():
    """
    شبه‌کد: این تابع باید توسط یک زمان‌بند (Scheduler) مانند Celery یا Cron
    به صورت روزانه اجرا شود تا قیمت خروج و سود/زیان سیگنال‌های فعال را به‌روز کند.
    """
    logger.info("شروع جاب زمان‌بندی شده برای ارزیابی سیگنال‌های فعال...")
    try:
        # 1. بازیابی تمام سیگنال‌های فعال که هنوز قیمت خروج ندارند.
        active_signals = WeeklyWatchlistResult.query.filter(
            WeeklyWatchlistResult.status == 'active'
        ).all()
        
        # 2. برای هر سیگنال، آخرین قیمت را از دیتابیس لوکال دریافت و وضعیت آن را ارزیابی کن.
        for signal in active_signals:
            try:
                # قیمت از دیتابیس لوکال HistoricalData خوانده می‌شود
                latest_historical_data = HistoricalData.query.filter_by(
                    symbol_id=signal.symbol_id
                ).order_by(HistoricalData.jdate.desc()).first()

                if not latest_historical_data:
                    logger.warning(f"❌ آخرین داده تاریخی برای نماد {signal.symbol_name} یافت نشد. ارزیابی انجام نشد.")
                    continue

                # استفاده از 'close' به عنوان قیمت خروج یکنواخت (مانند گزارش روزانه)
                latest_price = getattr(latest_historical_data, 'close', getattr(latest_historical_data, 'close_price', None))

                # ... منطق ارزیابی (حد سود/ضرر) ...
                if latest_price: 
                    signal.exit_price = latest_price 
                    signal.status = 'evaluated' 
                    # استفاده از _calculate_pnl
                    signal.profit_loss_percentage = _calculate_pnl(signal.entry_price, signal.exit_price)
                    db.session.add(signal)
            except Exception as e:
                logger.error(f"❌ خطا در ارزیابی نماد {signal.symbol_name}: {e}")
            
        db.session.commit()
        logger.info(f"ارزیابی {len(active_signals)} سیگنال فعال با موفقیت انجام شد.")
        
    except SQLAlchemyError as e:
        db.session.rollback()
        logger.error(f"❌ خطای پایگاه داده در جاب ارزیابی: {e}", exc_info=True)
    except Exception as e:
        db.session.rollback() # بهتر است در خطاهای بیرونی هم rollback کنیم
        logger.error(f"❌ خطای ناشناخته در جاب ارزیابی: {e}", exc_info=True)