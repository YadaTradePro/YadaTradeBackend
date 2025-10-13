# services/sector_analysis_service.py
import pandas as pd
from extensions import db
from models import HistoricalData, ComprehensiveSymbolData, DailySectorPerformance
from datetime import timedelta
import jdatetime
from sqlalchemy import func
import logging

logger = logging.getLogger(__name__)

def run_daily_sector_analysis():
    """
    صنایع را بر اساس ارزش معاملات و ورود پول هوشمند در ۵ روز گذشته تحلیل و رتبه‌بندی کرده
    و نتایج را در دیتابیس ذخیره می‌کند.
    """
    today_jdate = jdatetime.date.today()
    # 7 روز برای اطمینان از پوشش 5 روز معاملاتی
    start_date_j = (today_jdate - timedelta(days=7)).strftime('%Y-%m-%d')
    today_jdate_str = today_jdate.strftime('%Y-%m-%d')

    # ۱. خواندن داده‌های مورد نیاز از دیتابیس
    hist_records = db.session.query(
        HistoricalData.symbol_id,
        HistoricalData.jdate, 
        HistoricalData.value, # ارزش معاملات
        (HistoricalData.buy_i_volume - HistoricalData.sell_i_volume).label('net_flow')
    ).filter(HistoricalData.jdate >= start_date_j).all()
    
    hist_df = pd.DataFrame(hist_records)

    if hist_df.empty:
        logger.info("داده‌ای برای تحلیل صنعت یافت نشد.")
        return

    # B. دریافت اطلاعات صنعت هر نماد
    symbols_query = db.session.query(
        ComprehensiveSymbolData.symbol_id,
        ComprehensiveSymbolData.group_name
    ).all()
    symbols_df = pd.DataFrame(symbols_query, columns=['symbol_id', 'group_name'])

    # C. ترکیب دو DataFrame در Pandas
    df = pd.merge(hist_df, symbols_df, on='symbol_id', how='left')
    df = df.dropna(subset=['group_name'])
    
    if df.empty:
        logger.warning("داده‌ای با اطلاعات صنعت مرتبط یافت نشد. تحلیل متوقف شد.")
        return

    # ۲. گروه‌بندی و محاسبه معیارها برای هر صنعت (میانگین عملکرد روزانه)
    daily_sector_performance = df.groupby(['jdate', 'group_name']).agg(
        daily_trade_value=('value', 'sum'),
        daily_net_money_flow=('net_flow', 'sum')
    ).reset_index()

    sector_performance = daily_sector_performance.groupby('group_name').agg(
        avg_daily_trade_value=('daily_trade_value', 'mean'), 
        avg_daily_net_money_flow=('daily_net_money_flow', 'mean'),
    ).reset_index()

    # ۳. رتبه‌بندی صنایع 
    sector_performance['value_rank'] = sector_performance['avg_daily_trade_value'].rank(ascending=False, method='min')
    sector_performance['flow_rank'] = sector_performance['avg_daily_net_money_flow'].rank(ascending=False, method='min')
    
    sector_performance['final_rank'] = sector_performance['value_rank'] + sector_performance['flow_rank']
    sector_performance = sector_performance.sort_values('final_rank').reset_index(drop=True)
    sector_performance['rank'] = sector_performance.index + 1 

    # ۴. ذخیره نتایج در دیتابیس (رفع خطای UNIQUE constraint failed)
    
    # 4.1. حذف رکوردهای قدیمی برای تاریخ امروز (Upsert تمیز)
    try:
        db.session.query(DailySectorPerformance).filter(
            DailySectorPerformance.jdate == today_jdate_str
        ).delete()
        logger.info(f"✅ رکوردهای تحلیل صنعت برای تاریخ {today_jdate_str} حذف شدند تا جایگزین شوند.")
    except Exception as e:
        db.session.rollback()
        logger.error(f"❌ خطا در حذف رکوردهای قدیمی تحلیل صنعت: {e}")
        raise

    # 4.2. آماده‌سازی داده‌ها برای درج گروهی (Bulk Insert)
    records_to_insert = []
    for _, row in sector_performance.iterrows():
        records_to_insert.append({
            'jdate': today_jdate_str, 
            'sector_name': row['group_name'],
            'total_trade_value': row['avg_daily_trade_value'],
            'net_money_flow': row['avg_daily_net_money_flow'],
            'rank': row['rank']
        })

    # 4.3. درج گروهی داده‌های جدید
    db.session.bulk_insert_mappings(DailySectorPerformance, records_to_insert)
        
    db.session.commit()
    logger.info(f"تحلیل و رتبه‌بندی {len(sector_performance)} صنعت بر اساس میانگین {len(daily_sector_performance['jdate'].unique())} روز گذشته (تاریخ: {today_jdate_str}) با موفقیت انجام و ذخیره شد.")