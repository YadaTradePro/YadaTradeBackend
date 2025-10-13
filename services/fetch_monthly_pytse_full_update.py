import pytse_client as tse
import pandas as pd
from typing import List, Optional, Tuple, Dict
from datetime import date, datetime
from sqlalchemy.orm import Session
from sqlalchemy import func, or_, text
from sqlalchemy.exc import SQLAlchemyError
import jdatetime
import traceback
import numpy as np
import time
import logging

# --- تنظیمات اولیه و Import های فرضی ---

# فرض کنید این مدل‌ها در models.py تعریف شده‌اند
# **توجه:** در محیط واقعی، این کلاس‌ها باید از دیتابیس (مثل db.Model) import شوند.
class HistoricalData:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)
    def __repr__(self):
        return f'<HistoricalData {self.symbol_name}>'

class FundamentalData:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)
    def __repr__(self):
        return f'<FundamentalData {self.symbol_id}>'

class ComprehensiveSymbolData:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)
        self.last_historical_update_date = None
    def __repr__(self):
        return f'<ComprehensiveSymbolData {self.symbol_name}>'

# فرض کنید این توابع و Logger در services.utils یا جای دیگری تعریف شده‌اند
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO) # تنظیم سطح لاگ برای نمایش پیام‌ها

def safe_sleep(delay: float):
    """مکث ایمن برای مدیریت تایم‌اوت شبکه"""
    time.sleep(delay)

DEFAULT_PER_SYMBOL_DELAY = 1.5 # تاخیر بین هر درخواست Ticker
SYMBOL_BATCH_SIZE = 100

# **توابع تحلیل به عنوان Placeholder** (فرض بر import از services.data_fetch_and_process.py)
# این توابع باید پیاده‌سازی واقعی را داشته باشند.
def run_technical_analysis(db_session, limit: int = None, symbols_list: list = None): 
    logger.info("فراخوانی run_technical_analysis...")
    return len(symbols_list) if symbols_list else 0, "تحلیل تکنیکال شبیه‌سازی شده"

def run_candlestick_detection(db_session, limit: int = None, symbols_list: list = None): 
    logger.info("فراخوانی run_candlestick_detection...")
    return len(symbols_list) if symbols_list else 0

# --------------------------------------------------------------------------------
# 2. توابع کمکی
# --------------------------------------------------------------------------------

def delete_all_historical_data(db_session: Session) -> int:
    """
    حذف تمام رکوردهای جدول HistoricalData.
    استفاده از TRUNCATE برای کارایی بالاتر در دیتابیس‌های بزرگ.
    """
    try:
        logger.warning("🗑️ در حال حذف تمام رکوردهای جدول HistoricalData با TRUNCATE...")
        # توجه: این دستور برای PostgreSQL و MySQL (با NO CHECK) کار می‌کند.
        db_session.execute(text("TRUNCATE TABLE stock_data RESTART IDENTITY CASCADE;"))
        db_session.commit()
        logger.info(f"✅ جدول HistoricalData (stock_data) خالی شد.")
        return 0 
    except SQLAlchemyError as e:
        logger.error(f"❌ خطای پایگاه داده در حذف HistoricalData: {e}", exc_info=True)
        db_session.rollback()
        raise

def update_fundamental_data(db_session: Session, ticker: tse.Ticker, symbol_id: str) -> bool:
    """
    دریافت اطلاعات فاندامنتال، هندل کردن مقادیر نامعتبر و ذخیره/به‌روزرسانی در FundamentalData.
    """
    try:
        # پاکسازی Session قبل از Merge برای جلوگیری از خطاهای Integrity
        db_session.expire_all()
        
        data = {
            'eps': getattr(ticker, 'eps', None),
            'pe': getattr(ticker, 'p_e_ratio', None),
            'group_pe_ratio': getattr(ticker, 'group_p_e_ratio', None),
            'psr': getattr(ticker, 'psr', None),
            'p_s_ratio': getattr(ticker, 'p_s_ratio', None),
            'base_volume': getattr(ticker, 'base_volume', None),
            'float_shares': getattr(ticker, 'float_shares', None),
            'market_cap': getattr(ticker, 'market_cap', None),
        }
        
        # هندل کردن مقادیر نامعتبر ('-', '--') و تبدیل Market Cap
        for key, value in data.items():
            if isinstance(value, str):
                cleaned_value = value.strip()
                if cleaned_value in ['-', '--', 'nan', ''] or pd.isna(value):
                    data[key] = None
                elif key == 'market_cap':
                    try:
                        data[key] = int(cleaned_value.replace(',', ''))
                    except:
                        data[key] = None
            elif pd.isna(value) or value in (np.nan, np.inf, -np.inf):
                data[key] = None
            
        fundamental_record = FundamentalData(
            symbol_id=symbol_id,
            eps=data['eps'],
            pe=data['pe'],
            group_pe_ratio=data['group_pe_ratio'],
            psr=data['psr'],
            p_s_ratio=data['p_s_ratio'],
            market_cap=data['market_cap'],
            base_volume=data['base_volume'],
            float_shares=data['float_shares']
        )
        
        db_session.merge(fundamental_record)
        return True
    
    except Exception as e:
        logger.error(f"❌ خطا در به‌روزرسانی FundamentalData برای {symbol_id}: {e}", exc_info=True)
        return False

def _commit_historical_batch(db_session: Session, records_to_commit: List[HistoricalData], symbol_ids_to_update: set) -> Tuple[int, bool]:
    """یک بچ از رکوردهای HistoricalData را درج کرده و ComprehensiveSymbolData را به‌روز می‌کند."""
    if not records_to_commit:
        return 0, True
        
    try:
        logger.info(f"⏳ شروع درج دسته‌ای برای بچ {len(symbol_ids_to_update)} نماد ({len(records_to_commit)} رکورد)...")
        # Bulk insert
        db_session.bulk_save_objects(records_to_commit)
        
        # استفاده از datetime.now() برای ستون DateTime
        current_timestamp = datetime.now()
        
        # به‌روزرسانی تاریخ آخرین به‌روزرسانی در ComprehensiveSymbolData برای نمادهای این بچ
        db_session.query(ComprehensiveSymbolData).filter(
            ComprehensiveSymbolData.symbol_id.in_(list(symbol_ids_to_update))
        ).update(
            {ComprehensiveSymbolData.last_historical_update_date: current_timestamp}, 
            synchronize_session='fetch'
        )
        
        db_session.commit()
        logger.info(f"✅ بچ {len(symbol_ids_to_update)} نماد با موفقیت ذخیره و کامیت شد.")
        return len(records_to_commit), True
        
    except SQLAlchemyError as e:
        db_session.rollback()
        logger.error(f"❌ خطای دیتابیس در درج بچ {len(symbol_ids_to_update)} نماد. Rollback انجام شد.: {e}", exc_info=True)
        return 0, False
    except Exception as e:
        db_session.rollback()
        logger.error(f"❌ خطای ناشناخته در درج بچ. Rollback انجام شد.: {e}", exc_info=True)
        return 0, False


# --------------------------------------------------------------------------------
# 3. تابع اصلی با مکانیزم بچ‌بندی
# --------------------------------------------------------------------------------
def fetch_full_historical_pytse(
    db_session: Session,
    symbols_to_update: Optional[List[str]] = None
) -> Tuple[int, str]:
    """
    رفرش کامل داده‌های تاریخی و فاندامنتال با مکانیزم بچ‌بندی ۱۰۰ تایی و مدیریت خطا.
    """
    logger.info("🧠 شروع رفرش کامل داده‌های تاریخی (Full Historical Refresh) با بچ‌بندی...")
    
    # 1. آماده‌سازی و دریافت لیست نمادها
    try:
        query = db_session.query(ComprehensiveSymbolData).filter(
            ComprehensiveSymbolData.symbol_name != None 
        )
        if symbols_to_update:
            query = query.filter(ComprehensiveSymbolData.symbol_name.in_(symbols_to_update))
            
        all_symbols = query.all()
        
        if not all_symbols:
            return 0, "❌ هیچ نمادی برای به‌روزرسانی در ComprehensiveSymbolData یافت نشد."
            
        # 2. حذف تمام داده‌های HistoricalData
        delete_all_historical_data(db_session)

    except Exception as e:
        return 0, f"❌ خطای آماده‌سازی دیتابیس یا دریافت نمادها: {e}"

    # متغیرهای جدید برای مدیریت بچ
    current_batch_records = []
    current_batch_symbol_ids = set()
    total_records_inserted = 0
    total_symbols = len(all_symbols)
    updated_symbol_ids = set() 

    # 3. حلقه برای پردازش نماد به نماد
    for index, sym in enumerate(all_symbols):
        symbol_name = sym.symbol_name
        symbol_id = sym.symbol_id

        if not symbol_name:
             logger.warning(f"⚠️ نماد با ID {symbol_id} نام ندارد. رد شد.")
             continue

        logger.info(f"📊 پردازش ({index+1}/{total_symbols}) داده‌های تاریخی برای {symbol_name} (ID: {symbol_id})")
        
        try:
            # --- فاز فچ داده‌ها (با مدیریت تایم‌اوت و پایداری شبکه) ---
            
            # الف) دریافت Ticker برای Final Price و Fundamental Data
            ticker_adj = tse.Ticker(symbol_name, adjust=True)
            
            # Sleep بین دو فراخوانی Ticker برای پایداری شبکه
            safe_sleep(0.5) 
            
            # ب) دریافت Ticker برای Last Price
            ticker_unadj = tse.Ticker(symbol_name, adjust=False)
            
            df_adj = ticker_adj.history.copy()
            df_unadj = ticker_unadj.history.copy()
            
            # هندل کردن None برای client_types
            df_client_types = ticker_adj.client_types.copy() if ticker_adj.client_types is not None else pd.DataFrame()
            
            if df_adj is None or df_adj.empty or df_unadj is None or df_unadj.empty:
                logger.info(f"ℹ️ دیتای تاریخی معتبری برای نماد {symbol_name} یافت نشد. رد شد.")
                safe_sleep(DEFAULT_PER_SYMBOL_DELAY)
                continue
            
            # 4. آپدیت FundamentalData
            update_fundamental_data(db_session, ticker_adj, symbol_id)

            # --- فاز ادغام، محاسبه و آماده‌سازی رکوردها ---
            
            # تبدیل تاریخ‌ها به datetime و ادغام
            df_adj['date'] = pd.to_datetime(df_adj['date'])
            df_unadj['date'] = pd.to_datetime(df_unadj['date'])
            if 'date' in df_client_types.columns:
                df_client_types['date'] = pd.to_datetime(df_client_types['date'])
            
            # چک کردن 'adj_close' قبل از تغییر نام
            if 'adj_close' in df_adj.columns: 
                df_adj.rename(columns={'adj_close': 'final_price_value'}, inplace=True)
            else:
                df_adj.rename(columns={'close': 'final_price_value'}, inplace=True)

            df_unadj.rename(columns={'close': 'last_price_value'}, inplace=True)
            
            df_merged = pd.merge(
                df_adj, df_unadj[['date', 'last_price_value']], 
                on='date', how='inner'
            )
            
            df_final = pd.merge(df_merged, df_client_types, on='date', how='left')

            # 5. اصلاح و محاسبه ستون‌ها (PLC, PLP, PCC, PCP و نگاشت)
            df_final.sort_values(by='date', inplace=True)
            
            df_final['final'] = pd.to_numeric(df_final['final_price_value'], errors='coerce')
            df_final['close'] = pd.to_numeric(df_final['last_price_value'], errors='coerce')
            
            # yesterday_price بر اساس قیمت پایانی تعدیل‌شده است
            df_final['yesterday_price'] = df_final['final'].shift(1) 
            
            # Price Changes (مدیریت تقسیم بر صفر با replace(0, np.nan))
            df_final['pcc'] = df_final['final'] - df_final['yesterday_price']
            df_final['pcp'] = (df_final['pcc'] / df_final['yesterday_price'].replace(0, np.nan)) * 100
            
            df_final['plc'] = df_final['close'] - df_final['yesterday_price']
            df_final['plp'] = (df_final['plc'] / df_final['yesterday_price'].replace(0, np.nan)) * 100
            
            # سایر نگاشت‌ها
            df_final['mv'] = df_final['value']
            df_final['num_trades'] = df_final['count'] 
            
            # نگاشت ستون‌های حقیقی/حقوقی
            df_final.rename(columns={
                "individual_buy_count": "buy_count_i", "corporate_buy_count": "buy_count_n",
                "individual_sell_count": "sell_count_i", "corporate_sell_count": "sell_count_n",
                "individual_buy_vol": "buy_i_volume", "corporate_buy_vol": "buy_n_volume",
                "individual_sell_vol": "sell_i_volume", "corporate_sell_vol": "sell_n_volume"
            }, inplace=True) 

            # تبدیل تاریخ میلادی به شمسی
            df_final['jdate'] = df_final['date'].apply(
                lambda x: jdatetime.date.fromgregorian(date=x.date()).strftime("%Y-%m-%d")
            )
            
            # 6. آماده‌سازی برای درج دسته‌ای (Bulk Insert)
            db_columns_to_keep = [
                'date', 'jdate', 'open', 'high', 'low', 'close', 'final', 
                'volume', 'value', 'num_trades', 'yesterday_price', 
                'plc', 'plp', 'pcc', 'pcp', 'mv',
                'buy_count_i', 'buy_count_n', 'sell_count_i', 'sell_count_n', 
                'buy_i_volume', 'buy_n_volume', 'sell_i_volume', 'sell_n_volume',
            ]
            
            final_data = df_final[[col for col in db_columns_to_keep if col in df_final.columns]].copy()
            final_data.replace([np.inf, -np.inf, np.nan], None, inplace=True)
            records_dict = final_data.to_dict('records')
            
            # تبدیل دیکشنری به آبجکت‌های HistoricalData
            historical_records = [
                HistoricalData(
                    symbol_id=symbol_id, symbol_name=sym.symbol_name,
                    date=rec['date'].date() if rec.get('date') is not None else None, 
                    jdate=rec.get('jdate'), 
                    open=rec.get('open'), high=rec.get('high'), low=rec.get('low'),
                    close=rec.get('close'), final=rec.get('final'), 
                    yesterday_price=rec.get('yesterday_price'), 
                    volume=rec.get('volume'), value=rec.get('value'),
                    num_trades=rec.get('num_trades'), mv=rec.get('mv'),
                    plc=rec.get('plc'), plp=rec.get('plp'), pcc=rec.get('pcc'), pcp=rec.get('pcp'),
                    buy_count_i=rec.get('buy_count_i'), buy_count_n=rec.get('buy_count_n'), 
                    sell_count_i=rec.get('sell_count_i'), sell_count_n=rec.get('sell_count_n'), 
                    buy_i_volume=rec.get('buy_i_volume'), buy_n_volume=rec.get('buy_n_volume'), 
                    sell_i_volume=rec.get('sell_i_volume'), sell_n_volume=rec.get('sell_n_volume'),
                    # وارد کردن تمام ستون‌های Order Book با مقدار None
                    zd1=None, qd1=None, pd1=None, zo1=None, qo1=None, po1=None, 
                    zd2=None, qd2=None, pd2=None, zo2=None, qo2=None, po2=None, 
                    zd3=None, qd3=None, pd3=None, zo3=None, qo3=None, po3=None, 
                    zd4=None, qd4=None, pd4=None, zo4=None, qo4=None, po4=None, 
                    zd5=None, qd5=None, pd5=None, zo5=None, qo5=None, po5=None
                ) for rec in records_dict
            ]
            
            if historical_records:
                current_batch_records.extend(historical_records)
                current_batch_symbol_ids.add(symbol_id)
                updated_symbol_ids.add(symbol_id)
                logger.info(f"💾 {len(historical_records)} رکورد برای {symbol_name} جمع‌آوری شد. (بچ: {len(current_batch_symbol_ids)}/{SYMBOL_BATCH_SIZE})")

                # 7. بررسی برای انجام بچ‌بندی و کامیت
                if len(current_batch_symbol_ids) >= SYMBOL_BATCH_SIZE:
                    inserted_count, success = _commit_historical_batch(
                        db_session, current_batch_records, current_batch_symbol_ids
                    )
                    total_records_inserted += inserted_count
                    
                    # ریست کردن متغیرهای بچ
                    current_batch_records = []
                    current_batch_symbol_ids = set()
                    
            # استراحت کوتاه
            safe_sleep(DEFAULT_PER_SYMBOL_DELAY)
            
        except Exception as e:
            logger.error(f"❌ خطای بحرانی در پردازش داده‌های تاریخی برای نماد {symbol_name}. نماد رد شد. خطا: {e}", exc_info=True)
            safe_sleep(DEFAULT_PER_SYMBOL_DELAY * 2) 
            continue 

    # 8. درج دسته‌ای رکوردهای باقی‌مانده (Last Batch)
    if current_batch_records:
        inserted_count, success = _commit_historical_batch(
            db_session, current_batch_records, current_batch_symbol_ids
        )
        total_records_inserted += inserted_count
        
    logger.info(f"✅ درج داده‌های تاریخی بچ‌بندی شده نهایی شد. مجموع رکوردها: {total_records_inserted}")

    # 9. فراخوانی تحلیل‌های تکنیکال و کندل استیک
    symbol_ids_list = list(updated_symbol_ids)
    
    # اطمینان از پاک بودن Session قبل از فراخوانی توابع خارجی
    db_session.commit()
    
    # الف) اجرای تحلیل تکنیکال
    tech_count, tech_msg = run_technical_analysis(db_session, symbols_list=symbol_ids_list)
    logger.info(f"گزارش تحلیل تکنیکال: {tech_msg}")
    
    # ب) اجرای تشخیص الگوهای شمعی
    candle_count = run_candlestick_detection(db_session, symbols_list=symbol_ids_list)
    logger.info(f"گزارش تشخیص الگوهای شمعی: {candle_count} نماد با الگو.")
        
    message = f"✅ رفرش کامل داده‌های تاریخی و فاندامنتال (Full Historical Refresh) کامل شد. {total_records_inserted} رکورد تاریخی جدید درج شد. {tech_count} نماد تحلیل تکنیکال شدند. {candle_count} نماد الگوی شمعی داشتند."
    logger.info(message)
    
    return total_records_inserted, message