# services/historical_data_service.py

from extensions import db
from models import HistoricalData, ComprehensiveSymbolData 
from sqlalchemy import or_, func
from typing import List, Dict, Optional
from sqlalchemy.orm import sessionmaker
import logging
from datetime import date
from flask import current_app 

logger = logging.getLogger(__name__)

# ----------------------------
# Session maker 
# ----------------------------
def get_session_local():
    """
    ایجاد و بازگرداندن یک Session جدید SQLAlchemy.
    این تابع باید بتواند هم در context اپلیکیشن (برای API) و هم خارج از آن (برای CLI) کار کند.
    """
    try:
        with current_app.app_context():
            SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=db.engine)
            return SessionLocal()
    except Exception:
        # Fallback در صورتی که خارج از App Context فراخوانی شود (مانند CLI)
        SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=db.engine)
        return SessionLocal()


def get_historical_data_for_symbol(
    symbol_identifier: str, 
    start_date: Optional[date] = None, 
    end_date: Optional[date] = None, 
    days: int = 61 
) -> Optional[List[Dict]]:
    """
    بازیابی داده‌های تاریخی (HistoricalData) از دیتابیس برای یک نماد مشخص.
    """
    session = get_session_local()
    
    try:
        # ۱. پیدا کردن symbol_id داخلی (PK)
        sym_mapping_id = session.query(ComprehensiveSymbolData.symbol_id).filter(
            or_(
                ComprehensiveSymbolData.tse_index == symbol_identifier,
                ComprehensiveSymbolData.symbol_name == symbol_identifier
            )
        ).scalar()
        
        if not sym_mapping_id:
            logger.warning(f"⚠️ نماد '{symbol_identifier}' در دیتابیس ComprehensiveSymbolData یافت نشد.")
            return []

        # ۲. ساخت کوئری کامل GROUP BY برای تجمیع داده‌ها
        query = session.query(
            HistoricalData.date, 
            HistoricalData.jdate, 
            func.max(HistoricalData.open).label('open'),
            func.max(HistoricalData.high).label('high'),
            func.max(HistoricalData.low).label('low'),
            func.max(HistoricalData.final).label('final'), 
            func.max(HistoricalData.close).label('close'), 
            func.max(HistoricalData.yesterday_price).label('yesterday_price'), 
            
            # Percentages and change values
            func.max(HistoricalData.pcp).label('pcp'), 
            func.max(HistoricalData.plp).label('plp'), 
            func.max(HistoricalData.pcc).label('pcc'), 
            func.max(HistoricalData.plc).label('plc'), 
            
            # Volume and Trades (SUM is critical here)
            func.sum(HistoricalData.volume).label('volume'), 
            func.sum(HistoricalData.value).label('value'),
            func.sum(HistoricalData.num_trades).label('num_trades'),
            
            # Client Type Data (SUM is critical here)
            func.sum(HistoricalData.buy_count_i).label('buy_count_i'),
            func.sum(HistoricalData.buy_count_n).label('buy_count_n'),
            func.sum(HistoricalData.sell_count_i).label('sell_count_i'),
            func.sum(HistoricalData.sell_count_n).label('sell_count_n'),
            func.sum(HistoricalData.buy_i_volume).label('buy_i_volume'),
            func.sum(HistoricalData.buy_n_volume).label('buy_n_volume'),
            func.sum(HistoricalData.sell_i_volume).label('sell_i_volume'),
            func.sum(HistoricalData.sell_n_volume).label('sell_n_volume'),

        ).filter(
            HistoricalData.symbol_id == sym_mapping_id 
        ).group_by(
            HistoricalData.date, 
            HistoricalData.jdate 
        )

        # ۳. اعمال فیلترهای زمانی و مرتب‌سازی
        use_date_range = start_date is not None and end_date is not None
        if use_date_range:
            query = query.filter(HistoricalData.date >= start_date, HistoricalData.date <= end_date)
            query = query.order_by(HistoricalData.date.asc())
        else:
            query = query.order_by(HistoricalData.date.desc()).limit(days)
            
        history_records = query.all()
        
        # اگر محدودیت روزی اعمال شده، داده‌ها را معکوس کن تا از قدیمی به جدید باشد
        if not use_date_range:
            history_records.reverse()
        
        # ۴. تبدیل به فرمت Dict
        result = []
        # به‌روزرسانی لیست ستون‌ها برای مپینگ صحیح
        column_names = [
            'date', 'jdate', 'open', 'high', 'low', 'final', 'close', 
            'yesterday_price', 'pcp', 'plp', 'pcc', 'plc', 
            'volume', 'value', 'num_trades', 
            'buy_count_i', 'buy_count_n', 'sell_count_i', 'sell_count_n', 
            'buy_i_volume', 'buy_n_volume', 'sell_i_volume', 'sell_n_volume', 
        ]
        
        for record in history_records:
            record_dict = dict(zip(column_names, record))
            record_date = record_dict.get('date')
            formatted_date = record_date.isoformat() if record_date else None
            
            result.append({
                "date": formatted_date,
                "jdate": record_dict.get('jdate'),
                "open": record_dict.get('open'),
                "high": record_dict.get('high'),
                "low": record_dict.get('low'),
                
                # 'close' در پاسخ API شامل Final Price است
                "close": record_dict.get('final'), 
                # 'last_price' در پاسخ API شامل Close Price (آخرین قیمت) است
                "last_price": record_dict.get('close'), 
                
                # مقادیر درصد
                "final_change_percent": record_dict.get('pcp'),
                "last_change_percent": record_dict.get('plp'),
                
                # حجم و معاملات
                "volume": record_dict.get('volume'),
                "value": record_dict.get('value'),
                "trades_count": record_dict.get('num_trades'), 
                
                # خریداران
                "buyers_count": record_dict.get('buy_count_i'),
                # توجه: سایر داده‌های حقیقی/حقوقی در record_dict در دسترس هستند.
                # در این نمونه فقط buy_count_i برگشت داده شده است.
            })
            
        return result
        
    except Exception:
        logger.error(f"❌ خطا در بازیابی سابقه معاملات برای {symbol_identifier}", exc_info=True)
        return None 
    finally:
        session.close()