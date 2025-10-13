# cleanup_symbols.py
from extensions import db
from models import ComprehensiveSymbolData
from config import SessionLocal
from sqlalchemy import or_

def get_session_local():
    """ایجاد session local"""
    return SessionLocal()

def cleanup_symbols(delete: bool = True):
    """پاکسازی نمادهای نامعتبر از جدول comprehensive_symbol_data"""
    session = get_session_local()

    try:
        # ترکیب دو شرط: (nav==None و base_volume==1) یا eps منفی
        bad_symbols = session.query(ComprehensiveSymbolData).filter(
            or_(
                (ComprehensiveSymbolData.nav == None) & 
                (ComprehensiveSymbolData.base_volume == 1),
                ComprehensiveSymbolData.eps < 0
            )
        ).all()

        print(f"🔍 {len(bad_symbols)} نماد نامعتبر پیدا شد.")

        if not bad_symbols:
            return

        if delete:
            for sym in bad_symbols:
                print(f"❌ حذف: {sym.symbol_name} (id={sym.id}, eps={sym.eps})")
                session.delete(sym)
            session.commit()
            print("✅ عملیات حذف کامل شد.")
        else:
            for sym in bad_symbols:
                sym.is_valid = False
                print(f"⚠️ علامت‌گذاری نامعتبر: {sym.symbol_name} (id={sym.id}, eps={sym.eps})")
            session.commit()
            print("⚠️ رکوردهای نامعتبر فقط علامت‌گذاری شدند.")

    except Exception as e:
        print(f"خطا: {e}")
        session.rollback()
    finally:
        session.close()


if __name__ == "__main__":
    cleanup_symbols(delete=True)
