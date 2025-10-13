# services/symbol_initializer.py
import logging
import re
import time
import requests
from bs4 import BeautifulSoup
from datetime import datetime
from typing import List, Dict, Optional

# Flask-SQLAlchemy db (for create_all) and model
from extensions import db
from models import ComprehensiveSymbolData

# از config SessionLocal و engine بیرون می‌کشیم (باید در config ساخته شده باشند)
from config import SessionLocal, engine

logger = logging.getLogger(__name__)

# --- پیکربندی و نقشه‌ها ---
MARKET_TYPE_MAP = {
    '1': 'بورس',
    '2': 'فرابورس',
    '3': 'بورس کالا',
    '4': 'بورس انرژی',
    '5': 'صندوق سرمایه گذاری',
    '6': 'اوراق با درآمد ثابت',
    '7': 'مشتقه',
    '8': 'عمومی',
    '9': 'پایه فرابورس',
    '10': 'اوراق تامین مالی',
    '11': 'اوراق با درآمد ثابت'
}

HTML_MARKET_TYPE_MAP = {
    'بورس اوراق بهادار تهران': 'بورس',
    'فرابورس ایران': 'فرابورس',
    'بورس کالای ایران': 'بورس کالا',
    'بورس انرژی ایران': 'بورس انرژی',
    'صندوق سرمایه‌گذاری': 'صندوق سرمایه گذاری',
    'بازار پایه فرابورس': 'پایه فرابورس',
    'اوراق با درآمد ثابت': 'اوراق با درآمد ثابت',
    'اوراق تامین مالی': 'اوراق تامین مالی'
}

# الگوی فیلتر ساده برای نام‌های نامناسب (قابل گسترش)
BAD_NAME_RE = re.compile(r'\b(حق|وص|ح\W|و\W|ض\W)\b', flags=re.IGNORECASE)


# -------------------------
# کمکی‌های داخلی
# -------------------------
def _ensure_project_table_exists():
    """
    مطمئن می‌شویم جدول مدل ComprehensiveSymbolData در دیتابیس اصلی ایجاد شده باشد.
    از engine که در config ساخته شده (و باید به همان database_url اشاره کند) استفاده می‌کنیم.
    """
    try:
        # استفاده از متد create با checkfirst=True تا اگر موجود بود دوباره نسازد
        ComprehensiveSymbolData.__table__.create(bind=engine, checkfirst=True)
        logger.info("✅ جدول ComprehensiveSymbolData در دیتابیس اصلی بررسی/ایجاد شد")
    except OperationalError as oe:
        logger.error("خطای دسترسی به دیتابیس هنگام ایجاد جدول: %s", oe, exc_info=True)
        raise
    except Exception as e:
        logger.error("خطا در ایجاد/بررسی جدول ComprehensiveSymbolData: %s", e, exc_info=True)
        raise


def _extract_market_type_from_loader_html(html_content: str) -> Optional[str]:
    """
    تلاش می‌کند از HTML صفحه‌ی loader (Loader.aspx) نوع بازار را استخراج کند.
    """
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        for t in soup.stripped_strings:
            for key in HTML_MARKET_TYPE_MAP.keys():
                if key in t:
                    return HTML_MARKET_TYPE_MAP[key]
        return None
    except Exception as e:
        logger.debug("خطا در استخراج نوع بازار از HTML: %s", e)
        return None


# -------------------------
# جمع‌آوری نمادها با pytse_client
# -------------------------
def get_symbols_from_pytse_with_enrichment(max_symbols: Optional[int] = None,
                                          per_symbol_delay: float = 0.01) -> List[Dict]:
    """
    لیستی از نمادها از pytse_client دریافت می‌کند و سعی می‌کند
    market_type و base_volume را از Ticker استخراج کند.
    برمی‌گرداند: لیست دیکشنری‌هایی با کلیدهای symbol_id, symbol_name, market_type, base_volume
    """
    try:
        import pytse_client as tse
    except ImportError:
        logger.error("پکیج pytse_client نصب نیست. نصب کن: pip install pytse-client")
        return []

    logger.info("📥 گرفتن لیست اولیه نمادها از pytse_client...")
    try:
        all_symbols = tse.all_symbols()
    except Exception as e:
        logger.error("خطا در فراخوانی tse.all_symbols(): %s", e, exc_info=True)
        return []

    if not all_symbols:
        logger.warning("لیست نمادهای بازگشتی از pytse_client خالی است.")
        return []

    # اگر dict برگردونده شد از کلیدها استفاده کن، در غیر اینصورت خود لیست
    if isinstance(all_symbols, dict):
        symbol_ids = list(all_symbols.keys())
    else:
        symbol_ids = list(all_symbols)

    logger.info("تعداد نماد یافت‌شده: %d", len(symbol_ids))

    enriched = []
    processed = 0

    for sid in symbol_ids:
        if max_symbols is not None and processed >= max_symbols:
            break
        processed += 1

        symbol_id = sid
        try:
            ticker = None
            # تلاش برای ساخت Ticker با signature های مختلف
            try:
                ticker = tse.Ticker(symbol=symbol_id)
            except TypeError:
                try:
                    ticker = tse.Ticker(symbol_id)
                except Exception:
                    ticker = None
            except Exception:
                ticker = None

            actual_symbol = symbol_id
            actual_title = symbol_id
            market_type_name = 'نامشخص'
            base_volume = 0

            if ticker:
                actual_symbol = getattr(ticker, 'symbol', actual_symbol)
                actual_title = getattr(ticker, 'title', actual_title)

                flow_val = getattr(ticker, 'flow', None)
                if flow_val is not None:
                    market_type_name = MARKET_TYPE_MAP.get(str(flow_val), 'نامشخص')

                base_volume = getattr(ticker, 'base_volume', 0) or 0

                group_name = getattr(ticker, 'group_name', '') or ''
                if market_type_name == 'نامشخص' and group_name:
                    market_type_name = HTML_MARKET_TYPE_MAP.get(group_name.strip(), market_type_name)

                loader_url = getattr(ticker, 'url', '') or ''
                if market_type_name == 'نامشخص' and loader_url and 'Loader.aspx' in loader_url:
                    try:
                        resp = requests.get(loader_url, timeout=6)
                        if resp.ok:
                            html_mt = _extract_market_type_from_loader_html(resp.text)
                            if html_mt:
                                market_type_name = html_mt
                    except Exception:
                        logger.debug("خطا در خواندن loader page برای %s", symbol_id)

            else:
                # fallback: اگر all_symbols dict بود تلاش کن عنوان را بگیریم
                if isinstance(all_symbols, dict):
                    entry = all_symbols.get(symbol_id, {})
                    if isinstance(entry, dict):
                        actual_title = entry.get('title', actual_title) or actual_title
                        base_volume = entry.get('base_volume', base_volume) or base_volume

            enriched.append({
                'symbol_id': actual_symbol,
                'symbol_name': actual_title,
                'market_type': market_type_name,
                'base_volume': int(base_volume) if base_volume is not None else 0
            })

            if per_symbol_delay:
                time.sleep(per_symbol_delay)

        except Exception as e:
            logger.warning("⚠️ خطا در پردازش نماد %s: %s", symbol_id, e)
            continue

    logger.info("✅ جمع‌آوری تکمیلی نمادها پایان یافت. تعداد: %d", len(enriched))
    return enriched


# -------------------------
# درج در دیتابیس (با SessionLocal - هر thread session خودش را دارد)
# -------------------------
def populate_symbols_into_db(max_symbols: Optional[int] = 10,
                             skip_if_exists: bool = True) -> (int, str):
    """
    داده‌های نمادها را از pytse_client خوانده و به جدول ComprehensiveSymbolData در دیتابیس پروژه اضافه می‌کند.
    - max_symbols: حداکثر تعداد پردازش‌شده (برای تست)
    - skip_if_exists: اگر جدول دارای رکورد بود و True باشد، از کار کردن جلوگیری می‌کند.
    بازمی‌گرداند (count, message)
    """

    # اطمینان از اینکه جدول مدل پروژه ساخته شده باشد
    _ensure_project_table_exists()

    # اگر بخواهیم جلوی دوباره‌کاری را بگیریم
    if skip_if_exists:
        try:
            # یک session موقت برای چک استفاده نمی‌کنیم چون ComprehensiveSymbolData.query ممکن است به app context نیاز داشته باشد
            # برای اطمینان از عدم وابستگی به Flask session، از SessionLocal یک session مختص می‌سازیم و count می‌گیریم
            session_check = SessionLocal()
            try:
                row = session_check.execute(text("SELECT 1 FROM comprehensive_symbol_data LIMIT 1")).fetchone()
                if row:
                    logger.warning("⚠️ جدول comprehensive_symbol_data قبلاً داده دارد — لغو عملیات.")
                    return 0, "جدول قبلاً پر شده است."
            except Exception:
                # اگر جدول وجود نداشت، ادامه می‌دهیم (اینجا احتمال خطا کم است چون _ensure پروژه ساخت)
                pass
            finally:
                session_check.close()
        except Exception as e:
            logger.debug("خطا در بررسی وجود رکورد: %s", e)

    logger.info("📥 شروع دریافت نمادها از pytse_client برای درج در دیتابیس پروژه...")
    symbols_data = get_symbols_from_pytse_with_enrichment(max_symbols=max_symbols)

    if not symbols_data:
        logger.warning("هیچ نمادی برای درج دریافت نشد.")
        return 0, "هیچ نمادی دریافت نشد."

    inserted_count = 0
    updated_count = 0
    now = datetime.utcnow()

    session = SessionLocal()
    try:
        for item in symbols_data:
            try:
                symbol_id = str(item.get('symbol_id', '')).strip()
                symbol_name = str(item.get('symbol_name', '')).strip()
                market_type = str(item.get('market_type', '')).strip()
                base_volume = item.get('base_volume', 0)

                if not symbol_id or not symbol_name:
                    continue

                # فیلتر کردن نام‌های نامناسب با regex
                if BAD_NAME_RE.search(symbol_name):
                    logger.debug("رد نماد به خاطر نام نامناسب: %s (%s)", symbol_name, symbol_id)
                    continue

                # تلاش برای یافتن موجودیت
                existing = session.query(ComprehensiveSymbolData).filter_by(symbol_id=symbol_id).first()

                if existing:
                    needs_update = (
                        (existing.symbol_name or '') != symbol_name or
                        (existing.market_type or '') != market_type or
                        (existing.base_volume or 0) != (base_volume or 0)
                    )
                    if needs_update:
                        existing.symbol_name = symbol_name
                        existing.market_type = market_type
                        existing.base_volume = base_volume
                        existing.updated_at = now
                        updated_count += 1
                else:
                    new_symbol = ComprehensiveSymbolData(
                        symbol_id=symbol_id,
                        symbol_name=symbol_name,
                        company_name=None,
                        isin=None,
                        market_type=market_type,
                        flow=None,
                        industry=None,
                        capital=None,
                        legal_shareholder_percentage=None,
                        real_shareholder_percentage=None,
                        float_shares=None,
                        base_volume=base_volume,
                        group_name=None,
                        description=None,
                        last_historical_update_date=None,
                        created_at=now,
                        updated_at=now,
                    )
                    session.add(new_symbol)
                    inserted_count += 1

            except Exception as inner_e:
                logger.warning("خطا در پردازش یک نماد هنگام درج: %s", inner_e)
                continue

        session.commit()
        msg = f"{inserted_count} نماد اضافه و {updated_count} نماد به‌روزرسانی شد"
        logger.info("✅ عملیات درج پایان یافت: %s", msg)
        return inserted_count + updated_count, msg

    except SQLAlchemyError as sa_e:
        session.rollback()
        logger.error("خطای پایگاه‌داده هنگام درج نمادها: %s", sa_e, exc_info=True)
        return 0, f"خطای پایگاه‌داده: {sa_e}"
    except Exception as e:
        session.rollback()
        logger.error("خطا هنگام درج نمادها: %s", e, exc_info=True)
        return 0, f"خطا: {e}"
    finally:
        session.close()

def _find_or_create_app_context():
    """
    تلاش می‌کند یک Flask app context برای اجرای db.create_all() پیدا یا بسازد.
    - اگر یک متغیر app در main یا package تعریف شده باشد از آن استفاده می‌کند.
    - در غیر اینصورت، caller باید app context را فراهم کند.
    """
    try:
        # تلاش برای import مستقیم app از main.py
        from main import app as main_app
        return main_app
    except Exception:
        # اگر main.create_app در دسترس بود می‌توانستیم آن را صدا بزنیم؛ ولی اینجا از آن استفاده نمی‌کنیم تا circular import نشود.
        logger.debug("اپلیکیشن Flask از main وارد نشد؛ فرض می‌کنیم caller app context را فراهم می‌کند.")
        return None


# -------------------------
# توابع کمکی برای دیباگ
# -------------------------
def debug_pytse_import():
    try:
        import pytse_client as tse
        return {
            'success': True,
            'version': getattr(tse, '__version__', 'نامشخص'),
            'all_symbols_exists': hasattr(tse, 'all_symbols'),
            'Ticker_exists': hasattr(tse, 'Ticker')
        }
    except Exception as e:
        return {'success': False, 'error': str(e)}


def test_pytse_functionality(sample_limit: int = 3):
    try:
        import pytse_client as tse
        symbols = tse.all_symbols()
        if not symbols:
            return {'success': True, 'symbols_count': 0}
        sample = []
        count = 0
        iter_symbols = symbols if isinstance(symbols, (list, tuple)) else list(symbols.keys())
        for s in iter_symbols:
            if count >= sample_limit:
                break
            try:
                t = tse.Ticker(symbol=s)
                sample.append({
                    'symbol': getattr(t, 'symbol', None),
                    'title': getattr(t, 'title', None),
                    'flow': getattr(t, 'flow', None),
                    'base_volume': getattr(t, 'base_volume', None),
                    'group_name': getattr(t, 'group_name', None)
                })
            except Exception as ee:
                sample.append({'symbol': s, 'error': str(ee)})
            count += 1
        return {'success': True, 'symbols_count': len(iter_symbols), 'sample': sample}
    except Exception as e:
        return {'success': False, 'error': str(e)}


# -------------------------
# اگر مستقیم اجرا شود
# -------------------------
if __name__ == "__main__":
    # تلاش برای پیدا کردن app و اجرای one_time_init
    app_candidate = None
    try:
        from main import app as main_app
        app_candidate = main_app
    except Exception:
        # اگر اینجا main قابل وارد کردن نیست، باید caller هنگام فراخوانی یک Flask app context ارائه دهد.
        app_candidate = None

    if app_candidate:
        one_time_init(app_obj=app_candidate, max_symbols=5000)
    else:
        logger.error("App Flask پیدا نشد؛ one_time_init اجرا نشد. لطفاً این تابع را داخل app context صدا بزنید.")