# -*- coding: utf-8 -*-
# services/data_fetch_and_process.py
# نسخه بازنویسی شده با توابع نسخه 10 جولای

from extensions import db
from models import HistoricalData, ComprehensiveSymbolData, TechnicalIndicatorData, FundamentalData, CandlestickPatternDetection
from flask import current_app
from sqlalchemy import func, distinct, text
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.exc import SQLAlchemyError, OperationalError
from sqlalchemy.dialects.sqlite import insert
from sqlalchemy import or_ # مطمئن شوید این خط import شده باشد
from datetime import datetime, date, timedelta
import jdatetime
import pandas as pd
import ta
import numpy as np
import requests
from bs4 import BeautifulSoup
import lxml
import time
import gc
import psutil
import logging
import re
from typing import Dict, List, Optional, Tuple, Any, Union
import concurrent.futures
import pytse_client
import traceback

from utils import calculate_stochastic, calculate_squeeze_momentum, calculate_halftrend, calculate_support_resistance_break, check_candlestick_patterns
from sqlalchemy.orm import aliased

import urllib3
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# تنظیمات لاگینگ
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# ----------------------------
# Session maker
# ----------------------------
def get_session_local():
    """ایجاد session local با application context"""
    try:
        from flask import current_app
        with current_app.app_context():
            return sessionmaker(bind=db.engine)()
    except RuntimeError:
        # اگر خارج از application context هستیم
        return sessionmaker(bind=db.get_engine())()

# ----------------------------
# Market type mappings
# ----------------------------
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
    '11': 'اوراق با درآمد ثابت',
    '12': '-'
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

# ----------------------------
# Filter configuration
# ----------------------------
BAD_SUFFIXES = ('ح', 'ض', 'ص', 'و')  # حق تقدم و مشابه

VALID_MARKET_KEYWORDS = list(set(list(MARKET_TYPE_MAP.values()) + list(HTML_MARKET_TYPE_MAP.values())))  # e.g., ['بورس', 'فرابورس', 'بورس کالا', ...]

INVALID_MARKET_KEYWORDS = [
    'اختیار',   # اختیار معامله
    'آتی',      # قرارداد آتی
    'مشتقه',    # بازار مشتقه
    'تسهیلات'   # اوراق تسهیلات مسکن
]

# ----------------------------
# Unified filter
# ----------------------------
def is_symbol_valid(symbol_name: str, market_type_name: str) -> bool:
    """
    بررسی اعتبار نماد (هم برای Ticker و هم برای dict).
    """
    try:
        if not symbol_name:
            return False

        # گام ۱: فیلتر حق تقدم‌ها (بهبود regex: فقط انتهایی یا عبارت کامل)
        if (symbol_name.endswith(BAD_SUFFIXES) and len(symbol_name) > 1) or re.search(r"\b(حق\s*تقدم|ح\.?\s*تقدم)\b", symbol_name, re.IGNORECASE):
            logger.debug(f"فیلتر حق تقدم: {symbol_name}")
            return False

        # گام ۳: بررسی بازار نامعتبر (blacklist - همیشه اعمال کن)
        market_lower = market_type_name.lower() if market_type_name else ''
        if any(keyword.lower() in market_lower for keyword in INVALID_MARKET_KEYWORDS):
            logger.debug(f"فیلتر INVALID: {symbol_name} ({market_type_name})")
            return False

        # گام ۲: بررسی بازار معتبر (whitelist - فقط اگر market_type_name غیرخالی باشه)
        if market_type_name:  # اگر خالی باشه، فرض معتبر
            market_lower = market_type_name.lower()
            if not any(keyword.lower() in market_lower for keyword in VALID_MARKET_KEYWORDS):
                logger.debug(f"فیلتر VALID fail: {symbol_name} ({market_type_name})")  # log برای debug
                return False

        return True

    except Exception as e:
        logger.error(f"خطا در فیلتر نماد {symbol_name}: {e}")
        return False  # safe fallback: فیلتر نکن اگر exception

# ----------------------------
# نسخه dict-based برای استفاده در لیست‌ها
# ----------------------------
def filter_symbols(symbols: List[Dict]) -> List[Dict]:
    """اعمال فیلتر مرکزی روی لیست دیکشنری نمادها"""
    if not symbols:
        return []

    filtered = []
    filtered_count = 0
    for symbol in symbols:
        symbol_name = symbol.get('symbol_name', '')
        market_type_code = symbol.get('market_type_code')
        market_type_name = MARKET_TYPE_MAP.get(market_type_code, '')  # از MAP استفاده کن

        if is_symbol_valid(symbol_name, market_type_name):
            filtered.append(symbol)
            filtered_count += 1

    logger.info(f"فیلتر نمادها: از {len(symbols)}، {filtered_count} معتبر (فیلترشده: {len(symbols) - filtered_count})")
    return filtered


# ----------------------------
# Global defaults and constants
# ----------------------------
# Network
DEFAULT_REQUESTS_TIMEOUT = 8  # seconds
DEFAULT_RETRY_TOTAL = 3
DEFAULT_RETRY_BACKOFF_FACTOR = 0.5
DEFAULT_RETRY_STATUS_FORCELIST = (429, 500, 502, 503, 504)

# Batching / memory
DEFAULT_BATCH_SIZE = 200  # for DB bulk ops & symbol processing
DEFAULT_PER_SYMBOL_DELAY = 0.03  # polite delay between external website hits
MEMORY_CHECK_INTERVAL = 10  # check memory every N symbols during full runs (configurable)
MEMORY_LIMIT_MB = 1500  # warn threshold

# Misc
ZERO_WIDTH_CHARS = ["\u200c", "\u200b", "\ufeff"]  # remove zero width and BOM
HALF_SPACE = "\u200f"  # any specific characters you'd like to normalize

# ----------------------------
# Utilities
# ----------------------------
def normalize_symbol_text(txt: Optional[str]) -> str:
    """Normalize symbol / company names: strip, remove zero-width, collapse spaces."""
    if not txt:
        return ""
    s = str(txt)
    for ch in ZERO_WIDTH_CHARS:
        s = s.replace(ch, "")
    # replace multiple whitespace with single space and strip
    s = " ".join(s.split())
    return s.strip()

def check_memory_usage_mb() -> float:
    """Return current process memory usage in MB (if psutil available)."""
    try:
        if psutil:
            proc = psutil.Process()
            mem = proc.memory_info().rss / (1024 * 1024)
            return mem
        else:
            # fallback: attempt approximate using resource module (Unix only) or 0
            return 0.0
    except Exception as e:
        logger.debug("Memory check failed: %s", e)
        return 0.0

def safe_sleep(seconds: float):
    """Sleep but allow KeyboardInterrupt to bubble up quickly."""
    try:
        time.sleep(seconds)
    except KeyboardInterrupt:
        raise

# ----------------------------
# Retry decorator (general)
# ----------------------------
def retry_on_exception(
    exceptions: Tuple[type, ...] = (Exception,),
    tries: int = 3,
    delay: float = 0.5,
    backoff: float = 2.0,
    logger_obj: Optional[logging.Logger] = None,
):
    """
    Generic retry decorator with exponential backoff.
    Usage:
        @retry_on_exception((requests.RequestException,), tries=3)
        def fn(...): ...
    """
    def deco_retry(func):
        def wrapper(*args, **kwargs):
            _tries, _delay = tries, delay
            while _tries > 1:
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    msg = f"Retryable error in {func.__name__}: {e}. Retrying in {_delay}s..."
                    if logger_obj:
                        logger_obj.warning(msg)
                    else:
                        logger.warning(msg)
                    time.sleep(_delay)
                    _tries -= 1
                    _delay *= backoff
            # final attempt
            return func(*args, **kwargs)
        wrapper.__name__ = func.__name__
        return wrapper
    return deco_retry

# ----------------------------
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ایجاد session برای requests
def create_requests_session():
    """Create a requests session with retry logic"""
    session = requests.Session()
    
    # Retry configuration
    retry_strategy = Retry(
        total=3,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
    )
    
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    
    # Set default headers
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Connection': 'keep-alive',
    })
    
    return session

_requests_session = create_requests_session()

# ----------------------------
# Wrapper: pytse-client safe adapter (primary)
# ----------------------------

try:
    import pytse_client as tse
    from pytse_client import Ticker, download, config
except ImportError:
    tse = None
    Ticker = None
    download = None
    config = None
    logger.warning("pytse_client not available")

class PytseClientWrapper:
    """
    Safe wrapper around pytse_client (primary source).
    Provides:
      - all_symbols()
      - ticker(symbol)
      - fetch_historical(symbol_id, start_date, end_date)
    Behavior:
      - If pytse-client is not importable or throws errors, this wrapper raises its own exceptions
        so caller can fallback to InternalTseWrapper.
    """
    def __init__(self, per_symbol_delay: float = DEFAULT_PER_SYMBOL_DELAY):
        self.per_symbol_delay = per_symbol_delay
        self.tse = tse  # may be None
        if self.tse is None:
            logger.info("pytse_client not available at import time.")
        # local small cache to reduce repeated ticker creations in same run
        self._ticker_cache: Dict[str, Any] = {}
        # requests session reuse for any direct HTTP calls if needed
        self.session = _requests_session

    def is_available(self):
        """بررسی آیا pytse_client در دسترس است"""
        return self.tse is not None

    def available(self) -> bool:
        """متد جایگزین برای سازگاری"""
        return self.is_available()

    @retry_on_exception((Exception,), tries=2, delay=0.5, backoff=2.0, logger_obj=logger)
    def all_symbols(self) -> List[str]:
        """Return list of symbol ids (or keys) from pytse-client"""
        if not self.is_available():
            raise RuntimeError("pytse_client not available")
        try:
            result = self.tse.all_symbols()
            # some versions return dict of id->meta
            if isinstance(result, dict):
                return list(result.keys())
            if isinstance(result, (list, tuple, set)):
                return list(result)
            # fallback: try to coerce
            return [str(x) for x in result]
        except Exception as e:
            logger.exception("pytse_client.all_symbols() failed: %s", e)
            raise

    def get_all_symbols(self):
        """دریافت تمامی نمادها - متد سازگار با کد موجود"""
        if not self.is_available():
            return {}
        try:
            tickers = download(symbols="all", write_to_csv=False)
            return tickers
        except Exception as e:
            logger.error(f"Error getting all symbols: {e}")
            return {}

    def ticker(self, symbol_id: str):
        """Return Ticker object from pytse_client, cached"""
        if not self.is_available():
            raise RuntimeError("pytse_client not available")
        key = str(symbol_id)
        if key in self._ticker_cache:
            return self._ticker_cache[key]
        try:
            ticker = Ticker(symbol=key)
            self._ticker_cache[key] = ticker
            # polite delay
            if self.per_symbol_delay:
                safe_sleep(self.per_symbol_delay)
            return ticker
        except Exception:
            logger.exception("Failed to get Ticker for %s", symbol_id)
            raise

    def get_ticker(self, symbol_name):
        """ایجاد Ticker object با مدیریت خطا - نسخه سازگار با کد موجود"""
        if not self.is_available():
            return None
        try:
            ticker = Ticker(symbol_name)
            return ticker
        except Exception as e:
            logger.error(f"Error creating Ticker for {symbol_name}: {e}")
            return None

    @retry_on_exception((Exception,), tries=2, delay=1.0, backoff=2.0, logger_obj=logger)
    def fetch_historical(self, symbol_id: str, start_date: Optional[datetime] = None, end_date: Optional[datetime] = None) -> Any:
        """
        Fetch historical data for symbol using pytse-client high-level APIs if available.
        Return types may vary by pytse-client version: pandas.DataFrame OR dict OR text.
        Caller should handle/normalize.
        """
        if not self.is_available():
            raise RuntimeError("pytse_client not available")

        try:
            # many pytse-client versions expose a function to download history. We try common patterns.
            # 1) tse.download... or tse.get_historical or Ticker().history
            if hasattr(self.tse, "download") or hasattr(self.tse, "history"):
                # attempt Ticker.history first if available
                try:
                    t = self.ticker(symbol_id)
                    if hasattr(t, "history"):
                        hist = getattr(t, "history")()
                        return hist
                except Exception:
                    pass

            # Fallback generic APIs (some versions implement get_history or download_from)
            if hasattr(self.tse, "get_history"):
                return self.tse.get_history(symbol_id, start_date=start_date, end_date=end_date)
            if hasattr(self.tse, "download"):
                # typical signature: tse.download(symbol_id, start, end)
                try:
                    return self.tse.download(symbol_id, start=start_date, end=end_date)
                except TypeError:
                    # try alternative signature
                    return self.tse.download(symbol_id)
            # Last resort: try calling tse.Ticker and reading attributes or methods
            t = self.ticker(symbol_id)
            if hasattr(t, "history"):
                return t.history()
            # If nothing works, raise
            raise RuntimeError("No supported historical API in installed pytse_client")
        except Exception as e:
            logger.exception("pytse-client historical fetch failed for %s: %s", symbol_id, e)
            raise

# ----------------------------
# Internal fallback wrapper (if pytse-client fails)
# ----------------------------

class InternalTseWrapper:
    """
    Fallback methods that use TSETMC endpoints directly (HTTP scraping/JSON endpoints).
    This is slower and more brittle, but necessary as a fallback.
    Key functions:
      - search_symbol_by_name(name)
      - fetch_instrument_info_by_id(instrument_id)
      - fetch_export_txt(...)
      - parse loader page to infer market/group etc.
    NOTE: Keep polite delays and caching here to avoid hammering TSETMC.
    """
    def __init__(self, per_call_delay: float = DEFAULT_PER_SYMBOL_DELAY):
        self.session = _requests_session
        self.per_call_delay = per_call_delay
        self._simple_cache: Dict[str, Any] = {}

    def _get(self, url: str, params: Optional[Dict[str, Any]] = None, timeout: Optional[int] = None) -> requests.Response:
        try:
            resp = self.session.get(url, params=params, timeout=timeout or DEFAULT_REQUESTS_TIMEOUT)
            return resp
        except Exception:
            logger.exception("HTTP GET failed for %s", url)
            raise

    def search_symbol_by_name(self, query: str) -> List[Dict[str, Any]]:
        """
        Use old.tsetmc.com search endpoint to find instrument IDs by textual name.
        Returns a list of dicts (best-effort) with keys like 'id', 'symbol', 'title'
        """
        q = normalize_symbol_text(query)
        cache_key = f"search:{q}"
        if cache_key in self._simple_cache:
            return self._simple_cache[cache_key]

        try:
            url = "https://old.tsetmc.com/tsev2/data/search.aspx"
            params = {"skey": q}
            resp = self._get(url, params=params)
            if resp.status_code != 200:
                logger.debug("search endpoint returned status %s for %s", resp.status_code, q)
                return []
            text = resp.text or ""
            # The search endpoint often returns a small text: small html or some csv-like
            # try to parse ids from text heuristically
            results = []
            # simple heuristics: split lines and find parenthesis with id or pattern 'i=...'
            for line in text.splitlines():
                line = line.strip()
                if not line:
                    continue
                # example pattern: "some name (symbol) - <a href='...i=1234567'>"
                # fallback: look for sequences of digits length>6
                import re
                m = re.search(r"i=(\d{6,})", line)
                if m:
                    iid = m.group(1)
                    results.append({"id": iid, "raw": line})
            # fallback more: if we didn't parse anything, try to extract trailing tokens
            if not results:
                # try whitespace tokenization for any long numeric tokens
                import re
                for token in re.findall(r"\d{6,}", text):
                    results.append({"id": token, "raw": token})
            self._simple_cache[cache_key] = results
            safe_sleep(self.per_call_delay)
            return results
        except Exception:
            logger.exception("search_symbol_by_name failed for query: %s", query)
            return []

    def fetch_export_txt(self, instrument_id: str) -> Optional[str]:
        """
        Call Export-txt.aspx to get instrument details text
        """
        try:
            url = f"https://old.tsetmc.com/tsev2/data/Export-txt.aspx"
            params = {"t": "i", "a": 1, "b": 0, "i": instrument_id}
            resp = self._get(url, params=params)
            if resp.status_code != 200:
                logger.debug("export-txt returned %s for %s", resp.status_code, instrument_id)
                return None
            safe_sleep(self.per_call_delay)
            return resp.text
        except Exception:
            logger.exception("fetch_export_txt failed for %s", instrument_id)
            return None

    def fetch_instrument_info_api(self, instrument_id: str) -> Optional[Dict[str, Any]]:
        """Call cdn.tsetmc.com/api/Instrument/GetInstrumentInfo/{id}"""
        try:
            url = f"https://cdn.tsetmc.com/api/Instrument/GetInstrumentInfo/{instrument_id}"
            resp = self._get(url)
            if resp.status_code != 200:
                return None
            data = resp.json()
            if not data or not isinstance(data, dict):
                return None
            # extract useful fields
            result = {}
            # example structure: {"instrumentInfo": { ... }, ... }
            # we try to extract common fields
            if "instrumentInfo" in data:
                info = data["instrumentInfo"]
                if isinstance(info, dict):
                    result["name"] = info.get("lVal18AFC")
                    result["symbol"] = info.get("lVal18")
                    result["company_name"] = info.get("lVal30")
                    result["market"] = info.get("market")
                    result["industry"] = info.get("sector")
            safe_sleep(self.per_call_delay)
            return result
        except Exception:
            logger.exception("instrument info API failed for %s", instrument_id)
            return None

    def fetch_loader_page(self, instrument_id: str) -> Optional[str]:
        """Fetch loader.aspx page to parse market/group info from HTML"""
        try:
            url = f"https://old.tsetmc.com/Loader.aspx"
            params = {"i": instrument_id, "Partree": "15131M"}
            resp = self._get(url, params=params)
            if resp.status_code != 200:
                return None
            safe_sleep(self.per_call_delay)
            return resp.text
        except Exception:
            logger.exception("loader page fetch failed for %s", instrument_id)
            return None

    def parse_loader_page(self, html: str) -> Dict[str, Any]:
        """Parse loader.aspx HTML to extract market/group info"""
        result = {}
        if not html:
            return result
        try:
            soup = BeautifulSoup(html, "lxml")
            # look for breadcrumbs or specific table rows
            # example: <a href="...">بازار اول</a> or <td>بازار</td><td>بازار اول</td>
            # try to find common patterns
            # pattern 1: breadcrumb links
            links = soup.find_all("a")
            for link in links:
                href = link.get("href", "")
                text = normalize_symbol_text(link.get_text())
                if "Partree" in href and text:
                    # example href: "?Partree=15131P&i=1234567"
                    # text might be like "بازار اول", "بازار دوم", etc.
                    result["market"] = text
            # pattern 2: table rows with key-value
            rows = soup.find_all("tr")
            for row in rows:
                tds = row.find_all("td")
                if len(tds) >= 2:
                    key = normalize_symbol_text(tds[0].get_text())
                    value = normalize_symbol_text(tds[1].get_text())
                    if key and value:
                        # common keys: "بازار", "گروه صنعت", "نماد", "نام شرکت"
                        if "بازار" in key:
                            result["market"] = value
                        elif "گروه" in key:
                            result["industry"] = value
                        elif "نام شرکت" in key:
                            result["company_name"] = value
                        elif "نماد" in key:
                            result["symbol"] = value
            return result
        except Exception:
            logger.exception("parse_loader_page failed")
            return result

    def get_instrument_details(self, instrument_id: str) -> Dict[str, Any]:
        """Combined method to get details for an instrument ID using multiple fallbacks"""
        result = {}
        # try API first
        api_info = self.fetch_instrument_info_api(instrument_id)
        if api_info:
            result.update(api_info)
        # then try loader page
        html = self.fetch_loader_page(instrument_id)
        if html:
            parsed = self.parse_loader_page(html)
            result.update(parsed)
        # then try export-txt
        txt = self.fetch_export_txt(instrument_id)
        if txt:
            # simple parsing: split lines and look for patterns
            lines = txt.splitlines()
            for line in lines:
                line = line.strip()
                if ":" in line:
                    parts = line.split(":", 1)
                    key = parts[0].strip()
                    value = parts[1].strip() if len(parts) > 1 else ""
                    # map known keys
                    if key == "نماد":
                        result["symbol"] = value
                    elif key == "نام شرکت":
                        result["company_name"] = value
                    elif key == "بازار":
                        result["market"] = value
                    elif key == "گروه صنعت":
                        result["industry"] = value
        return result

# ----------------------------
# Global instances
# ----------------------------
pytse_wrapper = PytseClientWrapper()
internal_wrapper = InternalTseWrapper()


# ----------------------------
# توابع کمکی
# ----------------------------
def setup_robust_session(retries=5, backoff_factor=2, timeout=60):
    """تنظیم سشن با Retry و Timeout بالا"""
    # افزایش تعداد تلاش مجدد و ضریب تاخیر برای شبکه ناپایدار
    retry_strategy = Retry(
        total=retries,
        backoff_factor=backoff_factor, # تاخیر: 2s, 4s, 8s, 16s...
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "OPTIONS"]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    http = requests.Session()
    http.mount("https://", adapter)
    http.mount("http://", adapter)
    # تنظیم زمان‌بندی پیش‌فرض برای سشن (اختیاری، اما توصیه می‌شود)
    http.timeout = timeout
    return http

# ----------------------------
# تابع لیست کردن سهم‌ها (populate_comprehensive_symbols)
# ----------------------------
def populate_comprehensive_symbols(db_session, batch_size: int = 200):
    """
    دریافت لیست نمادها از pytse-client و درج/به‌روزرسانی در ComprehensiveSymbolData به صورت دسته‌ای.
    این تابع اکنون Retry را برای فاز دریافت اولیه و فاز تیکرینگ (Ticker) اعمال می‌کند.
    """
    
    logger.info(f"📥 شروع دریافت و به‌روزرسانی لیست نمادها در ComprehensiveSymbolData با بچ‌سایز {batch_size}...")

    try:
        import pytse_client as tse
        
        # 1. فاز اولیه: دریافت لیست تمام نمادها (با Retry در سطح بالا)
        
        # 💡 ایجاد سشن قوی برای فراخوانی اولیه download (این سشن در اینجا مستقیماً استفاده نمی‌شود اما الگوی خوبی است)
        # robust_http_session = setup_robust_session(retries=5, backoff_factor=2, timeout=120) 
        
        all_tickers = None
        # تلاش برای 3 بار دریافت لیست کل نمادها
        for attempt in range(1, 4):
            try:
                logger.info(f"🚀 تلاش {attempt} برای دریافت لیست کل نمادها از TSETMC...")
                
                # ❌ پارامتر 'timeout' حذف شد، زیرا pytse_client از آن پشتیبانی نمی‌کند.
                all_tickers = tse.download(symbols="all", write_to_csv=False) 
                break # اگر موفق شد، حلقه را می‌شکنیم
            except (requests.exceptions.Timeout, urllib3.exceptions.ReadTimeoutError, TypeError, Exception) as e:
                # 💡 TypeError را به لیست استثناها اضافه کردیم تا در صورتی که دوباره خطای پارامتر داد، لاگ شود.
                if isinstance(e, TypeError) and "unexpected keyword argument 'timeout'" in str(e):
                    logger.error("❌ خطای پارامتر: تابع tse.download از 'timeout' پشتیبانی نمی‌کند. به تلاش‌های مجدد تکیه می‌کنیم.")
                    # در این مرحله، پارامتر 'timeout' که باعث خطا شده حذف شده است. 
                    # اگر این خطا مجدداً رخ دهد (که نباید بدهد)، شاید مشکل دیگری باشد.
                else:
                    logger.warning(f"⚠️ خطای زمان‌بندی یا شبکه در تلاش {attempt} برای دریافت لیست کل نمادها: {e}")
                    
                if attempt == 3:
                    raise Exception(f"❌ خطای بحرانی در دریافت لیست کل نمادها پس از ۳ تلاش: {e}") from e
                time.sleep(10 * attempt) # تاخیر تصاعدی قبل از تلاش مجدد

        if not all_tickers:
             return {"added": 0, "updated": 0, "message": "لیست نمادها دریافت نشد."}


        # 2. آماده‌سازی لیست نمادها برای بچ‌بندی
        all_symbol_names = list(all_tickers.keys())
        total_symbols = len(all_symbol_names)
        logger.info(f"✅ لیست اولیه نمادها دریافت شد. مجموع نمادها: {total_symbols}")
        
        added_count = 0
        updated_count = 0
        now = datetime.now()
        
        # 3. حلقه اصلی برای پردازش دسته‌ای
        for i in range(0, total_symbols, batch_size):
            batch_symbol_names = all_symbol_names[i:i + batch_size]
            logger.info(f"--- شروع پردازش دسته {i//batch_size + 1} از {len(batch_symbol_names)} نماد ---")
            
            # ذخیره رکوردها به صورت موقت در هر بچ
            batch_records_to_commit = []

            for symbol_name in batch_symbol_names:
                
                # 3.1. فیلتر اولیه (بر اساس flow که در tse.download آمده است)
                ticker_data = all_tickers.get(symbol_name, {})
                flow = ticker_data.get('flow', '')
                # ⚠️ فرض بر این است که is_symbol_valid در دسترس است
                if not is_symbol_valid(symbol_name, flow):
                    logger.debug(f"⏩ نماد {symbol_name} فیلتر شد (flow: {flow})")
                    continue

                # 3.2. فاز جزئیات: دریافت اطلاعات کامل Ticker (با Retry در سطح نماد)
                ticker = None
                for retry in range(1, 4):
                    try:
                        ticker = tse.Ticker(symbol_name)
                        break
                    except Exception as e:
                        logger.warning(f"⚠️ خطای دریافت جزئیات نماد {symbol_name} (تلاش {retry}): {e}")
                        if retry == 3:
                            logger.error(f"❌ نماد {symbol_name} پس از ۳ تلاش Skip شد.")
                            ticker = None
                            break
                        time.sleep(5 * retry) # تاخیر قبل از تلاش مجدد
                
                if ticker is None:
                    continue # اگر دریافت جزئیات شکست خورد، به نماد بعدی برو

                # 3.3. پردازش و ذخیره‌سازی داده
                try:
                    
                    if not is_symbol_valid(symbol_name, getattr(ticker, 'flow', '')):
                        continue
                    
                    tse_index = getattr(ticker, 'index', None)
                    if not tse_index:
                        logger.warning(f"⚠️ نماد {symbol_name} شناسه بورس ندارد و نادیده گرفته شد.")
                        continue
                    
                    base_volume = getattr(ticker, 'base_volume', None)
                    eps = getattr(ticker, 'eps', None)
                    p_e_ratio = getattr(ticker, 'p_e_ratio', None)
                    market_cap = getattr(ticker, 'market_cap', None)
                    float_shares = getattr(ticker, 'float_shares', None)
                    fiscal_year = getattr(ticker, 'fiscal_year', None)
                    state = getattr(ticker, 'state', None)
                    p_s_ratio = getattr(ticker, 'p_s_ratio', None)
                    nav = getattr(ticker, 'nav', None)

                    if nav and isinstance(nav, str):
                        try:
                            nav = float(nav.replace(',', ''))
                        except ValueError:
                            nav = None

                    if nav is None and base_volume == 1:
                        logger.debug(f"⏩ نماد {symbol_name} به دلیل nav=None و base_volume=1 رد شد.")
                        continue

                    # ⚠️ فرض بر این است که ComprehensiveSymbolData در دسترس است
                    existing_symbol = db_session.query(ComprehensiveSymbolData).filter_by(
                        tse_index=tse_index
                    ).first()
                    
                    if existing_symbol:
                        # به‌روزرسانی نماد موجود
                        existing_symbol.symbol_name = symbol_name
                        existing_symbol.company_name = getattr(ticker, 'title', '')
                        existing_symbol.group_name = getattr(ticker, 'group_name', '')
                        existing_symbol.market_type = getattr(ticker, 'flow', '')
                        existing_symbol.base_volume = base_volume
                        existing_symbol.eps = eps
                        existing_symbol.p_e_ratio = p_e_ratio
                        existing_symbol.market_cap = market_cap
                        existing_symbol.float_shares = float_shares
                        existing_symbol.fiscal_year = fiscal_year
                        existing_symbol.state = state
                        existing_symbol.tse_index = tse_index
                        if p_s_ratio is not None:
                            existing_symbol.p_s_ratio = p_s_ratio
                        if nav is not None:
                            existing_symbol.nav = nav
                        existing_symbol.updated_at = now
                        updated_count += 1
                        batch_records_to_commit.append(existing_symbol)
                        
                    else:
                        # ایجاد نماد جدید
                        new_symbol = ComprehensiveSymbolData(
                            symbol_id=tse_index,
                            tse_index=tse_index,
                            symbol_name=symbol_name,
                            company_name=getattr(ticker, 'title', ''),
                            group_name=getattr(ticker, 'group_name', ''),
                            market_type=getattr(ticker, 'flow', ''),
                            base_volume=base_volume,
                            eps=eps,
                            p_e_ratio=p_e_ratio,
                            market_cap=market_cap,
                            float_shares=float_shares,
                            fiscal_year=fiscal_year,
                            state=state,
                            created_at=now,
                            updated_at=now
                        )
                        if p_s_ratio is not None:
                            new_symbol.p_s_ratio = p_s_ratio
                        if nav is not None:
                            new_symbol.nav = nav
                        
                        added_count += 1
                        batch_records_to_commit.append(new_symbol)
                        
                except Exception as e:
                    logger.error(f"❌ خطا در پردازش رکورد نماد {symbol_name}: {e}", exc_info=True)
                    # در اینجا rollback نمی‌کنیم تا فقط این نماد Skip شود و بچ ادامه یابد.
                    continue
            
            # 4. Commit دسته‌ای در پایان هر بچ
            try:
                for record in batch_records_to_commit:
                    db_session.add(record)
                db_session.commit()
                logger.info(f"✅ ثبت دسته {i//batch_size + 1} با موفقیت در دیتابیس.")
                time.sleep(3) # مکث کوتاه بین بچ‌ها
            except Exception as e:
                logger.error(f"❌ خطای ثبت دسته {i//batch_size + 1} در دیتابیس: {e}", exc_info=True)
                db_session.rollback()
                # اگر Commit شکست خورد، کل عملیات را متوقف می‌کنیم.
                raise Exception(f"خطا در ثبت اطلاعات در دیتابیس در دسته {i//batch_size + 1}.") from e

        # 5. جمع‌بندی نهایی
        logger.info(f"✅ دریافت و ذخیره نمادها کامل شد. {added_count} نماد جدید اضافه شد، {updated_count} نماد به‌روزرسانی شد.")
        return {"added": added_count, "updated": updated_count}
        
    except Exception as e:
        logger.error(f"❌ خطای کلی در populate_comprehensive_symbols: {e}", exc_info=True)
        db_session.rollback()
        # اگر خطای زمان‌بندی رخ داد، پیام مناسب‌تری برگردانید.
        error_msg = str(e)
        if "Read timed out" in error_msg or "timeout" in error_msg.lower():
            # این خطا به دلیل عدم پشتیبانی از پارامتر timeout ایجاد شده بود، اما پیام مناسب را حفظ می‌کنیم.
            error_msg = "خطای زمان‌بندی (Timeout) در ارتباط با سرور بورس رخ داد. لطفاً دوباره تلاش کنید."
        raise Exception(error_msg) from e




# ----------------------------
#  تابع دریافت داده‌های تاریخی و بنیادی (fetch_historical_and_fundamental_data)
# ----------------------------
def fetch_historical_and_fundamental_data(db_session: Session, batch_size: int = 200, limit: int = None):
    """
    دریافت و به‌روزرسانی داده‌های تاریخی و بنیادی برای نمادهای موجود.

    این تابع نمادها را به صورت دسته‌ای (Batch) از دیتابیس واکشی می‌کند و سپس
    داده‌های تاریخی و بنیادی هر دسته را به‌روزرسانی می‌کند تا از بارگذاری زیاد
    حافظه و خطاهای Timeout جلوگیری شود.
    
    Args:
        db_session: سشن SQLAlchemy برای کار با دیتابیس.
        batch_size (int): تعداد نمادها در هر دسته برای پردازش (پیش‌فرض 200).
        limit (int | None): حداکثر تعداد کلی نمادهایی که باید پردازش شوند.
    """
    try:
        logger.info(f"🔄 شروع فرآیند دریافت داده‌های تاریخی و بنیادی با بچ‌سایز {batch_size}...")

        # ⚠️ فرض بر این است که ComprehensiveSymbolData، fetch_and_process_historical_data، 
        # update_symbol_fundamental_data و logger در دسترس هستند.
        
        # 1. آماده‌سازی متغیرهای حلقه
        offset = 0
        total_processed_count = 0
        total_historical_count = 0
        total_fundamental_count = 0
        
        # شمارش کلی نمادها (فقط برای لاگ اولیه)
        symbols_count_query = db_session.query(ComprehensiveSymbolData).count()
        max_symbols_to_process = limit if limit is not None else symbols_count_query
        
        logger.info(f"🔍 مجموع نمادهای موجود در دیتابیس: {symbols_count_query}. حداکثر تعداد برای پردازش: {max_symbols_to_process}")

        # 2. حلقه اصلی پردازش دسته‌ای
        while total_processed_count < max_symbols_to_process:
            
            # تعیین limit برای کوئری فعلی
            current_limit = min(batch_size, max_symbols_to_process - total_processed_count)

            # کوئری گرفتن id و tse_index (با اعمال offset و limit)
            symbols_query = db_session.query(
                ComprehensiveSymbolData.id,
                ComprehensiveSymbolData.tse_index
            ).order_by(ComprehensiveSymbolData.id).offset(offset).limit(current_limit)
            
            symbols_batch = symbols_query.all()
            
            if not symbols_batch:
                # اگر هیچ نماد دیگری برای واکشی نبود، حلقه را می‌شکنیم
                break

            current_batch_size = len(symbols_batch)
            # استخراج tse_index‌ها (که کلید اصلی برای فراخوانی Ticker است)
            batch_tse_indices = [tse_index for _, tse_index in symbols_batch]

            logger.info(f"--- شروع پردازش دسته {offset // batch_size + 1} از {current_batch_size} نماد (Offset: {offset}) ---")

            # 2.1. آپدیت داده‌های تاریخی برای دسته فعلی
            try:
                # فراخوانی تابع داخلی با لیست نمادهای بچ فعلی
                processed_historical_count, historical_msg = fetch_and_process_historical_data(
                    db_session, 
                    specific_symbols_list=batch_tse_indices # ارسال لیست نمادهای بچ فعلی
                )
                total_historical_count += processed_historical_count
                logger.info(f"📈 [بچ] داده تاریخی برای {len(symbols_batch)} نماد مورد بررسی قرار گرفت.")
                
            except Exception as e:
                logger.error(f"❌ خطای بحرانی در آپدیت تاریخی دسته {offset // batch_size + 1}: {e}", exc_info=True)
            
            # 2.2. آپدیت داده‌های بنیادی برای دسته فعلی
            batch_fundamental_count = 0
            try:
                # فراخوانی تابع داخلی با لیست نمادهای بچ فعلی
                updated_count, msg = update_symbol_fundamental_data(
                    db_session,
                    specific_symbols_list=batch_tse_indices
                )
                batch_fundamental_count += updated_count
                total_fundamental_count += batch_fundamental_count
                logger.info(f"📊 [بچ] داده بنیادی برای {batch_fundamental_count} نماد به‌روز شد.")
                
            except Exception as e:
                db_session.rollback()
                logger.error(f"❌ خطای بحرانی در آپدیت بنیادی دسته {offset // batch_size + 1}: {e}", exc_info=True)
                
            
            # 2.3. به‌روزرسانی Offset و شمارنده‌ها
            total_processed_count += current_batch_size
            offset += current_batch_size

            if offset >= max_symbols_to_process and limit is not None:
                break # به limit مورد نظر رسیدیم

        # 3. جمع‌بندی نهایی (پس از اتمام حلقه)
        success_msg = f"""
✅ فرآیند دریافت داده‌های تاریخی و بنیادی کامل شد:
• {total_historical_count} رکورد جدید تاریخی ثبت شد.
• {total_fundamental_count} نماد داده‌های بنیادی دریافت کردند
        """
        logger.info(success_msg)
        
        return {
            "historical": total_historical_count,
            "fundamental": total_fundamental_count,
            "message": success_msg
        }

    except Exception as e:
        error_msg = f"❌ خطا در فرآیند دریافت داده‌های تاریخی و بنیادی: {e}"
        logger.error(error_msg, exc_info=True)
        # فقط در صورتی Rollback می‌کنیم که خطای کلی داشته باشیم (زیرا خطاهای بچ قبلاً Rollback شده‌اند)
        if db_session.is_active:
            db_session.rollback()
            
        return {
            "historical": 0,
            "fundamental": 0,
            "message": error_msg
        }




# ----------------------------
# تابع fetch_symbols_from_pytse_client (نسخه اصلاح‌شده با فیلتر مرکزی)
# ----------------------------
def fetch_symbols_from_pytse_client(db_session: Session, limit: int = None):
    """
    گرفتن لیست نمادها از pytse-client و درج در ComprehensiveSymbolData.
    """
    logger.info("📥 شروع دریافت نمادها از pytse_client برای درج در comprehensive_symbol_data...")

    try:
        import pytse_client as tse
        all_tickers = tse.download(symbols="all", write_to_csv=False)
        
        added_count = 0
        updated_count = 0
        now = datetime.now()

        for idx, (symbol_name, ticker_data) in enumerate(all_tickers.items()):
            if limit and idx >= limit:
                break

            # فیلتر زودتر (قبل از Ticker creation)
            # flow رو از ticker_data بگیر اگر ممکنه، وگرنه ''
            flow = ticker_data.get('flow', '') if isinstance(ticker_data, dict) else ''
            if not is_symbol_valid(symbol_name, flow):
                logger.debug(f"⏩ نماد {symbol_name} فیلتر شد (flow: {flow})")
                continue  # زودتر skip کن

            try:
                # ایجاد شی Ticker برای دسترسی به اطلاعات کامل
                ticker = tse.Ticker(symbol_name)
                
                # 💡 استفاده از فیلتر مرکزی (نسخه جدید)
                if not is_symbol_valid(symbol_name, getattr(ticker, 'flow', '')):
                    continue  # اگر نماد معتبر نیست، برو به بعدی
                
                # دریافت شناسه منحصر به فرد (index) از Tsetmc
                tse_index = getattr(ticker, 'index', None)
                if not tse_index:
                    logger.warning(f"⚠️ نماد {symbol_name} شناسه بورس ندارد و نادیده گرفته شد.")
                    continue
                
                # ... بقیه کد بدون تغییر باقی می‌ماند ...
                base_volume = getattr(ticker, 'base_volume', None)
                eps = getattr(ticker, 'eps', None)
                p_e_ratio = getattr(ticker, 'p_e_ratio', None)
                market_cap = getattr(ticker, 'market_cap', None)
                float_shares = getattr(ticker, 'float_shares', None)
                fiscal_year = getattr(ticker, 'fiscal_year', None)
                state = getattr(ticker, 'state', None)
                p_s_ratio = getattr(ticker, 'p_s_ratio', None)
                nav = getattr(ticker, 'nav', None)
                
                if nav and isinstance(nav, str):
                    try:
                        nav = float(nav.replace(',', ''))
                    except ValueError:
                        nav = None

                # 🚨 جلوگیری از ورود رکوردهای ناقص
                if nav is None and base_volume == 1:
                    logger.debug(f"⏩ نماد {symbol_name} به دلیل nav=None و base_volume=1 رد شد.")
                    continue

                # بررسی وجود نماد در دیتابیس با tse_index
                existing_symbol = db_session.query(ComprehensiveSymbolData).filter_by(
                    tse_index=tse_index
                ).first()
                
                if existing_symbol:
                    # به‌روزرسانی نماد موجود
                    existing_symbol.symbol_name = symbol_name
                    existing_symbol.company_name = getattr(ticker, 'title', '')
                    existing_symbol.group_name = getattr(ticker, 'group_name', '')
                    existing_symbol.market_type = getattr(ticker, 'flow', '')
                    existing_symbol.base_volume = base_volume
                    existing_symbol.eps = eps
                    existing_symbol.p_e_ratio = p_e_ratio
                    existing_symbol.market_cap = market_cap
                    existing_symbol.float_shares = float_shares
                    existing_symbol.fiscal_year = fiscal_year
                    existing_symbol.state = state
                    existing_symbol.tse_index = tse_index
                    
                    if p_s_ratio is not None:
                        existing_symbol.p_s_ratio = p_s_ratio
                    if nav is not None:
                        existing_symbol.nav = nav
                    
                    existing_symbol.updated_at = now
                    updated_count += 1
                    
                else:
                    # ایجاد نماد جدید
                    new_symbol = ComprehensiveSymbolData(
                        symbol_id=tse_index,
                        tse_index=tse_index,
                        symbol_name=symbol_name,
                        company_name=getattr(ticker, 'title', ''),
                        group_name=getattr(ticker, 'group_name', ''),
                        market_type=getattr(ticker, 'flow', ''),
                        base_volume=base_volume,
                        eps=eps,
                        p_e_ratio=p_e_ratio,
                        market_cap=market_cap,
                        float_shares=float_shares,
                        fiscal_year=fiscal_year,
                        state=state,
                        created_at=now,
                        updated_at=now
                    )
                    
                    if p_s_ratio is not None:
                        new_symbol.p_s_ratio = p_s_ratio
                    if nav is not None:
                        new_symbol.nav = nav
                    
                    db_session.add(new_symbol)
                    added_count += 1
                    
                # commit هر 5 رکورد برای جلوگیری از lock طولانی
                if (added_count + updated_count) % 5 == 0:
                    db_session.commit()
                    time.sleep(3)
            
            except Exception as e:
                logger.error(f"❌ خطا در پردازش نماد {symbol_name}: {e}")
                db_session.rollback()
                continue

        db_session.commit()
        logger.info(f"✅ دریافت و ذخیره نمادها کامل شد. {added_count} نماد جدید اضافه شد، {updated_count} نماد به‌روزرسانی شد.")
        return {"added": added_count, "updated": updated_count}
        
    except Exception as e:
        logger.error(f"❌ خطا در دریافت نمادها از pytse-client: {e}")
        db_session.rollback()
        return {"added": 0, "updated": 0}



def fetch_realtime_data_for_all_symbols(db_session: Session):
    """
    دریافت اطلاعات لحظه‌ای برای تمام نمادها
    """
    try:
        # این تابع نیاز به پیاده‌سازی دارد
        # فعلاً return 0 می‌کنیم تا خطا ندهد
        logger.warning("⚠️ تابع fetch_realtime_data_for_all_symbols هنوز پیاده‌سازی نشده است")
        return 0
        
    except Exception as e:
        logger.error(f"❌ خطا در دریافت اطلاعات لحظه‌ای: {e}")
        return 0


# ----------------------------
# تابع fetch_and_process_historical_data (نسخه بازنویسی شده و جامع)
# ----------------------------



def fetch_and_process_historical_data(
    db_session: Session,
    limit: Optional[int] = None,
    specific_symbols_list: Optional[List[str]] = None,
    limit_per_run: Optional[int] = None,
    days_limit: Optional[int] = None  # compatibility alias
) -> Tuple[int, str]:
    # normalize
    if limit is None and limit_per_run is not None:
        limit = limit_per_run
    # days_limit فعلاً استفاده نمیشه، فقط برای جلوگیری از ارور
    """
    دریافت و پردازش داده‌های تاریخی جامع برای نمادهای مشخص یا تمام نمادها.
    این تابع داده‌های تاریخی، داده‌های حقیقی/حقوقی را در یک حلقه تجمیع می‌کند.
    """
    logger.info("📈 آپدیت داده‌های تاریخی به صورت جامع...")
    updated_count = 0
    message = ""

    try:
        query = db_session.query(ComprehensiveSymbolData)
        if specific_symbols_list:
            symbol_conditions = []
            for symbol_identifier in specific_symbols_list:
                if str(symbol_identifier).isdigit():
                    symbol_conditions.append(ComprehensiveSymbolData.tse_index == str(symbol_identifier))
                else:
                    symbol_conditions.append(ComprehensiveSymbolData.symbol_name == symbol_identifier)
            query = query.filter(or_(*symbol_conditions))

        if limit:
            query = query.limit(limit)
        
        symbols_to_update = query.all()
        if not symbols_to_update:
            return 0, "No symbols to update."

        for sym in symbols_to_update:
            logger.info(f"📊 آپدیت دیتای تاریخی جامع برای {sym.symbol_name} (ID: {sym.id})")
            
            last_db_date = db_session.query(
                func.max(HistoricalData.date)
            ).filter(HistoricalData.symbol_id == sym.id).scalar()
            
            try:
                if sym.tse_index:
                    ticker = tse.Ticker("", index=str(sym.tse_index))
                else:
                    ticker = tse.Ticker(sym.symbol_name)

                # 1. دریافت داده‌های اصلی تاریخی (شامل OHLCV)
                df_history = ticker.history
                if df_history is None or df_history.empty:
                    logger.info(f"ℹ️ دیتای تاریخی جدیدی برای نماد {sym.symbol_name} یافت نشد.")
                    continue
                
                # 2. دریافت داده‌های حقیقی/حقوقی
                df_client_types = ticker.client_types
                if df_client_types is None or df_client_types.empty:
                    # ===> بهینه‌سازی: ایجاد DataFrame خالی فقط با ستون date برای merge موفق
                    df_client_types = pd.DataFrame(columns=['date'])
                else:
                    # تغییر نام ستون‌ها برای تطابق با مدل دیتابیس
                    df_client_types = df_client_types.rename(columns={
                        "individual_buy_count": "buy_count_i",
                        "corporate_buy_count": "buy_count_n",
                        "individual_sell_count": "sell_count_i",
                        "corporate_sell_count": "sell_count_n",
                        "individual_buy_vol": "buy_i_volume",
                        "corporate_buy_vol": "buy_n_volume",
                        "individual_sell_vol": "sell_i_volume",
                        "corporate_sell_vol": "sell_n_volume"
                    })                                             

                # 3. ادغام داده‌ها (کامنت‌های توضیحی شما حذف شد چون منطق در کد پیاده‌سازی می‌شود)
                df_history['date'] = pd.to_datetime(df_history['date'])
                if 'date' in df_client_types.columns:
                    df_client_types['date'] = pd.to_datetime(df_client_types['date'])
                
                df_merged = pd.merge(df_history, df_client_types, on='date', how='left')

                # فیلتر کردن داده‌های جدید
                if last_db_date:
                    new_data = df_merged[df_merged['date'] > pd.to_datetime(last_db_date)].copy()
                else:
                    new_data = df_merged.copy()

                if new_data.empty:
                    logger.info(f"ℹ️ دیتای تاریخی جدیدی برای نماد {sym.symbol_name} یافت نشد.")
                    continue
                
                # ===> CHANGE START: بخش اصلی اضافه شده برای محاسبه ستون‌های جدید
                # اطمینان از مرتب‌سازی بر اساس تاریخ برای محاسبات صحیح
                new_data.sort_values(by='date', inplace=True)
                
                # محاسبه ستون‌های قیمت
                new_data['final'] = new_data['close']
                new_data['yesterday_price'] = new_data['close'].shift(1)
                
                # محاسبه تغییرات قیمت (Price Changes)
                new_data['plc'] = new_data['close'] - new_data['yesterday_price']
                new_data['plp'] = (new_data['plc'] / new_data['yesterday_price']) * 100
                new_data['pcc'] = new_data['final'] - new_data['yesterday_price']
                new_data['pcp'] = (new_data['pcc'] / new_data['yesterday_price']) * 100
                
                # بهترین تقریب برای ارزش بازار تاریخی، ارزش معاملات همان روز است
                new_data['mv'] = new_data['value']

                # جایگزینی مقادیر بی‌نهایت (inf) با None تا در دیتابیس خطا ایجاد نکند
                new_data.replace([np.inf, -np.inf], None, inplace=True)
                # ===> CHANGE END

                # تبدیل تاریخ میلادی به شمسی
                new_data['jdate'] = new_data['date'].apply(
                    lambda x: jdatetime.date.fromgregorian(date=x.date()).strftime("%Y-%m-%d")
                )
                
                # تبدیل NaN به None و سپس به دیکشنری
                records = new_data.where(pd.notnull(new_data), None).to_dict('records')
            
                historical_records = [
                    HistoricalData(
                        symbol_id=sym.symbol_id,
                        symbol_name=sym.symbol_name,
                        date=rec['date'].date(),
                        jdate=rec['jdate'],
                        open=rec.get('open'),
                        high=rec.get('high'),
                        low=rec.get('low'),
                        close=rec.get('close'),
                        volume=rec.get('volume'),
                        value=rec.get('value'),
                        num_trades=rec.get('count'),
                        
                        # ===> CHANGE START: اصلاح کلیدها برای خواندن از ستون‌های محاسبه‌شده
                        final=rec.get('final'),
                        yesterday_price=rec.get('yesterday_price'),
                        plc=rec.get('plc'),
                        plp=rec.get('plp'),
                        pcc=rec.get('pcc'),
                        pcp=rec.get('pcp'),
                        mv=rec.get('mv'),
                        # ===> CHANGE END
                        
                        buy_count_i=rec.get('buy_count_i'),
                        buy_count_n=rec.get('buy_count_n'),
                        sell_count_i=rec.get('sell_count_i'),
                        sell_count_n=rec.get('sell_count_n'),
                        buy_i_volume=rec.get('buy_i_volume'),
                        buy_n_volume=rec.get('buy_n_volume'),
                        sell_i_volume=rec.get('sell_i_volume'),
                        sell_n_volume=rec.get('sell_n_volume'),
                        
                        # داده‌های عمق بازار در تاریخچه وجود ندارند و به درستی None باقی می‌مانند
                        zd1=None, qd1=None, pd1=None,
                        zo1=None, qo1=None, po1=None,
                        zd2=None, qd2=None, pd2=None,
                        zo2=None, qo2=None, po2=None,
                        zd3=None, qd3=None, pd3=None,
                        zo3=None, qo3=None, po3=None,
                        zd4=None, qd4=None, pd4=None,
                        zo4=None, qo4=None, po4=None,
                        zd5=None, qd5=None, pd5=None,
                        zo5=None, qo5=None, po5=None
                    ) for rec in records
                ]

                if historical_records:
                    db_session.bulk_save_objects(historical_records)
                    updated_count += len(historical_records)
                    logger.info(f"✅ {len(historical_records)} رکورد جدید برای {sym.symbol_name} اضافه شد.")
                    db_session.commit()
                
                safe_sleep(DEFAULT_PER_SYMBOL_DELAY)
            
            except Exception as e:
                logger.error(f"❌ خطا در پردازش داده‌های تاریخی برای نماد {sym.symbol_name}: {e}")
                logger.error(traceback.format_exc()) # لاگ کردن کامل خطا برای دیباگ بهتر
                db_session.rollback()
                continue
    
    except SQLAlchemyError as e:
        logger.error(f"❌ خطا در پایگاه داده: {e}")
        db_session.rollback()
        return 0, f"Database error: {e}"
    except Exception as e:
        logger.error(f"❌ خطا در تابع اصلی: {e}")
        return 0, f"An unexpected error occurred: {e}"
        
    message = f"✅ آپدیت داده‌های تاریخی کامل شد. {updated_count} رکورد جدید اضافه شد."
    logger.info(message)
    return updated_count, message



# ----------------------------
# تابع update_historical_data_limited (نسخه اصلاح‌شده)
# ----------------------------

#def update_historical_data_limited(DELETED)



# ----------------------------
# تابع اجرای update_symbol_fundamental_data
# ----------------------------
def update_symbol_fundamental_data(
    db_session: Session,
    limit: Optional[int] = None,
    specific_symbols_list: Optional[List[str]] = None,
    limit_per_run: Optional[int] = None,
    days_limit: Optional[int] = None  # compatibility alias
) -> Tuple[int, str]:
    # normalize
    if limit is None and limit_per_run is not None:
        limit = limit_per_run
    # days_limit فعلاً استفاده نمیشه، فقط برای جلوگیری از ارور
    """
    آپدیت اطلاعات بنیادی (Fundamental) نمادها.
    این تابع اطلاعاتی مانند EPS, P/E, و Market Cap را از Ticker دریافت می‌کند.
    """
    logger.info("📈 آپدیت اطلاعات بنیادی...")
    updated_symbols_count = 0
    message = "Fundamental data updated successfully."

    try:
        query = db_session.query(ComprehensiveSymbolData)
        if specific_symbols_list:
            symbol_conditions = []
            for symbol_identifier in specific_symbols_list:
                if str(symbol_identifier).isdigit():
                    # ورودی عددی → tse_index
                    symbol_conditions.append(ComprehensiveSymbolData.tse_index == int(symbol_identifier))
                else:
                    # ورودی متنی → نام نماد
                    symbol_conditions.append(ComprehensiveSymbolData.symbol_name == symbol_identifier)
            
            query = query.filter(or_(*symbol_conditions))

        symbols_to_update = query.order_by(
            ComprehensiveSymbolData.last_fundamental_update_date.asc()
        ).limit(limit).all()

        if not symbols_to_update:
            return 0, "No symbols to update."

        for sym in symbols_to_update:
            logger.info(f"📊 آپدیت دیتای بنیادی برای {sym.symbol_name} (TSEIndex: {sym.tse_index})")
            
            try:
                ticker = tse.Ticker(sym.symbol_name, index=sym.tse_index)

                
                # دریافت یا ایجاد رکورد FundamentalData
                fundamental_data = db_session.query(FundamentalData).filter_by(symbol_id=sym.symbol_id).first()
                if not fundamental_data:
                    fundamental_data = FundamentalData(symbol_id=sym.symbol_id)
                    db_session.add(fundamental_data)
                
                # بروزرسانی فیلدهای بنیادی
                try:
                    fundamental_data.eps = ticker.eps
                # ===> بهینه‌سازی: مدیریت خطای TypeError
                except (ValueError, TypeError):
                    fundamental_data.eps = None
                    logger.warning(f"⚠️ EPS برای {sym.symbol_name} معتبر نبود.")
                
                try:
                    p_e = ticker.p_e_ratio
                    fundamental_data.p_e_ratio = p_e
                    # ===> افزودن: پر کردن ستون pe اگر در مدل شما وجود دارد
                    if hasattr(fundamental_data, 'pe'):
                        fundamental_data.pe = p_e
                # ===> بهینه‌سازی: مدیریت خطای TypeError
                except (ValueError, TypeError):
                    fundamental_data.p_e_ratio = None
                    if hasattr(fundamental_data, 'pe'):
                        fundamental_data.pe = None
                    logger.warning(f"⚠️ P/E Ratio برای {sym.symbol_name} معتبر نبود.")

                try:
                    fundamental_data.group_p_e_ratio = ticker.group_p_e_ratio
                # ===> بهینه‌سازی: مدیریت خطای TypeError
                except (ValueError, TypeError):
                    fundamental_data.group_p_e_ratio = None

                try:
                    p_s = ticker.p_s_ratio
                    fundamental_data.p_s_ratio = p_s
                    # ===> افزودن: پر کردن ستون psr اگر در مدل شما وجود دارد
                    if hasattr(fundamental_data, 'psr'):
                        fundamental_data.psr = p_s
                # ===> بهینه‌سازی: مدیریت خطای TypeError
                except (ValueError, TypeError):
                    fundamental_data.p_s_ratio = None
                    if hasattr(fundamental_data, 'psr'):
                        fundamental_data.psr = None
                
                # ===> افزودن: خواندن و ذخیره حجم مبنا (Base Volume)
                try:
                    fundamental_data.base_volume = ticker.base_volume
                except (ValueError, TypeError):
                    fundamental_data.base_volume = None

                # این بخش بدون تغییر باقی مانده است
                fundamental_data.total_shares = ticker.total_shares
                fundamental_data.float_shares = ticker.float_shares
                fundamental_data.market_cap = ticker.market_cap
                fundamental_data.fiscal_year = ticker.fiscal_year
                fundamental_data.last_update_date = date.today()
                
                # آپدیت تاریخ آخرین بروزرسانی
                sym.last_fundamental_update_date = date.today()
                
                updated_symbols_count += 1
                db_session.commit()
            
            except Exception as e:
                db_session.rollback()
                # ===> بهینه‌سازی: لاگ کردن کامل خطا برای دیباگ بهتر
                logger.error(f"❌ خطا در آپدیت دیتای بنیادی برای {sym.symbol_name}: {e}", exc_info=True)
                continue

    except Exception as e:
        db_session.rollback()
        # ===> بهینه‌سازی: لاگ کردن کامل خطا برای دیباگ بهتر
        logger.error(f"❌ خطا در آپدیت دیتای بنیادی: {e}", exc_info=True)
        message = str(e)
        
    return updated_symbols_count, message




# ----------------------------
# تابع run_full_data_update (نسخه 10 جولای)
# ----------------------------
# تابع کمکی برای یافتن آخرین تاریخ هر نماد
def get_last_dates(db_session: Session) -> dict:
    """
    آخرین تاریخ ثبت شده در جدول HistoricalData را برای هر نماد برمی‌گرداند.
    """
    try:
        results = db_session.query(
            HistoricalData.symbol_id,
            func.max(HistoricalData.date)
        ).group_by(
            HistoricalData.symbol_id
        ).all()
        return {symbol_id: last_date for symbol_id, last_date in results}
    except Exception as e:
        logger.error(f"❌ خطا در دریافت آخرین تاریخ‌های نمادها: {e}")
        return {}


# ایجاد sessionmaker برای مدیریت connection pooling
def get_session_local():
    """ایجاد session local با application context"""
    try:
        from flask import current_app
        with current_app.app_context():
            return sessionmaker(bind=db.engine)()
    except RuntimeError:
        # اگر خارج از application context هستیم
        return sessionmaker(bind=db.get_engine())()


def get_symbol_id(symbol_identifier: str) -> Optional[int]:
    if not symbol_identifier:
        return None

    session = db.session  # استفاده از session پیش‌فرض

    from models import ComprehensiveSymbolData 

    try:
        if str(symbol_identifier).isdigit():
            sym = session.query(ComprehensiveSymbolData.id).filter(
                ComprehensiveSymbolData.tse_index == str(symbol_identifier)
            ).first()
            if sym:
                return sym[0]
    except Exception:
        pass

    sym = session.query(ComprehensiveSymbolData.id).filter(
        ComprehensiveSymbolData.symbol_name == symbol_identifier
    ).first()

    if sym:
        logger.info(f"Symbol found. ID: {sym[0]}") # ✅ لاگ موفقیت
        return sym[0]
    
    logger.warning(f"Symbol NOT found in DB with name: {symbol_identifier}") # ✅ لاگ عدم موفقیت
    return None




# ----------------------------
#این فانکشن برای هر نماد یک ردیف real-time می‌سازه و در HistoricalData یا یک جدول جدا ذخیره می‌کنه
# ----------------------------
def fetch_realtime_snapshot(db_session: Session, symbol_name: str, symbol_id: int):
    try:
        ticker = tse.Ticker(symbol_name)

        # دیتای اصلی قیمت
        final_price = ticker.last_price
        yesterday_price = ticker.yesterday_price
        adj_close = ticker.adj_close
        mv = ticker.market_cap
        eps = ticker.eps
        pe = ticker.p_e_ratio

        # تغییرات قیمتی
        plc = final_price - yesterday_price if final_price and yesterday_price else None
        plp = (plc / yesterday_price * 100) if plc and yesterday_price else None
        pcc = adj_close - yesterday_price if adj_close and yesterday_price else None
        pcp = (pcc / yesterday_price * 100) if pcc and yesterday_price else None

        # حقیقی حقوقی
        ct = ticker.client_types
        buy_count_i = ct["individual_buy_count"].iloc[-1] if not ct.empty else None
        buy_count_n = ct["corporate_buy_count"].iloc[-1] if not ct.empty else None
        sell_count_i = ct["individual_sell_count"].iloc[-1] if not ct.empty else None
        sell_count_n = ct["corporate_sell_count"].iloc[-1] if not ct.empty else None
        buy_i_volume = ct["individual_buy_vol"].iloc[-1] if not ct.empty else None
        buy_n_volume = ct["corporate_buy_vol"].iloc[-1] if not ct.empty else None
        sell_i_volume = ct["individual_sell_vol"].iloc[-1] if not ct.empty else None
        sell_n_volume = ct["corporate_sell_vol"].iloc[-1] if not ct.empty else None

        # عمق بازار (سطح ۵)
        orderbook = ticker.get_ticker_real_time_info_response()
        buy_orders = orderbook.buy_orders
        sell_orders = orderbook.sell_orders

        dom_fields = {}
        for i in range(5):
            dom_fields[f"zd{i+1}"] = buy_orders[i].count if len(buy_orders) > i else None
            dom_fields[f"qd{i+1}"] = buy_orders[i].volume if len(buy_orders) > i else None
            dom_fields[f"pd{i+1}"] = buy_orders[i].price if len(buy_orders) > i else None
            dom_fields[f"zo{i+1}"] = sell_orders[i].count if len(sell_orders) > i else None
            dom_fields[f"qo{i+1}"] = sell_orders[i].volume if len(sell_orders) > i else None
            dom_fields[f"po{i+1}"] = sell_orders[i].price if len(sell_orders) > i else None

        # ساخت رکورد دیتابیس
        snapshot = HistoricalData(
            symbol_id=symbol_id,
            symbol_name=symbol_name,
            date=date.today(),
            final=final_price,
            yesterday_price=yesterday_price,
            plc=plc,
            plp=plp,
            pcc=pcc,
            pcp=pcp,
            #mv=mv,
            #eps=eps,
            #pe=pe,
            buy_count_i=buy_count_i,
            buy_count_n=buy_count_n,
            sell_count_i=sell_count_i,
            sell_count_n=sell_count_n,
            buy_i_volume=buy_i_volume,
            buy_n_volume=buy_n_volume,
            sell_i_volume=sell_i_volume,
            sell_n_volume=sell_n_volume,
            **dom_fields
        )

        db_session.add(snapshot)


        fundamental = db_session.query(FundamentalData).filter_by(symbol_id=symbol_id).first()
        if fundamental:
            #fundamental.eps = eps
            fundamental.pe = pe
            #fundamental.market_cap = mv
        else:
            fundamental = FundamentalData(
                symbol_id=symbol_id,
                #eps=eps,
                pe=pe
                #market_cap=mv
            )
            db_session.add(fundamental)




        db_session.commit()
        return True, f"✅ Real-time snapshot stored for {symbol_name}"

    except Exception as e:
        db_session.rollback()
        return False, f"❌ Error fetching snapshot for {symbol_name}: {e}"




# ----------------------------
# تابع اجرای آپدیت کامل
# ----------------------------
def run_full_data_update(
    db_session: Session = None,
    limit_per_run: int = 100,
    specific_symbols_list: list = None,

    update_fundamental: bool = True,
    update_realtime: bool = True,
    update_technical: bool = True
):
    """
    اجرای آپدیت کامل دیتای نمادها (تاریخی + تکنیکال + بنیادی + لحظه‌ای)
    اگر db_session پاس داده نشود، خودش session را از extensions.db می‌سازد.
    """
    if db_session is None:
        db_session = db.session

    logger.info("🚀 شروع آپدیت کامل داده‌ها برای همه نمادها...")

    results = {
        "historical": {"count": 0, "message": ""},
        "technical": {"count": 0, "message": ""},
        "fundamental": {"count": 0, "message": ""},
        "realtime": {"count": 0, "message": ""},
        "candlestick": {"count": 0, "message": ""}
    }

    # 1. ابتدا نمادها را از pytse-client دریافت و به‌روزرسانی کنیم
    try:
        logger.info("📥 به‌روزرسانی لیست نمادها از pytse-client...")
        symbol_update_result = fetch_symbols_from_pytse_client(db_session, limit=None)
        logger.info(f"✅ به‌روزرسانی لیست نمادها کامل شد: {symbol_update_result}")
    except Exception as e:
        logger.error(f"❌ خطا در به‌روزرسانی لیست نمادها: {e}")

    # 2. آپدیت داده‌های تاریخی
    try:
        logger.info("📊 شروع آپدیت داده‌های تاریخی...")
        processed_hist_count, hist_msg = fetch_and_process_historical_data(
            db_session,
            limit=limit_per_run,
            specific_symbols_list=specific_symbols_list,

        )
        results["historical"]["count"] = processed_hist_count
        results["historical"]["message"] = hist_msg
        logger.info(hist_msg)
    except Exception as e:
        error_msg = f"❌ خطا در اجرای آپدیت تاریخی: {e}"
        results["historical"]["message"] = error_msg
        logger.error(error_msg)

    # 3. آپدیت داده‌های لحظه‌ای
    if update_realtime:
        try:
            logger.info("⏰ شروع آپدیت داده‌های لحظه‌ای...")
            realtime_count = fetch_realtime_data_for_all_symbols(db_session)
            results["realtime"]["count"] = realtime_count
            results["realtime"]["message"] = f"✅ اطلاعات لحظه‌ای برای {realtime_count} نماد به‌روزرسانی شد"
            logger.info(results["realtime"]["message"])

            # ✅ گرفتن snapshot کامل از همه نمادها
            all_symbols = db_session.query(ComprehensiveSymbolData).all()
            snapshot_count = 0
            for sym in all_symbols:
                success, msg = fetch_realtime_snapshot(db_session, sym.symbol_name, sym.id)
                logger.info(msg)
                if success:
                    snapshot_count += 1

            results["realtime"]["message"] += f" | ✅ snapshot برای {snapshot_count} نماد ذخیره شد"

        except Exception as e:
            error_msg = f"❌ خطا در اجرای آپدیت لحظه‌ای: {e}"
            results["realtime"]["message"] = error_msg
            logger.error(error_msg)



    # 4. آپدیت داده‌های بنیادی
    if update_fundamental:
        try:
            logger.info("📈 شروع آپدیت داده‌های بنیادی...")
            
            # 🛠️ بهینه‌سازی: دریافت لیست نمادها بر اساس آخرین آپدیت
            query = db_session.query(ComprehensiveSymbolData)
            if specific_symbols_list:
                symbol_conditions = [or_(ComprehensiveSymbolData.symbol_name == s, ComprehensiveSymbolData.tse_index == s) for s in specific_symbols_list]
                query = query.filter(or_(*symbol_conditions))
            
            # ⚠️ اصلاح فیلتر هوشمند بر اساس ستون درست و با استفاده از timedelta
            symbols_to_update = query.filter(
                (ComprehensiveSymbolData.last_fundamental_update_date.is_(None)) | 
                (ComprehensiveSymbolData.last_fundamental_update_date < (date.today() - timedelta(days=7)))
            ).order_by(ComprehensiveSymbolData.last_fundamental_update_date.asc()).limit(limit_per_run).all()
            
            fundamental_count = 0
            for symbol in symbols_to_update:
                try:
                    updated_count, msg = update_symbol_fundamental_data(db_session, specific_symbols_list=[symbol.tse_index])
                    if updated_count > 0:
                        fundamental_count += updated_count
                        # ⚠️ آپدیت ستون زمان آخرین آپدیت
                        symbol.last_fundamental_update_date = date.today()
                        db_session.add(symbol)
                        db_session.commit()
                    time.sleep(0.1)
                except Exception as e:
                    logger.warning(f"⚠️ خطا در به‌روزرسانی بنیادی {symbol.symbol_name}: {e}")
                    db_session.rollback()
                    continue
            
            results["fundamental"]["count"] = fundamental_count
            results["fundamental"]["message"] = f"✅ اطلاعات بنیادی برای {fundamental_count} نماد به‌روزرسانی شد"
            logger.info(results["fundamental"]["message"])
            
        except Exception as e:
            error_msg = f"❌ خطا در اجرای آپدیت بنیادی: {e}"
            results["fundamental"]["message"] = error_msg
            logger.error(error_msg)
            db_session.rollback()

    # 5. آپدیت تحلیل تکنیکال و 6. تشخیص الگوهای شمعی
    if update_technical:
        try:
            logger.info("📉 شروع تحلیل تکنیکال...")
            technical_count, tech_msg = run_technical_analysis(
                db_session,
                limit=limit_per_run,
                symbols_list=specific_symbols_list
            )
            results["technical"]["count"] = technical_count
            results["technical"]["message"] = tech_msg
            logger.info(results["technical"]["message"])
            
            
            # =======================================================
            # 🕯️ گام 6: تشخیص و ذخیره الگوهای شمعی (جدید)
            # این گام بلافاصله بعد از تحلیل تکنیکال و داخل همان بلوک try قرار می‌گیرد
            # =======================================================
            logger.info("🕯️ شروع تشخیص الگوهای شمعی...")
            try:
                # استفاده از همان لیست نمادها
                candlestick_count = run_candlestick_detection(
                    db_session, 
                    limit=limit_per_run,
                    symbols_list=specific_symbols_list
                )
                # ⚠️ اضافه کردن نتیجه به دیکشنری نتایج
                results["candlestick"] = {
                    "count": candlestick_count,
                    "message": f"✅ تشخیص الگوهای شمعی برای {candlestick_count} نماد انجام شد"
                }
                logger.info(results["candlestick"]["message"])
            except Exception as e:
                error_msg = f"❌ خطا در اجرای تشخیص الگوهای شمعی: {e}"
                results["candlestick"] = {"count": 0, "message": error_msg}
                logger.error(error_msg)
            

        except Exception as e:
            # این بلوک catch، خطای اصلی تحلیل تکنیکال را می‌گیرد
            error_msg = f"❌ خطا در اجرای تحلیل تکنیکال: {e}"
            results["technical"]["message"] = error_msg
            logger.error(error_msg)
            db_session.rollback()

    logger.info("✅ آپدیت کامل داده‌ها به اتمام رسید.")
    
    # خلاصه نتایج
    summary = f"""
📊 خلاصه نتایج آپدیت:
• تاریخی: {results['historical']['count']} نماد - {results['historical']['message']}
• لحظه‌ای: {results['realtime']['count']} نماد - {results['realtime']['message']}
• بنیادی: {results['fundamental']['count']} نماد - {results['fundamental']['message']}
• تکنیکال: {results['technical']['count']} نماد - {results['technical']['message']}
• شمعی: {results['candlestick']['count']} نماد - {results['candlestick']['message']}
    """
    logger.info(summary)

    # برای سازگاری با callerهای قدیمی، دو مقدار return می‌کنیم
    return results




# ----------------------------
# تابع اجرای آپدیت روزانه
# ----------------------------
def run_daily_update(
    db_session: Session,
    limit: int = 200, # limit now acts as BATCH_SIZE
    update_fundamental: bool = True,
    specific_symbols_list: Optional[List[str]] = None,
):
    """
    اجرای آپدیت روزانه به صورت دسته‌ای (batch) برای تضمین پردازش تمام نمادها.
    این تابع تا زمانی که نماد آپدیت‌نشده‌ای وجود داشته باشد، در یک حلقه اجرا می‌شود.
    """
    # ⚠️ حتماً مطمئن شوید که importsهای مورد نیاز (date, timedelta, logger, or_) در بالای فایل موجود باشند.
    logger.info("🚀 شروع آپدیت کامل روزانه به صورت دسته‌ای...")

    results = {
        "historical": {"total_count": 0},
        "technical": {"total_count": 0},
        "candlestick": {"total_count": 0, "message": ""},
        "fundamental": {"count": 0, "message": ""}
    }
    
    run_count = 0
    
    # ===============================
    # حلقه اصلی برای پردازش دسته‌ای
    # ===============================
    while True:
        run_count += 1
        logger.info(f"--- شروع پردازش دسته شماره {run_count} ---")

        today = date.today()
        
        # گام ۱: شناسایی یک دسته از نمادهایی که امروز آپدیت نشده‌اند
        symbols_to_update_query = db_session.query(ComprehensiveSymbolData).filter(
            or_(
                ComprehensiveSymbolData.last_historical_update_date.is_(None),
                ComprehensiveSymbolData.last_historical_update_date < today
            )
        )
        
        # اگر لیست خاصی از نمادها مشخص شده بود
        if specific_symbols_list:
            symbol_conditions = [
                or_(
                    ComprehensiveSymbolData.symbol_name == s,
                    ComprehensiveSymbolData.tse_index == s
                )
                for s in specific_symbols_list
            ]
            symbols_to_update_query = symbols_to_update_query.filter(or_(*symbol_conditions))


        # اعمال محدودیت به عنوان اندازه دسته
        symbols_in_batch = symbols_to_update_query.limit(limit).all()

        if not symbols_in_batch:
            logger.info("✅ تمام نمادها برای امروز به‌روز هستند. پایان عملیات Historical/Technical.")
            break

        symbol_ids_to_process = [s.tse_index for s in symbols_in_batch]
        logger.info(f"📊 یافت شد {len(symbol_ids_to_process)} نماد در این دسته برای آپدیت.")

        # گام ۲: دریافت داده‌های تاریخی برای این دسته
        hist_count, hist_msg = fetch_and_process_historical_data(
            db_session,
            specific_symbols_list=symbol_ids_to_process
        )
        results["historical"]["total_count"] += hist_count
        logger.info(hist_msg)

        # گام ۳ و ۴ فقط در صورت وجود داده جدید اجرا می‌شوند
        if hist_count > 0:
            # گام ۳: اجرای تحلیل تکنیکال
            tech_count, tech_msg = run_technical_analysis(
                db_session,
                symbols_list=symbol_ids_to_process
            )
            results["technical"]["total_count"] += tech_count
            logger.info(tech_msg)

            # گام ۴: تشخیص الگوهای شمعی
            try:
                candlestick_count = run_candlestick_detection(
                    db_session, 
                    symbols_list=symbol_ids_to_process
                )
                results["candlestick"]["total_count"] += candlestick_count
                logger.info(f"✅ تشخیص الگوهای شمعی برای {candlestick_count} نماد در این دسته انجام شد.")
            except Exception as e:
                logger.error(f"❌ خطا در اجرای تشخیص الگوهای شمعی برای این دسته: {e}")
        else:
            logger.warning("داده تاریخی جدیدی برای این دسته دریافت نشد، گام‌های تحلیل Skip شد.")
        

        # ----------------------------------------------------
        # 💥💥💥 FIX حلقه بی‌نهایت: به‌روزرسانی تاریخ آپدیت 💥💥💥
        # این کار تضمین می‌کند که این نمادها در دور بعدی کوئری فیلتر شوند.
        # این آپدیت باید انجام شود، حتی اگر داده جدیدی دریافت نشده باشد (hist_count=0).
        # ----------------------------------------------------
        for symbol in symbols_in_batch:
            symbol.last_historical_update_date = today # تنظیم به امروز
            db_session.add(symbol)
        
        # IMPROVEMENT: Commit changes after each successful batch
        try:
            db_session.commit()
            logger.info(f"✅ تغییرات دسته {run_count} با موفقیت در دیتابیس ثبت شد.")
        except Exception as e:
            logger.error(f"❌ خطا در ثبت تغییرات دسته {run_count}: {e}")
            db_session.rollback()
            break
            
    # ===============================
    # گام ۵: آپدیت داده‌های بنیادی (خارج از حلقه اصلی)
    # ===============================
    if update_fundamental:
        try:
            logger.info("📈 شروع آپدیت داده‌های بنیادی...")

            # limit_per_run در اینجا همان limit ورودی تابع است
            limit_per_run = limit

            query = db_session.query(ComprehensiveSymbolData)
            
            # فیلتر بر اساس لیست خاصی از نمادها
            if specific_symbols_list:
                symbol_conditions = [
                    or_(
                        ComprehensiveSymbolData.symbol_name == s,
                        ComprehensiveSymbolData.tse_index == s
                    )
                    for s in specific_symbols_list
                ]
                query = query.filter(or_(*symbol_conditions))

            # کوئری اصلی برای یافتن نمادهایی که آپدیت بنیادی قدیمی دارند
            symbols_to_update_fund = query.filter(
                (ComprehensiveSymbolData.last_fundamental_update_date.is_(None)) |
                (ComprehensiveSymbolData.last_fundamental_update_date < (date.today() - timedelta(days=3)))
            ).order_by(
                ComprehensiveSymbolData.last_fundamental_update_date.asc()
            ).limit(limit_per_run).all()
            
            fundamental_count = 0
            for symbol in symbols_to_update_fund:
                try:
                    # فراخوانی تابع آپدیت بنیادی (بر اساس tse_index)
                    updated_count, msg = update_symbol_fundamental_data(
                        db_session,
                        specific_symbols_list=[symbol.tse_index]
                    )
                    
                    if updated_count > 0:
                        fundamental_count += updated_count
                        # آپدیت تاریخ آخرین آپدیت بنیادی
                        symbol.last_fundamental_update_date = date.today()
                        db_session.add(symbol)
                        db_session.commit()
                    
                    # 💡 اضافه کردن یک تأخیر کوتاه برای جلوگیری از فشار بر API
                    time.sleep(0.1) 
                    
                except Exception as e:
                    db_session.rollback()
                    logger.warning(
                        f"⚠️ خطا در به‌روزرسانی بنیادی نماد {symbol.symbol_name} ({symbol.tse_index}): {e}"
                    )
                    continue

            results["fundamental"]["count"] = fundamental_count
            results["fundamental"]["message"] = f"✅ اطلاعات بنیادی برای {fundamental_count} نماد به‌روزرسانی شد"
            logger.info(results["fundamental"]["message"])

        except Exception as e:
            error_msg = f"❌ خطا در اجرای آپدیت بنیادی: {e}"
            results["fundamental"]["message"] = error_msg
            logger.error(error_msg, exc_info=True)
            db_session.rollback()
            
    else:
        results["fundamental"]["message"] = "آپدیت داده‌های بنیادی Skip شد."
        logger.info(results["fundamental"]["message"])

    # ===============================
    # جمع‌بندی نهایی و بازگشت نتایج
    # ===============================
    final_message = (
        f"🏁 آپدیت روزانه تکمیل شد. "
        f"Historical: {results['historical']['total_count']}, "
        f"Technical: {results['technical']['total_count']}, "
        f"Candlestick: {results['candlestick']['total_count']}, "
        f"Fundamental: {results['fundamental']['count']}."
    )
    logger.info(final_message)
    return results





# ----------------------------
# تابع initial_populate_all_symbols_and_data (نسخه اصلاح شده)
# ----------------------------

def initial_populate_all_symbols_and_data(db_session, limit: int = None):
    """
    تابع اولیه برای پر کردن دیتابیس با نمادها و داده‌های تاریخی، لحظه‌ای و بنیادی
    """
    try:
        logger.info("🔄 شروع فرآیند اولیه پر کردن دیتابیس...")
        
        # 1. دریافت نمادها از pytse-client و ذخیره در ComprehensiveSymbolData
        logger.info("📥 دریافت نمادها از pytse-client...")
        symbol_result = fetch_symbols_from_pytse_client(db_session, limit)
        
        added_count = symbol_result.get("added", 0)
        updated_count = symbol_result.get("updated", 0)
        
        if added_count > 0 or updated_count > 0:
            logger.info(f"📊 {added_count} نماد جدید اضافه شد، {updated_count} نماد به‌روزرسانی شد.")
            
            # 2. آپدیت داده‌های تاریخی
            logger.info("📈 آپدیت داده‌های تاریخی...")
            processed_count, msg = fetch_and_process_historical_data(
                db_session, 
                limit=limit
            )
            logger.info(msg)
            
            # 3. آپدیت داده‌های لحظه‌ای
            logger.info("⏰ آپدیت داده‌های لحظه‌ای...")
            realtime_count = fetch_realtime_data_for_all_symbols(db_session)
            logger.info(f"✅ اطلاعات لحظه‌ای برای {realtime_count} نماد به‌روزرسانی شد")
            
            # 4. آپدیت داده‌های بنیادی
            logger.info("📊 آپدیت داده‌های بنیادی...")

            # ⚠️ کوئری گرفتن همزمان id و tse_index (با اعمال limit در صورت وجود)
            symbols_to_update = db_session.query(
                ComprehensiveSymbolData.id,
                ComprehensiveSymbolData.tse_index
            ).limit(limit).all()

            fundamental_count = 0
            for symbol_id, tse_index in symbols_to_update:
                try:
                    # فراخوانی تابع آپدیت بنیادی (بر اساس tse_index)
                    updated_count, msg = update_symbol_fundamental_data(
                        db_session,
                        specific_symbols_list=[tse_index]
                    )
                    fundamental_count += updated_count

                    # 💡 نکته: 'time.sleep(0.2)' برای افزایش سرعت حذف شد.

                except Exception as e:
                    # 🛠️ در صورت خطا تراکنش Rollback شود تا دیتابیس قفل نشود
                    db_session.rollback()
                    logger.warning(
                        f"⚠️ خطا در به‌روزرسانی بنیادی نماد {tse_index} (ID={symbol_id}): {e}"
                    )
                    continue  # ادامه برای نماد بعدی

            logger.info(f"✅ اطلاعات بنیادی برای {fundamental_count} نماد به‌روزرسانی شد")

            # 5. اجرای تحلیل تکنیکال
            logger.info("📉 اجرای تحلیل تکنیکال...")
            try:
                # ⚠️ در تحلیل تکنیکال فقط به id نیاز داریم
                symbol_ids = [sid for sid, _ in symbols_to_update]

                run_technical_analysis(
                    db_session,
                    limit=limit,
                    symbols_list=symbol_ids
                )
                logger.info("✅ تحلیل تکنیکال با موفقیت اجرا شد")

            except Exception as e:
                logger.error(f"❌ خطا در اجرای تحلیل تکنیکال: {e}")
            
            total_processed = added_count + updated_count
            success_msg = f"""
✅ فرآیند اولیه پر کردن دیتابیس کامل شد:
• {added_count} نماد جدید اضافه شد
• {updated_count} نماد به‌روزرسانی شد
• {processed_count} نماد داده‌های تاریخی دریافت کردند
• {realtime_count} نماد داده‌های لحظه‌ای دریافت کردند
• {fundamental_count} نماد داده‌های بنیادی دریافت کردند
            """
            logger.info(success_msg)
            
            return {
                "added": added_count,
                "updated": updated_count,
                "historical": processed_count,
                "realtime": realtime_count,
                "fundamental": fundamental_count,
                "message": success_msg
            }
        else:
            logger.info("ℹ️ هیچ نماد جدیدی اضافه یا به‌روزرسانی نشد.")
            return {
                "added": 0,
                "updated": 0,
                "historical": 0,
                "realtime": 0,
                "fundamental": 0,
                "message": "ℹ️ هیچ نماد جدیدی اضافه یا به‌روزرسانی نشد."
            }
            
    except Exception as e:
        error_msg = f"❌ خطا در فرآیند اولیه پر کردن دیتابیس: {e}"
        logger.error(error_msg)
        import traceback
        logger.error(traceback.format_exc())
        
        return {
            "added": 0,
            "updated": 0,
            "historical": 0,
            "realtime": 0,
            "fundamental": 0,
            "message": error_msg
        }


# ----------------------------
# توابع تحلیل تکنیکال
# ----------------------------

# ----------------------------
# تابع run_technical_analysis (نسخه بازنویسی شده نهایی)
# ----------------------------

def run_technical_analysis(db_session: Session, limit: int = None, symbols_list: list = None, batch_size: int = 200):
    """
    اجرای تحلیل تکنیکال در بچ‌های کوچک برای جلوگیری از مصرف زیاد حافظه.
    """
    try:
        logger.info("📈 شروع تحلیل تکنیکال...")

        # دریافت لیست یکتا از نمادها
        symbol_ids_query = db_session.query(HistoricalData.symbol_id).distinct()
        if symbols_list:
            symbol_ids_query = symbol_ids_query.filter(HistoricalData.symbol_id.in_(symbols_list))

        all_symbols = [row[0] for row in symbol_ids_query.all()]
        total_symbols = len(all_symbols)
        logger.info(f"🔍 مجموع {total_symbols} نماد برای تحلیل تکنیکال یافت شد")

        processed_count = 0
        success_count = 0
        error_count = 0

        # اجرای تحلیل در بچ‌های 200تایی
        for i in range(0, total_symbols, batch_size):
            batch_symbols = all_symbols[i:i + batch_size]
            logger.info(f"📦 پردازش بچ {i // batch_size + 1}: نمادهای {i + 1} تا {min(i + batch_size, total_symbols)}")

            query = db_session.query(
                HistoricalData.symbol_id, HistoricalData.symbol_name, HistoricalData.date, HistoricalData.jdate, 
                HistoricalData.open, HistoricalData.close, HistoricalData.high, HistoricalData.low, 
                HistoricalData.volume, HistoricalData.final, HistoricalData.yesterday_price, HistoricalData.plc, 
                HistoricalData.plp, HistoricalData.pcc, HistoricalData.pcp, HistoricalData.mv, 
                HistoricalData.buy_count_i, HistoricalData.buy_count_n, HistoricalData.sell_count_i, 
                HistoricalData.sell_count_n, HistoricalData.buy_i_volume, HistoricalData.buy_n_volume, 
                HistoricalData.sell_i_volume, HistoricalData.sell_n_volume
            ).filter(HistoricalData.symbol_id.in_(batch_symbols))

            query = query.order_by(HistoricalData.symbol_id, HistoricalData.date)
            historical_data = query.all()

            if not historical_data:
                logger.warning(f"⚠️ هیچ داده‌ای برای این بچ یافت نشد.")
                continue

            columns = [
                'symbol_id', 'symbol_name', 'date', 'jdate', 'open', 'close', 'high', 'low', 'volume',
                'final', 'yesterday_price', 'plc', 'plp', 'pcc', 'pcp', 'mv',
                'buy_count_i', 'buy_count_n', 'sell_count_i', 'sell_count_n',
                'buy_i_volume', 'buy_n_volume', 'sell_i_volume', 'sell_n_volume'
            ]
            df = pd.DataFrame(historical_data, columns=columns)

            grouped = df.groupby('symbol_id')

            for symbol_id, group_df in grouped:
                if limit is not None and processed_count >= limit:
                    break

                processed_count += 1
                try:
                    df_indicators = calculate_all_indicators(group_df)
                    save_technical_indicators(db_session, symbol_id, df_indicators)
                    success_count += 1

                    if processed_count % 10 == 0:
                        logger.info(f"📊 پیشرفت تحلیل: {processed_count}/{total_symbols} نماد")

                except Exception as e:
                    error_count += 1
                    logger.error(f"❌ خطا در تحلیل نماد {symbol_id}: {e}")
                    db_session.rollback()

            # 🔹 آزادسازی حافظه‌ی DataFrame بعد از هر بچ
            del df
            import gc
            gc.collect()

        logger.info(f"✅ تحلیل تکنیکال کامل شد. موفق: {success_count} | خطا: {error_count}")
        return success_count, f"تحلیل کامل شد. {success_count} موفق، {error_count} خطا"

    except Exception as e:
        error_msg = f"❌ خطا در اجرای تحلیل تکنیکال: {e}"
        logger.error(error_msg)
        db_session.rollback()
        return 0, error_msg



# توابع کمکی برای محاسبه اندیکاتورها (با فرض import بودن pd, np, Tuple, logger)
# ----------------------------

# توابع استاندارد (بدون تغییر، چون از Series به عنوان ورودی استفاده می‌کنند):
def calculate_sma(series: pd.Series, period: int) -> pd.Series:
    """محاسبه میانگین متحرک ساده (SMA)"""
    return series.rolling(window=period).mean()

def calculate_volume_ma(series: pd.Series, period: int) -> pd.Series:
    """محاسبه میانگین متحرک حجم"""
    return series.rolling(window=period).mean()

def calculate_rsi(series: pd.Series, period: int = 14) -> pd.Series:
    """محاسبه RSI"""
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    with np.errstate(divide='ignore', invalid='ignore'):
        rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    return rsi

def calculate_macd(series: pd.Series, fast: int = 12, slow: int = 26, signal: int = 9) -> Tuple[pd.Series, pd.Series, pd.Series]:
    """محاسبه MACD"""
    ema_fast = series.ewm(span=fast, adjust=False).mean()
    ema_slow = series.ewm(span=slow, adjust=False).mean()
    macd = ema_fast - ema_slow
    macd_signal = macd.ewm(span=signal, adjust=False).mean()
    macd_histogram = macd - macd_signal
    return macd, macd_signal, macd_histogram

def calculate_bollinger_bands(series: pd.Series, period: int = 20, std_dev: int = 2) -> Tuple[pd.Series, pd.Series, pd.Series]:
    """محاسبه Bollinger Bands"""
    middle = series.rolling(window=period).mean()
    std = series.rolling(window=period).std()
    upper = middle + (std * std_dev)
    lower = middle - (std * std_dev)
    return upper, middle, lower

def calculate_atr(high: pd.Series, low: pd.Series, close: pd.Series, period: int = 14) -> pd.Series:
    """محاسبه ATR (Average True Range)"""
    tr1 = high - low
    tr2 = abs(high - close.shift())
    tr3 = abs(low - close.shift())
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    atr = tr.rolling(window=period).mean()
    return atr

def calculate_stochastic(high: pd.Series, low: pd.Series, close: pd.Series, window: int = 14, smooth_k: int = 3, smooth_d: int = 3) -> Tuple[pd.Series, pd.Series]:
    """محاسبه Stochastic Oscillator (%K و %D)."""
    high = high.squeeze()
    low = low.squeeze()
    close = close.squeeze()
    
    # 💡 نکته: بهتر است در اینجا نیز از نام‌های اصلی استفاده شود یا سری‌ها به عنوان ورودی مستقیم باشند
    df_stoch = pd.DataFrame({'high': high, 'low': low, 'close': close}).apply(pd.to_numeric, errors='coerce')
    if df_stoch.isnull().all().all() or len(df_stoch.dropna()) < window:
        nan_series = pd.Series([np.nan] * len(close), index=close.index)
        return nan_series, nan_series

    low_min = df_stoch['low'].rolling(window=window).min()
    high_max = df_stoch['high'].rolling(window=window).max()
    
    k = 100 * ((df_stoch['close'] - low_min) / (high_max - low_min).replace(0, np.nan))
    k = k.fillna(50) 
    
    d = k.rolling(window=smooth_k).mean()
    
    return k.astype(float).reindex(close.index), d.astype(float).reindex(close.index)

# --- توابع پیشرفته با ستون‌های اصلاح شده ---

def calculate_squeeze_momentum(df: pd.DataFrame, bb_window=20, bb_std=2, kc_window=20, kc_mult=1.5) -> Tuple[pd.Series, pd.Series]:
    """محاسبه Squeeze Momentum Indicator."""
    # ✅ اصلاح: استفاده از نام‌های اصلی ستون‌ها
    close = pd.to_numeric(df['close'].squeeze(), errors='coerce')
    high = pd.to_numeric(df['high'].squeeze(), errors='coerce')
    low = pd.to_numeric(df['low'].squeeze(), errors='coerce')
    
    # Bollinger Bands
    bb_ma = close.rolling(window=bb_window).mean()
    bb_std_dev = close.rolling(window=bb_window).std()
    bb_upper = bb_ma + (bb_std_dev * bb_std)
    bb_lower = bb_ma - (bb_std_dev * bb_std)

    # Keltner Channels
    tr = pd.concat([(high - low), abs(high - close.shift()), abs(low - close.shift())], axis=1).max(axis=1)
    atr = tr.rolling(window=kc_window).mean()
    kc_ma = close.rolling(window=kc_window).mean()
    kc_upper = kc_ma + (atr * kc_mult)
    kc_lower = kc_ma - (atr * kc_mult)
    
    # Squeeze condition (خروجی Boolean)
    # 💡 اصلاح عبارت اصلی: فرض بر این است که Squeeze زمانی روشن می‌شود که Bollinger Bands درون Keltner Channels قرار گیرد.
    squeeze_on = (bb_lower > kc_lower) & (bb_upper < kc_upper)

    # Momentum
    highest_high = high.rolling(window=bb_window).max()
    lowest_low = low.rolling(window=bb_window).min()
    avg_hl = (highest_high + lowest_low) / 2
    avg_close = close.rolling(window=bb_window).mean()
    momentum = (close - (avg_hl + avg_close) / 2)
    
    momentum_smoothed = momentum.rolling(window=bb_window).apply(lambda x: np.polyfit(np.arange(len(x)), x, 1)[0], raw=True)

    # تبدیل صریح Boolean به Integer و reindex برای حفظ تراز
    return squeeze_on.astype(int).reindex(df.index), momentum_smoothed.reindex(df.index)

def calculate_halftrend(df: pd.DataFrame, amplitude=2, channel_deviation=2) -> Tuple[pd.Series, pd.Series]:
    """محاسبه اندیکاتور HalfTrend."""
    try:
        # ✅ اصلاح: استفاده از نام‌های اصلی ستون‌ها
        high = pd.to_numeric(df['high'].squeeze(), errors='coerce')
        low = pd.to_numeric(df['low'].squeeze(), errors='coerce')
        close = pd.to_numeric(df['close'].squeeze(), errors='coerce')

        # منطق calculate_atr
        tr1 = high - low
        tr2 = abs(high - close.shift())
        tr3 = abs(low - close.shift())
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        # استفاده از 100 به جای پریود پیش فرض (بر اساس کد اصلی شما)
        atr = tr.rolling(window=100).mean() / 2 

        high_price = high.rolling(window=amplitude).max()
        low_price = low.rolling(window=amplitude).min()
        
        # منطق calculate_sma
        highma = high_price.rolling(window=amplitude).mean()
        lowma = low_price.rolling(window=amplitude).mean()

        # آماده سازی ستون‌های موقت
        trend_list = [0] * len(df)
        next_trend_list = [0] * len(df)
        
        # FIX: اطمینان از تبدیل به لیست‌های ساده عددی (بدون Series)
        close_list = close.to_list()
        lowma_list = lowma.to_list()
        highma_list = highma.to_list()
        
        for i in range(1, len(df)):
            # مدیریت مقادیر NaN در MAها با استفاده از قیمت بسته‌شدن روز قبل (fallback)
            prev_lowma = lowma_list[i-1] if i > 0 and not pd.isna(lowma_list[i-1]) else close_list[i-1] if i > 0 else close_list[i]
            prev_highma = highma_list[i-1] if i > 0 and not pd.isna(highma_list[i-1]) else close_list[i-1] if i > 0 else close_list[i]
            
            if next_trend_list[i-1] == 1:
                if close_list[i] < prev_lowma:
                    trend_list[i] = -1
                else:
                    trend_list[i] = 1
            else:
                if close_list[i] > prev_highma:
                    trend_list[i] = 1
                else:
                    trend_list[i] = -1

            if trend_list[i] == trend_list[i-1]:
                next_trend_list[i] = trend_list[i-1]
            else:
                next_trend_list[i] = trend_list[i]

        df['trend'] = pd.Series(trend_list, index=df.index, dtype=int)
        
        halftrend_signal = df['trend'].reindex(df.index)
        
        # خروجی (سیگنال نهایی و روند کامل)
        return halftrend_signal, halftrend_signal 

    except Exception as e:
        logger.error(f"خطای بحرانی در پردازش HalfTrend برای یک نماد: {e}", exc_info=True)
        nan_series = pd.Series([np.nan] * len(df), index=df.index)
        return nan_series, nan_series

def calculate_support_resistance_break(df: pd.DataFrame, window=50) -> Tuple[pd.Series, pd.Series]:
    """محاسبه ساده شکست مقاومت."""
    # ✅ اصلاح: استفاده از نام‌های اصلی ستون‌ها
    close = pd.to_numeric(df['close'].squeeze(), errors='coerce')
    high = pd.to_numeric(df['high'].squeeze(), errors='coerce')
    
    resistance = high.shift(1).rolling(window=window).max()
    
    # شکست زمانی رخ می‌دهد که قیمت پایانی امروز بالاتر از مقاومت دیروز باشد (خروجی Boolean)
    resistance_broken = close > resistance
    
    # تبدیل صریح Boolean به Integer و اطمینان از خروجی float برای resistance
    resistance_broken_int = resistance_broken.astype(int)
    
    return resistance.astype(float).reindex(df.index), resistance_broken_int.reindex(df.index)


# ----------------------------
# توابع اصلی تحلیل تکنیکال
# ----------------------------
def calculate_all_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """
    محاسبه تمام اندیکاتورهای تکنیکال مورد نیاز و اضافه کردن آنها به DataFrame.
    """
    
    # اطمینان از اینکه دیتافریم خالی نیست و دارای ستون‌های ضروری است
    if df.empty or not {'open', 'high', 'low', 'close', 'volume'}.issubset(df.columns):
        logger.warning("DataFrame خالی است یا ستون‌های لازم را ندارد.")
        return df

    try: # <--- شروع بلوک try اصلی
        # تبدیل ستون‌ها به نوع عددی و حذف مقادیر نامعتبر
        for col in ['open', 'high', 'low', 'close', 'volume']:
            if not pd.api.types.is_numeric_dtype(df[col]):
                df[col] = pd.to_numeric(df[col], errors='coerce')
        df.dropna(subset=['open', 'high', 'low', 'close', 'volume'], inplace=True)

        if df.empty:
            logger.warning("پس از تبدیل و پاکسازی، دیتای معتبری برای محاسبه اندیکاتورها باقی نماند.")
            return df
        
        # --- محاسبات اندیکاتورهای استاندارد ---
        df['RSI'] = calculate_rsi(df['close'])
        
        macd, signal, histogram = calculate_macd(df['close'])
        df['MACD'] = macd
        df['MACD_Signal'] = signal
        df['MACD_Histogram'] = histogram
        
        df['SMA_20'] = calculate_sma(df['close'], 20)
        df['SMA_50'] = calculate_sma(df['close'], 50)
        
        upper, middle, lower = calculate_bollinger_bands(df['close'])
        df['Bollinger_Upper'] = upper
        df['Bollinger_Middle'] = middle
        df['Bollinger_Lower'] = lower
        
        df['Volume_MA_20'] = calculate_volume_ma(df['volume'], 20)
        df['ATR'] = calculate_atr(df['high'], df['low'], df['close'])

        # --- محاسبات اندیکاتورهای جدید ---
        # 1. محاسبه Stochastic
        stochastic_k, stochastic_d = calculate_stochastic(df['high'], df['low'], df['close'])
        df['Stochastic_K'] = stochastic_k
        df['Stochastic_D'] = stochastic_d
        
        # 3. محاسبه اندیکاتورهای جدید با توابع اصلاح شده
        try: # <--- بلوک try داخلی
            # توابع اصلاح شده اکنون از نام‌های 'close', 'high', 'low' استفاده می‌کنند.
            squeeze_on, _ = calculate_squeeze_momentum(df)
            df['squeeze_on'] = squeeze_on
            
            halftrend_signal, _ = calculate_halftrend(df)
            df['halftrend_signal'] = halftrend_signal
            
            resistance_level, resistance_broken = calculate_support_resistance_break(df)
            df['resistance_level_50d'] = resistance_level
            df['resistance_broken'] = resistance_broken
        
        except Exception as e: # <--- بلوک except داخلی
            logger.error(f"❌ خطا در محاسبه اندیکاتورها در تابع calculate_all_indicators (داخلی): {e}", exc_info=True)
            # در اینجا ادامه کار با اندیکاتورهای استاندارد محاسبه شده مناسب‌تر است، پس df را برمی‌گردانیم.
            pass # اجازه می‌دهیم تابع به مسیر اصلی خود ادامه دهد و df را برگرداند.

    except Exception as e: # <--- بلوک except اصلی که مورد نیاز بود!
        logger.error(f"❌ خطای بحرانی در پردازش داده‌ها یا محاسبه اندیکاتورهای استاندارد: {e}", exc_info=True)
        return df
        
    return df # <--- خروجی نهایی


# ----------------------------
# تابع CandlestickPatternDetection
# ----------------------------
def run_candlestick_detection(db_session: Session, limit: int = None, symbols_list: list = None):
    """
    اجرای تشخیص الگوهای شمعی برای نمادها با استفاده از داده‌های تاریخی و ذخیره نتایج.
    از استراتژی حذف و درج (Delete & Insert) برای جلوگیری از تکرار استفاده می‌کند.
    (اصلاح شده برای رفع خطای MemoryError با پردازش نماد به نماد)
    """
    from datetime import datetime
    import pandas as pd
    
    # مدل‌ها و Logger فرض بر import بودن هستند

    try:
        logger.info("🕯️ شروع تشخیص الگوهای شمعی...")
        
        # 1. دریافت لیست symbol_id های فعال (از ComprehensiveSymbolData یا HistoricalData)
        # برای جلوگیری از فچ کل داده‌ها، ابتدا لیست نمادهایی که باید پردازش شوند را دریافت می‌کنیم.
        
        # ⚠️ کوئری برای دریافت symbol_idهای موجود در HistoricalData
        base_query = db_session.query(HistoricalData.symbol_id).distinct()
        
        if symbols_list:
            base_query = base_query.filter(HistoricalData.symbol_id.in_(symbols_list)) 
            
        symbol_ids_to_process = [str(s[0]) for s in base_query.all()]
        
        if not symbol_ids_to_process:
            logger.warning("⚠️ هیچ نمادی برای تشخیص الگوهای شمعی یافت نشد.")
            return 0
            
        logger.info(f"🔍 یافت شد {len(symbol_ids_to_process)} نماد برای تشخیص الگوهای شمعی")

        success_count = 0
        records_to_insert = []
        
        # 2. حلقه زدن روی هر نماد و فچ و پردازش جداگانه
        processed_count = 0
        for symbol_id in symbol_ids_to_process:
            if limit is not None and processed_count >= limit:
                break
            
            try:
                # 💡 نقطه کلیدی اصلاح شده: فچ داده‌های تاریخی فقط برای یک نماد
                # برای تشخیص الگوهای شمعی حداکثر به چند روز اخیر نیاز داریم (مثلاً 10 روز آخر).
                # این کار از بارگذاری کل دیتابیس در RAM جلوگیری می‌کند.
                
                historical_data_query = db_session.query(HistoricalData).filter(
                    HistoricalData.symbol_id == symbol_id
                ).order_by(HistoricalData.date.desc()).limit(30) # 👈 فچ محدود به ۳۰ روز اخیر
                
                # 💥 اکنون این کوئری فقط حدود ۳۰ رکورد برمی‌گرداند، نه کل تاریخچه.
                historical_data = historical_data_query.all() 
                
                if len(historical_data) < 5: 
                    continue 

                # 💡 تبدیل به DataFrame
                df = pd.DataFrame([row.__dict__ for row in historical_data])
                if '_sa_instance_state' in df.columns:
                    df = df.drop(columns=['_sa_instance_state']) 
                
                # مرتب‌سازی برای اطمینان از اینکه ioc[-1] روز جدید است (بر اساس date صعودی)
                df.sort_values(by='date', inplace=True) 

                # استخراج داده‌های لازم:
                today_record_dict = df.iloc[-1].to_dict()
                yesterday_record_dict = df.iloc[-2].to_dict()
                
                # فراخوانی تابع تشخیص الگو:
                patterns = check_candlestick_patterns(
                    today_record_dict, 
                    yesterday_record_dict, 
                    df # کل DataFrame محدود شده (مثلاً ۳۰ روزه)
                )
                
                # ذخیره الگوهای یافت‌شده
                if patterns:
                    now = datetime.now()
                    current_jdate = today_record_dict['jdate']
                    for pattern in patterns:
                        records_to_insert.append({
                            'symbol_id': symbol_id,
                            'jdate': current_jdate,
                            'pattern_name': pattern,
                            'created_at': now, 
                            'updated_at': now
                        })
                    success_count += 1
                
                processed_count += 1
                if processed_count % 50 == 0:
                    logger.info(f"🕯️ پیشرفت تشخیص الگوهای شمعی: {processed_count}/{len(symbol_ids_to_process)} نماد")

            except Exception as e:
                logger.error(f"❌ خطا در تشخیص الگوهای شمعی برای نماد {symbol_id}: {e}", exc_info=True)
                # بدون rollback در این سطح (زیرا bulk insert بعداً انجام می‌شود)
                
        logger.info(f"✅ تشخیص الگوهای شمعی برای {success_count} نماد با الگو انجام شد.")
                
        # 3. ذخیره نتایج در دیتابیس (استراتژی Delete & Insert - بدون تغییر)
        if records_to_insert:
            # الف) استخراج تاریخ و لیست نمادهای پردازش شده
            last_jdate = records_to_insert[0]['jdate'] 
            processed_symbol_ids = list({record['symbol_id'] for record in records_to_insert})
            
            # ب) حذف رکوردهای قدیمی
            try:
                db_session.query(CandlestickPatternDetection).filter(
                    CandlestickPatternDetection.symbol_id.in_(processed_symbol_ids),
                    CandlestickPatternDetection.jdate == last_jdate
                ).delete(synchronize_session=False) 
                
                db_session.commit()
                logger.info(f"🗑️ الگوهای شمعی قبلی ({len(processed_symbol_ids)} نماد) برای {last_jdate} حذف شدند.")
                
            except Exception as e:
                db_session.rollback()
                logger.error(f"❌ خطا در حذف رکوردهای قدیمی الگوهای شمعی: {e}", exc_info=True)
                return success_count
                
            # ج) درج رکوردهای جدید
            db_session.bulk_insert_mappings(CandlestickPatternDetection, records_to_insert)
            db_session.commit()
            logger.info(f"✅ {len(records_to_insert)} الگوی شمعی با موفقیت درج شد.")
        else:
            logger.info("ℹ️ هیچ الگوی شمعی جدیدی یافت نشد.")

        return success_count

    except Exception as e:
        logger.error(f"❌ خطای کلی در اجرای تشخیص الگوهای شمعی: {e}", exc_info=True)
        db_session.rollback()
        return 0



# ----------------------------
# توابع آپدیت داده‌های بنیادی (برای سازگاری)
# ----------------------------

def update_comprehensive_symbol_data(db_session: Session, symbols_list: list = None):
    """
    آپدیت داده‌های بنیادی نمادها
    """
    try:
        logger.info("📊 شروع آپدیت داده‌های بنیادی...")
        
        query = db_session.query(ComprehensiveSymbolData)
        if symbols_list:
            # اگر لیست شامل idهای ComprehensiveSymbolData است
            if all(isinstance(x, int) for x in symbols_list):
                query = query.filter(ComprehensiveSymbolData.id.in_(symbols_list))
            # اگر لیست شامل نام نمادها است
            else:
                query = query.filter(ComprehensiveSymbolData.symbol_name.in_(symbols_list))
            
        symbols = query.all()
        
        processed_count = 0
        for symbol in symbols:
            try:
                # اصلاحیه: استفاده از symbol_name و symbol_index
                fundamental_data = fetch_fundamental_data(symbol.symbol_name, symbol.tse_index) # در این تابع ورودی را اصلاح کنید.
                
                if fundamental_data:
                    # ذخیره داده‌های بنیادی
                    save_fundamental_data(db_session, symbol.id, fundamental_data)
                    processed_count += 1
                    
            except Exception as e:
                logger.error(f"❌ خطا در آپدیت داده‌های بنیادی برای نماد {symbol.symbol_name}: {e}")
        
        logger.info(f"✅ آپدیت داده‌های بنیادی کامل شد. {processed_count} نماد پردازش شد.")
        
    except Exception as e:
        logger.error(f"❌ خطا در اجرای آپدیت بنیادی: {e}")

def fetch_fundamental_data(symbol_name: str, symbol_index: str) -> dict:
    """
    دریافت داده‌های بنیادی از pytse-client
    """
    try:
        # اینجا از symbol_name و symbol_index استفاده می‌کنید
        ticker = tse.Ticker(symbol_name, index=symbol_index)
        
        fundamental_data = {
            'p_e': ticker.p_e_ratio,
            'eps': ticker.eps,
            'p_s': ticker.p_s_ratio,
            'p_b': ticker.p_b_ratio,
            'dividend_yield': ticker.dividend_yield,
            'market_cap': ticker.market_cap,
            'shares_outstanding': ticker.shares_outstanding,
            'float_shares': ticker.float_shares,
            'base_volume': ticker.base_volume,
            'sector_pe': ticker.sector_pe,
            'group_pe': ticker.group_pe,
            'sector_pb': ticker.sector_pb,
            'group_pb': ticker.group_pb,
            'sector_eps': ticker.sector_eps,
            'group_eps': ticker.group_eps,
            'sector_dividend_yield': ticker.sector_dividend_yield,
            'group_dividend_yield': ticker.group_dividend_yield
        }
        
        return fundamental_data
        
    except Exception as e:
        logger.error(f"❌ خطا در دریافت داده‌های بنیادی برای {symbol_name}: {e}")
        return {}

def save_fundamental_data(db_session: Session, symbol_id: int, fundamental_data: dict):
    """
    ذخیره داده‌های بنیادی در دیتابیس
    """
    try:
        existing_data = db_session.query(FundamentalData).filter_by(symbol_id=symbol_id).first()
        
        if existing_data:
            # آپدیت داده موجود
            for key, value in fundamental_data.items():
                if hasattr(existing_data, key):
                    setattr(existing_data, key, value)
        else:
            # ایجاد داده جدید
            new_data = FundamentalData(
                symbol_id=symbol_id,
                p_e=fundamental_data.get('p_e'),
                eps=fundamental_data.get('eps'),
                p_s=fundamental_data.get('p_s'),
                p_b=fundamental_data.get('p_b'),
                dividend_yield=fundamental_data.get('dividend_yield'),
                market_cap=fundamental_data.get('market_cap'),
                shares_outstanding=fundamental_data.get('shares_outstanding'),
                float_shares=fundamental_data.get('float_shares'),
                base_volume=fundamental_data.get('base_volume'),
                sector_pe=fundamental_data.get('sector_pe'),
                group_pe=fundamental_data.get('group_pe'),
                sector_pb=fundamental_data.get('sector_pb'),
                group_pb=fundamental_data.get('group_pb'),
                sector_eps=fundamental_data.get('sector_eps'),
                group_eps=fundamental_data.get('group_eps'),
                sector_dividend_yield=fundamental_data.get('sector_dividend_yield'),
                group_dividend_yield=fundamental_data.get('group_dividend_yield')
            )
            db_session.add(new_data)
            
        db_session.commit()
        
    except Exception as e:
        logger.error(f"❌ خطا در ذخیره داده‌های بنیادی: {e}")
        db_session.rollback()


def save_technical_indicators(db_session: Session, symbol_id: int, df: pd.DataFrame):
    """
    ذخیره (درج یا به‌روزرسانی) نتایج تحلیل تکنیکال محاسبه شده در جدول TechnicalIndicatorData.
    """
    # تبدیل symbol_id به رشته
    symbol_id_str = str(symbol_id)

    # تضمین وجود ستون symbol_id
    if 'symbol_id' not in df.columns:
        df['symbol_id'] = symbol_id_str

    # حذف تکراری‌ها فقط داخل DataFrame (ضروری برای اطمینان از صحت داده‌ها)
    # استفاده از .copy() برای جلوگیری از SettingWithCopyWarning
    df_unique = df.drop_duplicates(subset=['symbol_id', 'jdate'], keep='last').copy()
    df_to_save = df_unique.dropna(subset=['RSI', 'MACD'])

    if df_to_save.empty:
        logger.debug(f"⚠️ پس از پاکسازی، هیچ سطر معتبری برای ذخیره اندیکاتورهای نماد {symbol_id_str} وجود نداشت.")
        return
    
    updates_count = 0
    inserts_count = 0
    
    try: # بلوک try/except باید خارج از حلقه باشد تا commit یکپارچه انجام شود
        for _, row in df_to_save.iterrows():
            # 1. جستجو برای رکورد موجود
            existing = db_session.query(TechnicalIndicatorData).filter_by(
                symbol_id=symbol_id_str,
                jdate=row['jdate']
            ).first()

            # 2. منطق درج یا به‌روزرسانی (Upsert)
            if existing:
                # ✅ Update رکورد موجود
                existing.close_price = row.get('close')
                existing.RSI = row.get('RSI')
                existing.MACD = row.get('MACD')
                existing.MACD_Signal = row.get('MACD_Signal')
                existing.MACD_Hist = row.get('MACD_Histogram')
                existing.SMA_20 = row.get('SMA_20')
                existing.SMA_50 = row.get('SMA_50')
                existing.Bollinger_High = row.get('Bollinger_Upper')
                existing.Bollinger_Low = row.get('Bollinger_Lower')
                existing.Bollinger_MA = row.get('Bollinger_Middle')
                existing.Volume_MA_20 = row.get('Volume_MA_20')
                existing.ATR = row.get('ATR')
                existing.Stochastic_K = row.get('Stochastic_K')
                existing.Stochastic_D = row.get('Stochastic_D')
                existing.squeeze_on = bool(row.get('squeeze_on'))
                existing.halftrend_signal = row.get('halftrend_signal')
                existing.resistance_level_50d = row.get('resistance_level_50d')
                existing.resistance_broken = bool(row.get('resistance_broken'))
                
                # توجه: اگر مدل شما `updated_at` را به صورت خودکار به‌روز نمی‌کند، باید آن را اینجا به‌روز کنید
                # existing.updated_at = datetime.now() 
                
                updates_count += 1
            else:
                # ✅ Insert رکورد جدید
                indicator = TechnicalIndicatorData(
                    symbol_id=symbol_id_str,
                    jdate=row['jdate'],
                    close_price=row.get('close'),
                    RSI=row.get('RSI'),
                    MACD=row.get('MACD'),
                    MACD_Signal=row.get('MACD_Signal'),
                    MACD_Hist=row.get('MACD_Histogram'),
                    SMA_20=row.get('SMA_20'),
                    SMA_50=row.get('SMA_50'),
                    Bollinger_High=row.get('Bollinger_Upper'),
                    Bollinger_Low=row.get('Bollinger_Lower'),
                    Bollinger_MA=row.get('Bollinger_Middle'),
                    Volume_MA_20=row.get('Volume_MA_20'),
                    ATR=row.get('ATR'),
                    Stochastic_K=row.get('Stochastic_K'),
                    Stochastic_D=row.get('Stochastic_D'),
                    squeeze_on=bool(row.get('squeeze_on')),
                    halftrend_signal=row.get('halftrend_signal'),
                    resistance_level_50d=row.get('resistance_level_50d'),
                    resistance_broken=bool(row.get('resistance_broken'))
                )
                db_session.add(indicator)
                inserts_count += 1

        # Commit کردن تراکنش پس از اتمام حلقه
        db_session.commit()
        logger.info(f"✅ اندیکاتورهای نماد {symbol_id_str} با موفقیت ذخیره/بروزرسانی شدند. (درج: {inserts_count}، بروزرسانی: {updates_count})")
        
    except Exception as e:
        db_session.rollback()
        # 💡 این خطا اکنون فقط در صورتی رخ می‌دهد که دو رشته همزمان (concurrent processes) بخواهند یک رکورد را درج کنند
        logger.error(f"❌ خطا در ذخیره اندیکاتورها برای نماد {symbol_id_str}: {e}")



# ----------------------------
# توابع مدیریت حافظه و بهینه‌سازی
# ----------------------------

def cleanup_memory():
    """پاکسازی حافظه"""
    try:
        gc.collect()
        current_memory = check_memory_usage_mb()
        if current_memory > MEMORY_LIMIT_MB:
            logger.warning(f"⚠️ مصرف حافظه بالا: {current_memory:.2f} MB")
    except Exception as e:
        logger.debug(f"خطا در پاکسازی حافظه: {e}")

def batch_process_symbols(symbols: list, process_func: callable, batch_size: int = DEFAULT_BATCH_SIZE):
    """پردازش دسته‌ای نمادها"""
    results = []
    for i in range(0, len(symbols), batch_size):
        batch = symbols[i:i + batch_size]
        batch_results = []
        
        for symbol in batch:
            try:
                result = process_func(symbol)
                batch_results.append(result)
            except Exception as e:
                logger.error(f"❌ خطا در پردازش نماد {symbol}: {e}")
                batch_results.append(None)
                
        results.extend(batch_results)
        
        # پاکسازی حافظه بعد از هر بچ
        cleanup_memory()
        
    return results

# ----------------------------
# توابع گزارش‌گیری و مانیتورینگ
# ----------------------------

def get_data_status_report(db_session: Session) -> dict:
    """
    گزارش وضعیت داده‌های موجود در دیتابیس
    """
    try:
        total_symbols = db_session.query(ComprehensiveSymbolData).count()
        symbols_with_historical = db_session.query(
            func.count(distinct(HistoricalData.symbol_id))
        ).scalar()
        symbols_with_technical = db_session.query(
            func.count(distinct(TechnicalIndicatorData.symbol_id))
        ).scalar()
        symbols_with_fundamental = db_session.query(
            func.count(distinct(FundamentalData.symbol_id))
        ).scalar()
        
        total_historical_records = db_session.query(HistoricalData).count()
        total_technical_records = db_session.query(TechnicalIndicatorData).count()
        
        latest_historical_date = db_session.query(
            func.max(HistoricalData.date)
        ).scalar()
        
        return {
            'total_symbols': total_symbols,
            'symbols_with_historical': symbols_with_historical,
            'symbols_with_technical': symbols_with_technical,
            'symbols_with_fundamental': symbols_with_fundamental,
            'total_historical_records': total_historical_records,
            'total_technical_records': total_technical_records,
            'latest_historical_date': latest_historical_date,
            'historical_coverage': f"{symbols_with_historical}/{total_symbols}",
            'technical_coverage': f"{symbols_with_technical}/{total_symbols}",
            'fundamental_coverage': f"{symbols_with_fundamental}/{total_symbols}"
        }
        
    except Exception as e:
        logger.error(f"❌ خطا در تهیه گزارش وضعیت: {e}")
        return {}

def check_data_consistency(db_session: Session) -> dict:
    """
    بررسی سازگاری داده‌ها در دیتابیس
    """
    try:
        # بررسی نمادهایی که داده تاریخی دارند اما تحلیل تکنیکال ندارند
        symbols_missing_technical = db_session.query(ComprehensiveSymbolData).filter(
            ComprehensiveSymbolData.id.in_(
                db_session.query(HistoricalData.symbol_id).distinct()
            ),
            ~ComprehensiveSymbolData.id.in_(
                db_session.query(TechnicalIndicatorData.symbol_id).distinct()
            )
        ).count()
        
        # بررسی نمادهایی که داده تاریخی دارند اما داده بنیادی ندارند
        symbols_missing_fundamental = db_session.query(ComprehensiveSymbolData).filter(
            ComprehensiveSymbolData.id.in_(
                db_session.query(HistoricalData.symbol_id).distinct()
            ),
            ~ComprehensiveSymbolData.id.in_(
                db_session.query(FundamentalData.symbol_id).distinct()
            )
        ).count()
        
        # بررسی داده‌های تکراری
        duplicate_historical = db_session.query(
            HistoricalData.symbol_id,
            HistoricalData.date,
            func.count('*')
        ).group_by(
            HistoricalData.symbol_id,
            HistoricalData.date
        ).having(func.count('*') > 1).count()
        
        return {
            'symbols_missing_technical': symbols_missing_technical,
            'symbols_missing_fundamental': symbols_missing_fundamental,
            'duplicate_historical_records': duplicate_historical,
            'issues_found': symbols_missing_technical + symbols_missing_fundamental + duplicate_historical > 0
        }
        
    except Exception as e:
        logger.error(f"❌ خطا در بررسی سازگاری داده‌ها: {e}")
        return {}

# ----------------------------
# توابع بازیابی و تعمیر داده‌ها
# ----------------------------

def repair_missing_data(db_session: Session, data_type: str = 'all', limit: int = 50):
    """
    تعمیر داده‌های از دست رفته
    """
    try:
        logger.info(f"🔧 شروع تعمیر داده‌های از دست رفته ({data_type})...")
        
        if data_type in ['historical', 'all']:
            # تعمیر داده‌های تاریخی
            symbols_missing_historical = db_session.query(ComprehensiveSymbolData).filter(
                ~ComprehensiveSymbolData.id.in_(
                    db_session.query(HistoricalData.symbol_id).distinct()
                )
            ).limit(limit).all()
            
            if symbols_missing_historical:
                # استفاده از idهای ComprehensiveSymbolData
                symbol_ids = [s.id for s in symbols_missing_historical]
                processed_count, msg = fetch_and_process_historical_data(
                    db_session,
                    limit_per_run=limit,
                    specific_symbols_list=symbol_ids
                )
                logger.info(f"✅ تعمیر داده‌های تاریخی: {msg}")
        
        if data_type in ['technical', 'all']:
            # تعمیر داده‌های تکنیکال
            symbols_missing_technical = db_session.query(ComprehensiveSymbolData).filter(
                ComprehensiveSymbolData.id.in_(
                    db_session.query(HistoricalData.symbol_id).distinct()
                ),
                ~ComprehensiveSymbolData.id.in_(
                    db_session.query(TechnicalIndicatorData.symbol_id).distinct()
                )
            ).limit(limit).all()
            
            if symbols_missing_technical:
                # استفاده از idهای ComprehensiveSymbolData
                symbol_ids = [s.id for s in symbols_missing_technical]
                run_technical_analysis(
                    db_session,
                    limit=limit,
                    symbols_list=symbol_ids
                )
                logger.info(f"✅ تعمیر داده‌های تکنیکال برای {len(symbols_missing_technical)} نماد")
        
        if data_type in ['fundamental', 'all']:
            # تعمیر داده‌های بنیادی
            symbols_missing_fundamental = db_session.query(ComprehensiveSymbolData).filter(
                ComprehensiveSymbolData.id.in_(
                    db_session.query(HistoricalData.symbol_id).distinct()
                ),
                ~ComprehensiveSymbolData.id.in_(
                    db_session.query(FundamentalData.symbol_id).distinct()
                )
            ).limit(limit).all()
            
            if symbols_missing_fundamental:
                # استفاده از idهای ComprehensiveSymbolData
                symbol_ids = [s.id for s in symbols_missing_fundamental]
                update_comprehensive_symbol_data(
                    db_session,
                    symbols_list=symbol_ids
                )
                logger.info(f"✅ تعمیر داده‌های بنیادی برای {len(symbols_missing_fundamental)} نماد")
                
        logger.info("✅ تعمیر داده‌ها کامل شد.")
        
    except Exception as e:
        logger.error(f"❌ خطا در تعمیر داده‌ها: {e}")
        raise

def cleanup_duplicate_data(db_session: Session):
    """
    پاکسازی داده‌های تکراری
    """
    try:
        logger.info("🧹 شروع پاکسازی داده‌های تکراری...")
        
        # حذف داده‌های تاریخی تکراری
        duplicate_historical = db_session.query(
            HistoricalData.symbol_id,
            HistoricalData.date
        ).group_by(
            HistoricalData.symbol_id,
            HistoricalData.date
        ).having(func.count('*') > 1).all()
        
        for symbol_id, date in duplicate_historical:
            # نگه داشتن تنها اولین رکورد
            records_to_keep = db_session.query(HistoricalData).filter_by(
                symbol_id=symbol_id,
                date=date
            ).order_by(HistoricalData.id).first()
            
            if records_to_keep:
                db_session.query(HistoricalData).filter_by(
                    symbol_id=symbol_id,
                    date=date
                ).filter(HistoricalData.id != records_to_keep.id).delete()
        
        db_session.commit()
        logger.info(f"✅ پاکسازی داده‌های تکراری کامل شد. {len(duplicate_historical)} رکورد تکراری حذف شد.")
        
    except Exception as e:
        logger.error(f"❌ خطا در پاکسازی داده‌های تکراری: {e}")
        db_session.rollback()

# ----------------------------
# توابع اصلی برای endpointها
# ----------------------------

def run_full_update_with_limits(limit_per_run: int = 100, days_limit: int = 365):
    """
    اجرای آپدیت کامل با محدودیت‌های مشخص
    """
    try:
        session = get_session_local()
        try:
            run_full_data_update(
                session,
                limit_per_run=limit_per_run,
                days_limit=days_limit
            )
            return True, "آپدیت کامل با موفقیت انجام شد"
        finally:
            session.close()
    except Exception as e:
        logger.error(f"❌ خطا در اجرای آپدیت کامل: {e}")
        return False, f"خطا در آپدیت کامل: {str(e)}"

def run_historical_update_only(limit_per_run: int = 100, days_limit: int = 365):
    """
    اجرای تنها آپدیت داده‌های تاریخی
    """
    try:
        session = get_session_local()
        try:
            processed_count, msg = fetch_and_process_historical_data(
                session,
                limit_per_run=limit_per_run,
                days_limit=days_limit
            )
            return True, msg
        finally:
            session.close()
    except Exception as e:
        logger.error(f"❌ خطا در آپدیت داده‌های تاریخی: {e}")
        return False, f"خطا در آپدیت داده‌های تاریخی: {str(e)}"

def run_technical_update_only(limit: int = 100):
    """
    اجرای تنها آپدیت تحلیل تکنیکال
    """
    try:
        session = get_session_local()
        try:
            run_technical_analysis(session, limit=limit)
            return True, "آپدیت تحلیل تکنیکال با موفقیت انجام شد"
        finally:
            session.close()
    except Exception as e:
        logger.error(f"❌ خطا در آپدیت تحلیل تکنیکال: {e}")
        return False, f"خطا در آپدیت تحلیل تکنیکال: {str(e)}"

def run_fundamental_update_only():
    """
    اجرای تنها آپدیت داده‌های بنیادی
    """
    try:
        session = get_session_local()
        try:
            update_comprehensive_symbol_data(session)
            return True, "آپدیت داده‌های بنیادی با موفقیت انجام شد"
        finally:
            session.close()
    except Exception as e:
        logger.error(f"❌ خطا در آپدیت داده‌های بنیادی: {e}")
        return False, f"خطا در آپدیت داده‌های بنیادی: {str(e)}"

def get_status_report():
    """
    دریافت گزارش وضعیت
    """
    try:
        session = get_session_local()
        try:
            status = get_data_status_report(session)
            consistency = check_data_consistency(session)
            
            report = {
                'status': status,
                'consistency': consistency,
                'timestamp': datetime.now().isoformat(),
                'memory_usage_mb': check_memory_usage_mb()
            }
            
            return True, report
        finally:
            session.close()
    except Exception as e:
        logger.error(f"❌ خطا در دریافت گزارش وضعیت: {e}")
        return False, f"خطا در دریافت گزارش وضعیت: {str(e)}"

def run_data_repair(data_type: str = 'all', limit: int = 50):
    """
    اجرای تعمیر داده‌ها
    """
    try:
        session = get_session_local()
        try:
            repair_missing_data(session, data_type=data_type, limit=limit)
            return True, "تعمیر داده‌ها با موفقیت انجام شد"
        finally:
            session.close()
    except Exception as e:
        logger.error(f"❌ خطا در تعمیر داده‌ها: {e}")
        return False, f"خطا در تعمیر داده‌ها: {str(e)}"

def run_cleanup_duplicates():
    """
    اجرای پاکسازی داده‌های تکراری
    """
    try:
        session = get_session_local()
        try:
            cleanup_duplicate_data(session)
            return True, "پاکسازی داده‌های تکراری با موفقیت انجام شد"
        finally:
            session.close()
    except Exception as e:
        logger.error(f"❌ خطا در پاکسازی داده‌های تکراری: {e}")
        return False, f"خطا در پاکسازی داده‌های تکراری: {str(e)}"

# ----------------------------
# توابع کمکی برای مدیریت نمادها
# ----------------------------

def add_single_symbol(db_session: Session, symbol_name: str) -> Tuple[bool, str]:
    """
    اضافه کردن یک نماد جدید به دیتابیس و دریافت داده‌های اولیه آن.
    """
    logger.info(f"📥 تلاش برای افزودن نماد {symbol_name}...")
    
    try:
        # گام ۱: دریافت اطلاعات نماد اصلی و بررسی اعتبار
        import pytse_client as tse
        ticker = tse.Ticker(symbol_name)
        
        tse_index = getattr(ticker, 'index', None)
        if not tse_index:
            return False, f"⚠️ نماد {symbol_name} شناسه بورس ندارد و نمی‌تواند اضافه شود."
            
        # بررسی وجود نماد در دیتابیس بر اساس tse_index
        existing_symbol = db_session.query(ComprehensiveSymbolData).filter_by(tse_index=tse_index).first()
        if existing_symbol:
            return False, f"ℹ️ نماد {symbol_name} از قبل در دیتابیس وجود دارد."

        # ایجاد نماد جدید با استفاده از tse_index به عنوان symbol_id
        now = datetime.now()
        new_symbol = ComprehensiveSymbolData(
            symbol_id=tse_index,
            tse_index=tse_index,
            symbol_name=symbol_name,
            company_name=getattr(ticker, 'title', ''),
            isin=getattr(ticker, 'isin', None),
            market_type=getattr(ticker, 'flow', ''),
            group_name=getattr(ticker, 'group_name', ''),
            base_volume=getattr(ticker, 'base_volume', None),
            eps=getattr(ticker, 'eps', None),
            p_e_ratio=getattr(ticker, 'p_e_ratio', None),
            p_s_ratio=getattr(ticker, 'p_s_ratio', None),
            nav=getattr(ticker, 'nav', None),
            float_shares=getattr(ticker, 'float_shares', None),
            market_cap=getattr(ticker, 'market_cap', None),
            industry=getattr(ticker, 'industry', None),
            capital=getattr(ticker, 'capital', None),
            fiscal_year=getattr(ticker, 'fiscal_year', None),
            flow=getattr(ticker, 'flow', None),
            state=getattr(ticker, 'state', None),
            last_historical_update_date=None,
            last_fundamental_update_date=None,
            last_realtime_update=None,
            created_at=now,
            updated_at=now
        )
        
        db_session.add(new_symbol)
        db_session.commit()
        
        logger.info(f"✅ نماد {symbol_name} با موفقیت به دیتابیس اضافه شد. شناسه: {new_symbol.symbol_id}")

        # گام ۲: دریافت و ذخیره داده‌های تاریخی با تابع اختصاصی
        hist_updated_count, hist_msg = fetch_and_process_historical_data(db_session, specific_symbols_list=[new_symbol.symbol_name])
        logger.info(f"📊 دیتای تاریخی برای {symbol_name}: {hist_updated_count} رکورد به‌روزرسانی شد. {hist_msg}")
        
        # گام ۳: دریافت و ذخیره داده‌های بنیادی با تابع اختصاصی
        fund_updated_count, fund_msg = update_symbol_fundamental_data(db_session, specific_symbols_list=[new_symbol.symbol_name])
        logger.info(f"📊 دیتای بنیادی برای {symbol_name}: {fund_updated_count} رکورد به‌روزرسانی شد. {fund_msg}")
        
        # گام ۴: اجرای تحلیل تکنیکال با استفاده از شناسه صحیح
        calculate_all_indicators(db_session, specific_symbols_list=[new_symbol.symbol_name])
        logger.info(f"📈 تحلیل تکنیکال برای نماد {symbol_name} اجرا شد.")
        
        return True, f"✅ نماد {symbol_name} و داده‌های اولیه آن با موفقیت اضافه شدند."

    except Exception as e:
        db_session.rollback()
        logger.error(f"❌ خطا در افزودن نماد {symbol_name}: {e}")
        return False, f"❌ خطا در افزودن نماد: {str(e)}"


def remove_symbol(symbol_name: str):
    """
    حذف یک نماد از دیتابیس
    """
    try:
        session = get_session_local()
        try:
            # پیدا کردن نماد در ComprehensiveSymbolData
            symbol = session.query(ComprehensiveSymbolData).filter_by(symbol_id=symbol_name).first()
            if not symbol:
                return False, "نماد یافت نشد"
            
            # حذف داده‌های وابسته
            session.query(HistoricalData).filter_by(symbol_id=symbol.id).delete()
            session.query(TechnicalIndicatorData).filter_by(symbol_id=symbol.id).delete()
            session.query(FundamentalData).filter_by(symbol_id=symbol.id).delete()
            
            # حذف خود نماد
            session.delete(symbol)
            session.commit()
            
            return True, f"نماد {symbol_name} با موفقیت حذف شد"
            
        finally:
            session.close()
    except Exception as e:
        logger.error(f"❌ خطا در حذف نماد {symbol_name}: {e}")
        return False, f"خطا در حذف نماد: {str(e)}"

def update_symbol_info(symbol_name: str):
    """
    آپدیت اطلاعات یک نماد
    """
    try:
        session = get_session_local()
        try:
            # پیدا کردن نماد در ComprehensiveSymbolData
            symbol = session.query(ComprehensiveSymbolData).filter_by(symbol_id=symbol_name).first()
            if not symbol:
                return False, "نماد یافت نشد"
            
            # دریافت اطلاعات به روز از pytse-client
            ticker = tse.Ticker(symbol_name)
            if not ticker:
                return False, "خطا در دریافت اطلاعات نماد"
            
            # آپدیت اطلاعات نماد
            symbol.symbol_name = getattr(ticker, 'title', symbol_name)
            symbol.company_name = getattr(ticker, 'company_name', symbol.company_name)
            symbol.market_type = getattr(ticker, 'market', symbol.market_type)
            symbol.group_name = getattr(ticker, 'group_name', symbol.group_name)
            symbol.base_volume = getattr(ticker, 'base_volume', symbol.base_volume)
            symbol.updated_at = datetime.now()
            
            session.commit()
            
            return True, f"اطلاعات نماد {symbol_name} با موفقیت آپدیت شد"
            
        finally:
            session.close()
    except Exception as e:
        logger.error(f"❌ خطا در آپدیت اطلاعات نماد {symbol_name}: {e}")
        return False, f"خطا در آپدیت اطلاعات نماد: {str(e)}"

# ----------------------------
# توابع فیلتر و جستجو
# ----------------------------

def search_symbols(query: str, limit: int = 20):
    """
    جستجوی نمادها بر اساس نام یا نماد
    """
    try:
        session = get_session_local()
        try:
            results = session.query(ComprehensiveSymbolData).filter(
                (ComprehensiveSymbolData.symbol_name.ilike(f"%{query}%")) |
                (ComprehensiveSymbolData.symbol_id.ilike(f"%{query}%")) |
                (ComprehensiveSymbolData.company_name.ilike(f"%{query}%"))
            ).limit(limit).all()
            
            symbols_list = []
            for symbol in results:
                symbols_list.append({
                    'id': symbol.id,
                    'symbol_id': symbol.symbol_id,
                    'symbol_name': symbol.symbol_name,
                    'company_name': symbol.company_name,
                    'market_type': symbol.market_type,
                    'created_at': symbol.created_at.isoformat() if symbol.created_at else None
                })
            
            return True, symbols_list
            
        finally:
            session.close()
    except Exception as e:
        logger.error(f"❌ خطا در جستجوی نمادها: {e}")
        return False, f"خطا در جستجوی نمادها: {str(e)}"


def filter_symbols_by_market(market: str, limit: int = 100):
    """
    فیلتر نمادها بر اساس بازار
    """
    try:
        session = get_session_local()
        try:
            results = session.query(ComprehensiveSymbolData).filter(
                ComprehensiveSymbolData.market_type.ilike(f"%{market}%")
            ).limit(limit).all()
            
            symbols_list = []
            for symbol in results:
                symbols_list.append({
                    'id': symbol.id,
                    'symbol_id': symbol.symbol_id,
                    'symbol_name': symbol.symbol_name,
                    'market_type': symbol.market_type,
                    'company_name': symbol.company_name
                })
            
            return True, symbols_list
            
        finally:
            session.close()
    except Exception as e:
        logger.error(f"❌ خطا در فیلتر نمادها: {e}")
        return False, f"خطا در فیلتر نمادها: {str(e)}"

    
def get_symbol_comprehensive_report(symbol_identifier: str):
    """
    دریافت گزارش جامع برای یک نماد (با استفاده از symbol_id یا symbol_name)
    """
    try:
        session = get_session_local()
        try:
            # جستجوی نماد بر اساس symbol_id یا symbol_name
            symbol = session.query(ComprehensiveSymbolData).filter(
                (ComprehensiveSymbolData.symbol_id == symbol_identifier) |
                (ComprehensiveSymbolData.symbol_name == symbol_identifier)
            ).first()
            
            if not symbol:
                return False, "نماد یافت نشد"
            
            # اطلاعات پایه
            symbol_info = {
                'id': symbol.id,
                'symbol_id': symbol.symbol_id,
                'symbol_name': symbol.symbol_name,
                'company_name': symbol.company_name,
                'market_type': symbol.market_type,
                'industry': symbol.industry,
                'group_name': symbol.group_name,
                'base_volume': symbol.base_volume,
                'created_at': symbol.created_at.isoformat() if symbol.created_at else None
            }
            
            # اطلاعات تاریخی
            historical_data = session.query(HistoricalData).filter_by(
                symbol_id=symbol.id
            ).order_by(HistoricalData.date.desc()).limit(30).all()
            
            historical_list = []
            for hist in historical_data:
                historical_list.append({
                    'date': hist.date.isoformat() if hist.date else None,
                    'jdate': hist.jdate,
                    'open': hist.open,
                    'high': hist.high,
                    'low': hist.low,
                    'close': hist.close,
                    'volume': hist.volume,
                    'value': hist.value
                })
            
            # اطلاعات تکنیکال
            technical_data = session.query(TechnicalIndicatorData).filter_by(
                symbol_id=symbol.id
            ).order_by(TechnicalIndicatorData.jdate.desc()).first()
            
            technical_info = {}
            if technical_data:
                technical_info = {
                    'RSI': technical_data.RSI,
                    'MACD': technical_data.MACD,
                    'MACD_Signal': technical_data.MACD_Signal,
                    'MACD_Hist': technical_data.MACD_Hist,
                    'SMA_20': technical_data.SMA_20,
                    'SMA_50': technical_data.SMA_50,
                    'Bollinger_High': technical_data.Bollinger_High,
                    'Bollinger_Low': technical_data.Bollinger_Low,
                    'Bollinger_MA': technical_data.Bollinger_MA,
                    'Volume_MA_20': technical_data.Volume_MA_20,
                    'ATR': technical_data.ATR,
                    'jdate': technical_data.jdate
                }
            
            # اطلاعات بنیادی
            fundamental_data = session.query(FundamentalData).filter_by(
                symbol_id=symbol.id
            ).first()
            
            fundamental_info = {}
            if fundamental_data:
                fundamental_info = {
                    'eps': fundamental_data.eps,
                    'pe': fundamental_data.pe,
                    'group_pe_ratio': fundamental_data.group_pe_ratio,
                    'psr': fundamental_data.psr,
                    'p_s_ratio': fundamental_data.p_s_ratio,
                    'market_cap': fundamental_data.market_cap,
                    'base_volume': fundamental_data.base_volume,
                    'float_shares': fundamental_data.float_shares
                }
            
            report = {
                'symbol_info': symbol_info,
                'historical_data': historical_list,
                'technical_indicators': technical_info,
                'fundamental_data': fundamental_info,
                'report_date': datetime.now().isoformat()
            }
            
            return True, report
            
        finally:
            session.close()
    except Exception as e:
        logger.error(f"❌ خطا در دریافت گزارش جامع برای {symbol_identifier}: {e}")
        return False, f"خطا در دریافت گزارش جامع: {str(e)}"

def get_market_summary():
    """
    دریافت خلاصه وضعیت بازار
    """
    try:
        session = get_session_local()
        try:
            # تعداد نمادها به تفکیک بازار
            market_stats = session.query(
                ComprehensiveSymbolData.market_type,
                func.count(ComprehensiveSymbolData.id)
            ).group_by(ComprehensiveSymbolData.market_type).all()
            
            # میانگین P/E و P/B
            avg_pe = session.query(
                func.avg(FundamentalData.pe)
            ).filter(FundamentalData.pe.isnot(None)).scalar()
            
            avg_pb = session.query(
                func.avg(FundamentalData.p_b)
            ).filter(FundamentalData.p_b.isnot(None)).scalar()
            
            # تعداد نمادهای با داده کامل
            symbols_with_complete_data = session.query(ComprehensiveSymbolData).filter(
                ComprehensiveSymbolData.id.in_(session.query(HistoricalData.symbol_id).distinct()),
                ComprehensiveSymbolData.id.in_(session.query(TechnicalIndicatorData.symbol_id).distinct()),
                ComprehensiveSymbolData.id.in_(session.query(FundamentalData.symbol_id).distinct())
            ).count()
            
            summary = {
                'total_symbols': session.query(ComprehensiveSymbolData).count(),
                'symbols_with_historical': session.query(func.count(distinct(HistoricalData.symbol_id))).scalar(),
                'symbols_with_technical': session.query(func.count(distinct(TechnicalIndicatorData.symbol_id))).scalar(),
                'symbols_with_fundamental': session.query(func.count(distinct(FundamentalData.symbol_id))).scalar(),
                'symbols_with_complete_data': symbols_with_complete_data,
                'market_distribution': {market: count for market, count in market_stats},
                'average_pe': float(avg_pe) if avg_pe else None,
                'average_pb': float(avg_pb) if avg_pb else None,
                'last_updated': datetime.now().isoformat()
            }
            
            return True, summary
            
        finally:
            session.close()
    except Exception as e:
        logger.error(f"❌ خطا در دریافت خلاصه بازار: {e}")
        return False, f"خطا در دریافت خلاصه بازار: {str(e)}"

# ----------------------------
# توابع زمان‌بندی و اجرای خودکار
# ----------------------------

def schedule_daily_update():
    """
    زمان‌بندی آپدیت روزانه
    """
    try:
        logger.info("⏰ شروع آپدیت روزانه...")
        
        # آپدیت داده‌های تاریخی برای نمادها
        success, msg = run_historical_update_only(limit_per_run=200, days_limit=1)
        if not success:
            logger.warning(f"⚠️ آپدیت تاریخی روزانه با خطا مواجه شد: {msg}")
        
        # آپدیت تحلیل تکنیکال
        success, msg = run_technical_update_only(limit=200)
        if not success:
            logger.warning(f"⚠️ آپدیت تکنیکال روزانه با خطا مواجه شد: {msg}")
        
        logger.info("✅ آپدیت روزانه کامل شد")
        return True, "آپدیت روزانه با موفقیت انجام شد"
        
    except Exception as e:
        logger.error(f"❌ خطا در آپدیت روزانه: {e}")
        return False, f"خطا در آپدیت روزانه: {str(e)}"

def schedule_weekly_update():
    """
    زمان‌بندی آپدیت هفتگی
    """
    try:
        logger.info("⏰ شروع آپدیت هفتگی...")
        
        # آپدیت کامل داده‌ها
        success, msg = run_full_update_with_limits(limit_per_run=300, days_limit=7)
        if not success:
            logger.warning(f"⚠️ آپدیت هفتگی با خطا مواجه شد: {msg}")
        
        # تعمیر داده‌های از دست رفته
        success, msg = run_data_repair(data_type='all', limit=100)
        if not success:
            logger.warning(f"⚠️ تعمیر داده‌های هفتگی با خطا مواجه شد: {msg}")
        
        logger.info("✅ آپدیت هفتگی کامل شد")
        return True, "آپدیت هفتگی با موفقیت انجام شد"
        
    except Exception as e:
        logger.error(f"❌ خطا در آپدیت هفتگی: {e}")
        return False, f"خطا در آپدیت هفتگی: {str(e)}"

def schedule_monthly_maintenance():
    """
    زمان‌بندی نگهداری ماهانه
    """
    try:
        logger.info("⏰ شروع نگهداری ماهانه...")
        
        # پاکسازی داده‌های تکراری
        success, msg = run_cleanup_duplicates()
        if not success:
            logger.warning(f"⚠️ پاکسازی ماهانه با خطا مواجه شد: {msg}")
        
        # آپدیت داده‌های بنیادی
        success, msg = run_fundamental_update_only()
        if not success:
            logger.warning(f"⚠️ آپدیت بنیادی ماهانه با خطا مواجه شد: {msg}")
        
        # دریافت گزارش وضعیت
        success, report = get_status_report()
        if success:
            logger.info(f"📊 گزارش وضعیت ماهانه: {report}")
        
        logger.info("✅ نگهداری ماهانه کامل شد")
        return True, "نگهداری ماهانه با موفقیت انجام شد"
        
    except Exception as e:
        logger.error(f"❌ خطا در نگهداری ماهانه: {e}")
        return False, f"خطا در نگهداری ماهانه: {str(e)}"

# ----------------------------
# توابع کمکی برای دیباگ و عیب‌یابی
# ----------------------------

def debug_symbol_data(symbol_name: str):
    """
    دیباگ اطلاعات یک نماد
    """
    try:
        session = get_session_local()
        try:
            symbol = session.query(ComprehensiveSymbolData).filter_by(name=symbol_name).first()
            if not symbol:
                return False, "نماد یافت نشد"
            
            debug_info = {
                'symbol': {
                    'id': symbol.id,
                    'name': symbol.name,
                    'tse_index': symbol.tse_index,
                    'market': symbol.market
                },
                'historical_count': session.query(HistoricalData).filter_by(symbol_id=symbol.id).count(),
                'technical_count': session.query(TechnicalIndicatorData).filter_by(symbol_id=symbol.id).count(),
                'fundamental_exists': session.query(FundamentalData).filter_by(symbol_id=symbol.id).first() is not None,
                'latest_historical': session.query(HistoricalData).filter_by(symbol_id=symbol.id).order_by(HistoricalData.date.desc()).first(),
                'latest_technical': session.query(TechnicalIndicatorData).filter_by(symbol_id=symbol.id).order_by(TechnicalIndicatorData.date.desc()).first()
            }
            
            return True, debug_info
            
        finally:
            session.close()
    except Exception as e:
        logger.error(f"❌ خطا در دیباگ نماد {symbol_name}: {e}")
        return False, f"خطا در دیباگ نماد: {str(e)}"

def test_pytse_connection():
    """
    تست اتصال به pytse-client
    """
    try:
        if not pytse_wrapper.is_available():
            return False, "pytse-client در دسترس نیست"
        
        # تست دریافت نمادها
        symbols = pytse_wrapper.get_all_symbols()
        if not symbols:
            return False, "خطا در دریافت نمادها از pytse-client"
        
        # تست دریافت داده‌های یک نماد
        test_symbol = list(symbols.keys())[0] if symbols else None
        if test_symbol:
            ticker = pytse_wrapper.get_ticker(test_symbol)
            if not ticker:
                return False, f"خطا در دریافت اطلاعات نماد {test_symbol}"
        
        return True, "اتصال به pytse-client با موفقیت تست شد"
        
    except Exception as e:
        logger.error(f"❌ خطا در تست اتصال به pytse-client: {e}")
        return False, f"خطا در تست اتصال: {str(e)}"

def test_database_connection():
    """
    تست اتصال به دیتابیس
    """
    try:
        session = get_session_local()
        try:
            # تست query ساده
            count = session.query(ComprehensiveSymbolData).count()
            return True, f"اتصال به دیتابیس موفقیت‌آمیز. تعداد نمادها: {count}"
        finally:
            session.close()
    except Exception as e:
        logger.error(f"❌ خطا در تست اتصال به دیتابیس: {e}")
        return False, f"خطا در اتصال به دیتابیس: {str(e)}"

# ----------------------------
# توابع main برای اجرای مستقیم
# ----------------------------

def main():
    """
    تابع اصلی برای اجرای مستقیم
    """
    import argparse
    
    parser = argparse.ArgumentParser(description='مدیریت داده‌های بازار بورس')
    parser.add_argument('--full-update', action='store_true', help='اجرای آپدیت کامل')
    parser.add_argument('--historical', action='store_true', help='اجرای آپدیت تاریخی')
    parser.add_argument('--technical', action='store_true', help='اجرای آپدیت تکنیکال')
    parser.add_argument('--fundamental', action='store_true', help='اجرای آپدیت بنیادی')
    parser.add_argument('--repair', action='store_true', help='اجرای تعمیر داده‌ها')
    parser.add_argument('--cleanup', action='store_true', help='پاکسازی داده‌های تکراری')
    parser.add_argument('--status', action='store_true', help='دریافت گزارش وضعیت')
    parser.add_argument('--limit', type=int, default=100, help='محدودیت تعداد نمادها')
    parser.add_argument('--days', type=int, default=365, help='محدودیت روزهای تاریخی')
    
    args = parser.parse_args()
    
    try:
        if args.full_update:
            success, msg = run_full_update_with_limits(args.limit, args.days)
            print(f"آپدیت کامل: {'✅ موفق' if success else '❌ خطا'} - {msg}")
        
        elif args.historical:
            success, msg = run_historical_update_only(args.limit, args.days)
            print(f"آپدیت تاریخی: {'✅ موفق' if success else '❌ خطا'} - {msg}")
        
        elif args.technical:
            success, msg = run_technical_update_only(args.limit)
            print(f"آپدیت تکنیکال: {'✅ موفق' if success else '❌ خطا'} - {msg}")
        
        elif args.fundamental:
            success, msg = run_fundamental_update_only()
            print(f"آپدیت بنیادی: {'✅ موفق' if success else '❌ خطا'} - {msg}")
        
        elif args.repair:
            success, msg = run_data_repair('all', args.limit)
            print(f"تعمیر داده‌ها: {'✅ موفق' if success else '❌ خطا'} - {msg}")
        
        elif args.cleanup:
            success, msg = run_cleanup_duplicates()
            print(f"پاکسازی: {'✅ موفق' if success else '❌ خطا'} - {msg}")
        
        elif args.status:
            success, result = get_status_report()
            if success:
                import json
                print(json.dumps(result, indent=2, ensure_ascii=False))
            else:
                print(f"❌ خطا: {result}")
        
        else:
            parser.print_help()
            
    except Exception as e:
        logger.error(f"❌ خطا در اجرای دستور: {e}")
        print(f"خطا: {e}")

if __name__ == "__main__":
    main()

# ----------------------------
# Export functions for Flask app
# ----------------------------

__all__ = [
    # توابع کمکی
    'setup_robust_session'

     # توابع اصلی   
    'populate_comprehensive_symbols'
    'fetch_historical_and_fundamental_data'
    'fetch_symbols_from_pytse_client',
    'fetch_and_process_historical_data',
    #'update_historical_data_limited',(DELETED)
    'run_full_data_update',
    'run_technical_analysis',
    'update_comprehensive_symbol_data',
    'initial_populate_all_symbols_and_data',
    
    # توابع مدیریت
    'run_full_update_with_limits',
    'run_historical_update_only',
    'run_technical_update_only',
    'run_fundamental_update_only',
    'get_status_report',
    'run_data_repair',
    'run_cleanup_duplicates',
    
    # توابع نمادها
    'add_single_symbol',
    'remove_symbol',
    'update_symbol_info',
    'search_symbols',
    'filter_symbols_by_market',
    
    # توابع گزارش‌گیری
    'get_symbol_comprehensive_report',
    'get_market_summary',
    
    # توابع زمان‌بندی
    'schedule_daily_update',
    'schedule_weekly_update',
    'schedule_monthly_maintenance',
    
    # توابع دیباگ
    'debug_symbol_data',
    'test_pytse_connection',
    'test_database_connection',
    
    # utility
    'get_session_local',
    'cleanup_memory',
    'get_symbol_id'
]                        
