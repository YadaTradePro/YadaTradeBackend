import logging
from datetime import datetime
from typing import Dict, Any, Optional
import pandas as pd
import requests 

# ⚠️ برای سرکوب هشدار InsecureRequestWarning که به دلیل verify=False ظاهر می‌شود.
# این خط باید در ابتدای فایل باشد.
requests.packages.urllib3.disable_warnings(requests.packages.urllib3.exceptions.InsecureRequestWarning)

# --- تنظیمات API جدید BrsApi.ir ---
# کلید API دریافتی از BrsApi.ir (لطفاً محرمانه نگه دارید)
B_R_S_API_KEY = "BvhdYHBjqiyIQ7eTuQBKN17ZuLpHkQZ1"
# آدرس اصلی API جدید (استفاده از روش GET)
B_R_S_API_URL = "https://brsapi.ir/Api/Tsetmc/Index.php"
API_TYPE_PARAM = 3 # پارامتر type=3 برای دریافت شاخص‌های اصلی

# آدرس کامل که در لاگ‌ها گزارش می‌شود:
B_R_S_FULL_URL_LOG = f"{B_R_S_API_URL}?key={B_R_S_API_KEY}&type={API_TYPE_PARAM}"

# --- حذف کامل وابستگی به pytse_client و wrapper ---
try:
    from flask import current_app
    FLASK_AVAILABLE = True
except Exception:
    FLASK_AVAILABLE = False

logger = logging.getLogger(__name__)

# --- نگاشت نام‌های شاخص به‌روز شده ---
INDEX_NAME_MAPPING = {
    # نام شاخص در خروجی JSON API جدید : نام کلید داخلی مورد انتظار
    "شاخص کل": "Total_Index",
    "شاخص کل (هم وزن)": "Equal_Weighted_Index",
    "شاخص قیمت (هم وزن)": "Price_Equal_Weighted_Index",
    # Industry_Index وجود ندارد.
}

# --- هدرهای الگوبرداری شده از فایل موفق (fetch_latest_brsapi_eod.py) ---
CUSTOM_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*"
}

def _default_index_payload() -> Dict[str, Dict[str, Any]]:
    """
    خروجی پیش‌فرض و ایمن برای زمانی که نمی‌توانیم دادهٔ واقعی شاخص‌ها را بگیریم.
    """
    return {
        "Total_Index": {"value": None, "change": None, "percent": None, "date": None},
        "Equal_Weighted_Index": {"value": None, "change": None, "percent": None, "date": None},
        "Price_Equal_Weighted_Index": {"value": None, "change": None, "percent": None, "date": None},
        "Industry_Index": {"value": None, "change": None, "percent": None, "date": None},
    }

def _safe_to_float(x) -> Optional[float]:
    """
    تبدیل ایمن مقدار به float.
    """
    try:
        if x is None:
            return None
        # برای سازگاری، استفاده از pandas حفظ شد
        val = pd.to_numeric(x, errors="coerce")
        return float(val) if pd.notna(val) else None
    except Exception:
        return None

def fetch_iran_market_indices() -> Dict[str, Dict[str, Any]]:
    """
    دریافت لحظه‌ای داده‌های شاخص بازار از طریق API جدید BrsApi.ir (روش GET).
    """
    logger.info(f"در حال تلاش برای دریافت داده‌های شاخص بازار ایران از {B_R_S_FULL_URL_LOG}")

    result = _default_index_payload()

    # تنظیم پارامترهای GET (کلید و نوع)
    params = {
        'key': B_R_S_API_KEY,
        'type': API_TYPE_PARAM,
    }

    try:
        # **استفاده از requests.Session و verify=False برای دور زدن مشکل SSL**
        with requests.Session() as session:
            # ارسال درخواست GET با پارامترها، هدرها و **نادیده گرفتن SSL**
            response = session.get(
                B_R_S_API_URL, 
                params=params, 
                headers=CUSTOM_HEADERS, 
                timeout=15, 
                verify=False # 💥 این کلید مشکل ConnectionResetError را موقتاً حل می‌کند
            ) 
            
            response.raise_for_status() # برای تشخیص خطاهای HTTP (مثل 4xx یا 5xx)

            data_list = response.json()
            
        if not isinstance(data_list, list) or not data_list:
            logger.warning(f"پاسخ API خالی است یا ساختار صحیحی ندارد: {data_list}. بازگشت دادهٔ پیش‌فرض.")
            return result
        
        # --- تحلیل و پردازش داده‌های API جدید ---
        for index_item in data_list:
            index_name_raw = index_item.get("name")
            
            friendly_name = INDEX_NAME_MAPPING.get(index_name_raw)
            if not friendly_name:
                logger.debug(f"شاخص ناشناخته/غیرضروری از API دریافت شد: {index_name_raw}")
                continue
                
            value = _safe_to_float(index_item.get("index"))
            change = _safe_to_float(index_item.get("index_change")) 
            percent = _safe_to_float(index_item.get("index_change_percent"))
            
            date_fmt = datetime.now().strftime("%Y-%m-%d")

            result[friendly_name] = {
                "value": value,
                "change": change,
                "percent": percent,
                "date": date_fmt,
            }

        logger.info("داده‌های شاخص بازار با موفقیت از BrsApi.ir دریافت و پردازش شد.")
            
    except requests.exceptions.Timeout:
        logger.error("خطا: درخواست API به دلیل timeout (۱۵ ثانیه) لغو شد.")
    except requests.exceptions.RequestException as e:
        # این بلوک خطای ConnectionResetError را که علت اصلی مشکل بود، مدیریت می‌کند.
        logger.error(f"خطا در برقراری ارتباط با API BrsApi.ir: {e}", exc_info=True)
    except Exception as e:
        logger.error(f"خطای غیرمنتظره در پردازش داده‌ها: {e}", exc_info=True)

    return result