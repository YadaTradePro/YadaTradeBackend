import requests
from bs4 import BeautifulSoup
import logging
from flask import Flask, request, jsonify
from flask_cors import CORS
import threading
import time
import atexit

# --- تنظیمات اولیه و لاگینگ ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- متغیرهای سراسری برای کشینگ و قفل ---
# یک دیکشنری برای ذخیره آخرین داده‌های موفق
cached_data = {
    'gold': None,
    'coin': None
}
# یک قفل برای جلوگیری از تداخل در دسترسی به کش از تردها
cache_lock = threading.Lock()

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36'
}

# --- تابع‌های اسکرپینگ بهینه‌شده ---
def scrape_tgju_gold():
    """
    اسکرپ کردن داده‌های طلا و به‌روزرسانی کش.
    """
    gold_url = "https://www.tgju.org/gold-chart"
    try:
        g_r = requests.get(gold_url, headers=headers, timeout=10)
        g_r.raise_for_status()
        g_soup = BeautifulSoup(g_r.content, 'html.parser')
        g_tables = g_soup.find_all('table', class_='market-table')
        g_out = []
        for table in g_tables:
            body = table.find('tbody')
            if not body:
                continue
            rows = body.find_all('tr')
            title = table.find('th').text.strip()
            prices = []
            for item in rows:
                item_title_element = item.find('th')
                cells = item.find_all('td')

                if not item_title_element or len(cells) < 4:
                    continue

                item_title = item_title_element.text.strip()
                item_key = cells[-1].find('a').get('href').split('/')[-1]

                price = cells[0].text.strip()
                change_percent = cells[1].text.strip()
                change_value = cells[2].text.strip()
                last_update = cells[3].text.strip()

                prices.append(
                    {
                        'title': item_title,
                        'price': price,
                        'change_percent': change_percent,
                        'change_value': change_value,
                        'last_update': last_update,
                        'key': item_key
                    }
                )
            g_out.append({'title': title, 'prices': prices})

        with cache_lock:
            cached_data['gold'] = g_out
        logger.info("داده‌های طلا با موفقیت اسکرپ و کش شدند.")

    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching gold data, returning cached data. Error: {e}")
    except Exception as e:
        logger.error(f"An unexpected error occurred during gold data scraping: {e}")

# --- تابع جدید برای اسکرپ کردن داده‌های سکه ---
def scrape_tgju_coin():
    """
    اسکرپ کردن داده‌های سکه‌ها از tgju.org و به‌روزرسانی کش.
    """
    coin_url = "https://www.tgju.org/coin-chart"
    try:
        c_r = requests.get(coin_url, headers=headers, timeout=10)
        c_r.raise_for_status()
        c_soup = BeautifulSoup(c_r.content, 'html.parser')

        c_out = []
        coin_table = None

        # ✅ رویکرد جدید: پیدا کردن تمام جداول و بررسی عنوان هر کدام
        all_tables = c_soup.find_all('table')
        for table in all_tables:
            header = table.find('th')
            if header and "قیمت سکه" in header.text:
                coin_table = table
                break

        if coin_table:
            body = coin_table.find('tbody')
            if body:
                rows = body.find_all('tr')

                for row in rows:
                    title_element = row.find('th')
                    cells = row.find_all('td')

                    if not title_element or len(cells) < 4:
                        continue

                    title = title_element.text.strip()
                    key_element = cells[-1].find('a')
                    key = key_element.get('href').split('/')[-1] if key_element else 'N/A'

                    price = cells[0].text.strip()
                    change_percent = cells[1].text.strip()
                    change_value = cells[2].text.strip()
                    last_update = cells[3].text.strip()

                    c_out.append({
                        'title': title,
                        'price': price,
                        'change_percent': change_percent,
                        'change_value': change_value,
                        'last_update': last_update,
                        'key': key
                    })
            else:
                logger.warning("Could not find tbody element within the coin table.")
        else:
            logger.error("Could not find the 'قیمت سکه' table on the page.")

        with cache_lock:
            cached_data['coin'] = c_out
        logger.info("داده‌های سکه با موفقیت اسکرپ و کش شدند.")

    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching coin data, returning cached data. Error: {e}")
    except Exception as e:
        logger.error(f"An unexpected error occurred during coin data scraping: {e}")


def scrape_data_periodically():
    """
    تابع اصلی برای اجرای اسکرپینگ در یک حلقه بی‌نهایت.
    """
    # اولین اسکرپینگ بلافاصله انجام می‌شود
    scrape_tgju_gold()
    scrape_tgju_coin()

    while True:
        logger.info("در حال انتظار برای آپدیت بعدی...")
        time.sleep(3600) # 3600 ثانیه = 60 دقیقه
        scrape_tgju_gold()
        scrape_tgju_coin()

# --- راه‌اندازی سرور Flask ---
app = Flask(__name__)
CORS(app)

@app.route('/api/price/<of>', methods=['GET'])
def get_price(of):
    """
    برگرداندن داده‌های کش شده بر اساس نوع.
    """
    with cache_lock:
        data = cached_data.get(of)

    if data:
        return jsonify(data), 200
    else:
        logger.error(f"درخواست برای داده '{of}' دریافت شد، اما داده‌ای در کش موجود نیست.")
        return jsonify({'message': 'Data not available. Please try again later.'}), 503

@app.errorhandler(404)
def not_found(error):
    return jsonify({'message': 'Route not found'}), 404

@app.errorhandler(500)
def internal_error(error):
    return jsonify({'message': 'Server internal error'}), 500

# شروع ترد برای اسکرپینگ در پس‌زمینه
scraper_thread = threading.Thread(target=scrape_data_periodically, daemon=True)
scraper_thread.start()

if __name__ == '__main__':
    # استفاده از use_reloader=False ضروری است تا ترد اسکرپینگ دوبار اجرا نشود.
    # debug=True فقط برای توسعه است، برای پروداکشن باید False باشد.
    app.run(host="0.0.0.0", port=5001, debug=True, use_reloader=False)