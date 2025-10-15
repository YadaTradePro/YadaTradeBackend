# routes/analysis.py
from flask_restx import Namespace, Resource, fields, reqparse
from flask_jwt_extended import jwt_required, get_jwt_identity
from flask import request, current_app
from flask_cors import cross_origin

from services import market_analysis_service
from services.historical_data_service import get_historical_data_for_symbol
from datetime import date
import datetime
import jdatetime

from werkzeug.exceptions import HTTPException

from services.fetch_latest_brsapi_eod import update_daily_eod_from_brsapi


# از db و سایر مدل‌ها از extensions و models وارد کنید
from extensions import db
from models import (
    User, HistoricalData, ComprehensiveSymbolData, SignalsPerformance, FundamentalData,
    TechnicalIndicatorData, MLPrediction
)



#init اولیه نمادها و بروزرسانی دوره‌ای لیست.
from flask import Blueprint, jsonify
from services.symbol_initializer import populate_symbols_into_db


# Import services relevant to analysis_ns only
from services import data_fetch_and_process
from services.data_fetch_and_process import run_technical_analysis, run_candlestick_detection, populate_comprehensive_symbols

from services.ml_prediction_service import get_ml_predictions_for_symbol, get_all_ml_predictions, generate_and_save_predictions_for_watchlist

# Import func from sqlalchemy for database operations
from sqlalchemy import func 
from sqlalchemy import or_ 

analysis_ns = Namespace('analysis', description='Stock data analysis and fetching operations')


def parse_date(value):
    """
    تاریخ را از فرمت رشته (YYYY-MM-DD) به شیء datetime.date (میلادی) تبدیل می‌کند.
    اگر فرمت شمسی باشد، آن را به میلادی تبدیل می‌کند.
    """
    if not isinstance(value, str) or not value:
        return None
        
    # 1. تلاش برای پارس کردن به عنوان تاریخ میلادی (ISO format)
    try:
        return datetime.date.fromisoformat(value)
    except ValueError:
        pass # اگر پارس میلادی شکست خورد، ادامه می‌دهیم به پارس شمسی

    # 2. تلاش برای پارس کردن به عنوان تاریخ شمسی و تبدیل به میلادی
    try:
        # فرض بر این است که فرمت شمسی (YYYY-MM-DD) است
        j_year, j_month, j_day = map(int, value.split('-'))
        # تبدیل تاریخ شمسی به میلادی
        g_date = jdatetime.date(j_year, j_month, j_day).togregorian()
        return g_date
    except Exception:
        # اگر پارس شمسی هم شکست خورد، None برمی‌گردانیم.
        return None




# --- API Models for Flask-RESTX Documentation ---
# این مدل‌ها برای مستندسازی Swagger UI استفاده می‌شوند.
# مطمئن شوید که فیلدهای این مدل‌ها با فیلدهای خروجی توابع سرویس شما مطابقت دارند.

historical_data_model = analysis_ns.model('HistoricalData', {
    'symbol_id': fields.String(required=True, description='Stock symbol ID (Persian short name)'),
    'symbol_name': fields.String(description='Stock symbol name (Persian short name)'),
    'jdate': fields.String(description='Persian date (YYYY-MM-DD)'),
    'date': fields.String(description='Gregorian date (YYYY-MM-DD)'),
    'open': fields.Float(description='Opening price'),
    'high': fields.Float(description='Highest price'),
    'low': fields.Float(description='Lowest price'),
    'close': fields.Float(description='Closing price'),
    'final': fields.Float(description='Final price'),
    'yesterday_price': fields.Float(description='Yesterday\'s closing price'),
    'volume': fields.Integer(description='Trading volume'),
    'value': fields.Float(description='Trading value'),
    'num_trades': fields.Integer(description='Number of trades'),
    'plc': fields.Float(description='Price change (last closing)'),
    'plp': fields.Float(description='Price change percentage (last closing)'),
    'pcc': fields.Float(description='Price change (final closing)'),
    'pcp': fields.Float(description='Price change percentage (final closing)'),
    'mv': fields.Float(description='Market Value'),
    'eps': fields.Float(description='Earnings Per Share'),
    'pe': fields.Float(description='Price to Earnings Ratio'),
    'buy_count_i': fields.Integer(description='Number of real buyer accounts'),
    'buy_count_n': fields.Integer(description='Number of legal buyer accounts'),
    'sell_count_i': fields.Integer(description='Number of real seller accounts'),
    'sell_count_n': fields.Integer(description='Number of legal seller accounts'),
    'buy_i_volume': fields.Integer(description='Real buyer volume'),
    'buy_n_volume': fields.Integer(description='Legal buyer volume'),
    'sell_i_volume': fields.Integer(description='Real seller volume'),
    'sell_n_volume': fields.Integer(description='Legal seller volume'),
    'zd1': fields.Integer(description='Demand count 1'),
    'qd1': fields.Integer(description='Demand volume 1'),
    'pd1': fields.Float(description='Demand price 1'),
    'zo1': fields.Integer(description='Supply count 1'),
    'qo1': fields.Integer(description='Supply volume 1'),
    'po1': fields.Float(description='Supply price 1'),
    'zd2': fields.Integer(description='Demand count 2'),
    'qd2': fields.Integer(description='Demand volume 2'),
    'pd2': fields.Float(description='Demand price 2'),
    'zo2': fields.Integer(description='Supply count 2'),
    'qo2': fields.Integer(description='Supply volume 2'),
    'po2': fields.Float(description='Supply price 2'),
    'zd3': fields.Integer(description='Demand count 3'),
    'qd3': fields.Integer(description='Demand volume 3'),
    'pd3': fields.Float(description='Demand price 3'),
    'zo3': fields.Integer(description='Supply count 3'),
    'qo3': fields.Integer(description='Supply volume 3'),
    'po3': fields.Float(description='Supply price 3'),
    'zd4': fields.Integer(description='Demand count 4'),
    'qd4': fields.Integer(description='Demand volume 4'),
    'pd4': fields.Float(description='Demand price 4'),
    'zo4': fields.Integer(description='Supply count 4'),
    'qo4': fields.Integer(description='Supply volume 4'),
    'po4': fields.Float(description='Supply price 4'),
    'zd5': fields.Integer(description='Demand count 5'),
    'qd5': fields.Integer(description='Demand volume 5'),
    'pd5': fields.Float(description='Demand price 5'),
    'zo5': fields.Integer(description='Supply count 5'),
    'qo5': fields.Integer(description='Supply volume 5'),
    'po5': fields.Float(description='Supply price 5')
})

comprehensive_symbol_data_model = analysis_ns.model('ComprehensiveSymbolData', {
    'symbol_id': fields.String(required=True, description='Stock symbol ID (Persian short name)'),
    'symbol_name': fields.String(required=True, description='Stock symbol name (Persian short name)'),
    'company_name': fields.String(description='Company name'),
    'isin': fields.String(description='ISIN code'),
    'market_type': fields.String(description='Market type'),
    'flow': fields.String(description='Flow (e.g., 1 for main market, 2 for secondary)'),
    'industry': fields.String(description='Industry name'),
    'capital': fields.String(description='Company capital'),
    'legal_shareholder_percentage': fields.Float(description='Legal Shareholder Percentage'),
    'real_shareholder_percentage': fields.Float(description='Real Shareholder Percentage'),
    'float_shares': fields.Float(description='Float shares'),
    'base_volume': fields.Float(description='Base volume'),
    'group_name': fields.String(description='Group name'),
    'description': fields.String(description='Symbol description'),
    'last_historical_update_date': fields.String(description='Last historical update date (YYYY-MM-DD)')
})

# Model for TechnicalIndicatorData
technical_indicator_model = analysis_ns.model('TechnicalIndicatorData', {
    'symbol_id': fields.String(required=True, description='شناسه نماد'),
    'jdate': fields.String(required=True, description='تاریخ شمسی (YYYY-MM-DD)'),
    'close_price': fields.Float(description='قیمت پایانی'),
    'RSI': fields.Float(description='اندیکاتور RSI'),
    'MACD': fields.Float(description='اندیکاتور MACD'),
    'MACD_Signal': fields.Float(description='خط سیگنال MACD'),
    'MACD_Hist': fields.Float(description='هیستوگرام MACD'),
    'SMA_20': fields.Float(description='میانگین متحرک ساده ۲۰ روزه'),
    'SMA_50': fields.Float(description='میانگین متحرک ساده ۵۰ روزه'),
    'Bollinger_High': fields.Float(description='باند بالای بولینگر'),
    'Bollinger_Low': fields.Float(description='باند پایین بولینگر'),
    'Bollinger_MA': fields.Float(description='میانگین متحرک باند بولینگر'),
    'Volume_MA_20': fields.Float(description='میانگین متحرک حجم ۲۰ روزه'),
    'ATR': fields.Float(description='اندیکاتور ATR'),
    # New indicators added to the model
    'Stochastic_K': fields.Float(description='Stochastic Oscillator %K'),
    'Stochastic_D': fields.Float(description='Stochastic Oscillator %D'),
    'squeeze_on': fields.Boolean(description='وضعیت Squeeze Momentum'),
    'halftrend_signal': fields.Integer(description='سیگنال HalfTrend (1 برای خرید)'),
    'resistance_level_50d': fields.Float(description='سطح مقاومت ۵۰ روزه'),
    'resistance_broken': fields.Boolean(description='آیا مقاومت شکسته شده است')
})

# Model for FundamentalData
fundamental_data_model = analysis_ns.model('FundamentalData', {
    'symbol_id': fields.String(required=True, description='Stock symbol ID (Persian short name)'),
    'last_updated': fields.DateTime(description='Last update timestamp'),
    'eps': fields.Float(description='Earnings Per Share'),
    'pe_ratio': fields.Float(description='Price-to-Earnings Ratio'),
    'group_pe_ratio': fields.Float(description='Group Price-to-Earnings Ratio'),
    'psr': fields.Float(description='Price-to-Sales Ratio (PSR)'),
    'p_s_ratio': fields.Float(description='Price-to-Sales Ratio (P/S)'),
    'market_cap': fields.Float(description='Market Capitalization'),
    'base_volume': fields.Float(description='Base Volume'),
    'float_shares': fields.Float(description='Float Shares')
})

# NEW: Model for ML Predictions (ADDED)
ml_prediction_model = analysis_ns.model('MLPredictionModel', {
    'id': fields.Integer(readOnly=True, description='The unique identifier of the prediction'),
    'symbol_id': fields.String(required=True, description='The ID of the stock symbol'),
    'symbol_name': fields.String(required=True, description='The name of the stock symbol'),
    'prediction_date': fields.String(required=True, description='Gregorian date when the prediction was made (YYYY-MM-DD)'),
    'jprediction_date': fields.String(required=True, description='Jalali date when the prediction was made (YYYY-MM-DD)'),
    'prediction_period_days': fields.Integer(description='Number of days for the prediction horizon'),
    'predicted_trend': fields.String(required=True, description='Predicted trend: UP, DOWN, or NEUTRAL'),
    'prediction_probability': fields.Float(required=True, description='Probability/confidence of the predicted trend (0.0 to 1.0)'),
    'predicted_price_at_period_end': fields.Float(description='Optional: Predicted price at the end of the period'),
    'actual_price_at_period_end': fields.Float(description='Actual price at the end of the prediction period'),
    'actual_trend_outcome': fields.String(description='Actual trend outcome: UP, DOWN, or NEUTRAL'),
    'is_prediction_accurate': fields.Boolean(description='True if predicted_trend matches actual_trend_outcome'),
    'signal_source': fields.String(description='Source of the signal, e.g., ML-Trend'),
    'model_version': fields.String(description='Version of the ML model used for prediction'),
    'created_at': fields.String(description='Timestamp of creation'),
    'updated_at': fields.String(description='Timestamp of last update'),
})

# =================================================================================
# --- Parsers for API Endpoints ---
# =================================================================================

populate_symbols_parser = reqparse.RequestParser()
populate_symbols_parser.add_argument('batch_size', 
                                     type=int, 
                                     required=False, 
                                     help='تعداد نمادهایی که در هر دسته (Batch) از TSETMC خوانده و در دیتابیس ثبت می‌شوند. پیش‌فرض: 200.', 
                                     default=200, # مقدار پیش‌فرض به 200 تغییر یافت
                                     location='json')


update_parser = reqparse.RequestParser()
update_parser.add_argument('limit', type=int, default=200, help='محدودیت تعداد نمادها برای پردازش در هر اجرا')
# **توجه:** در صورت نیاز به پارامتر specific_symbols_list، آن را به این پارسر اضافه کنید.

repair_parser = reqparse.RequestParser()
repair_parser.add_argument('data_type', type=str, default='all', choices=['all', 'historical', 'technical', 'fundamental'], help='نوع داده برای ترمیم')
repair_parser.add_argument('limit', type=int, default=50, help='محدودیت تعداد نمادها برای ترمیم')

# ✅ FIX: این پارسر اکنون در جایگاه صحیح و قبل از اولین استفاده تعریف شده است.
historical_data_parser = reqparse.RequestParser()
historical_data_parser.add_argument('days', type=int, default=61, help='تعداد **رکوردهای تاریخی** (روزهای معاملاتی) اخیر برای واکشی. اگر start_date و end_date باشند، این پارامتر نادیده گرفته می‌شود.')
historical_data_parser.add_argument('start_date', type=str, help='تاریخ میلادی شروع بازه (YYYY-MM-DD) شمسی یا میلادی') # ✅ جدید: اشاره به شمسی یا میلادی
historical_data_parser.add_argument('end_date', type=str, help='تاریخ میلادی پایان بازه (YYYY-MM-DD) شمسی یا میلادی') # ✅ جدید: اشاره به شمسی یا میلادی


# 🆕 پارسر جدید برای عملیات Full Refresh
full_refresh_parser = reqparse.RequestParser()
full_refresh_parser.add_argument(
    'specific_symbols', 
    type=list, 
    location='json', 
    required=False, 
    help='لیست نام نمادهای مشخصی که باید به روز رسانی شوند (مثال: ["خودرو", "خساپا"]). اگر خالی باشد، تمام نمادها پردازش می‌شوند.'
)



# --- API Resources ---

# =================================================================================
# --- Section 1: Task Execution Endpoints ---
# =================================================================================


@analysis_ns.route('/stock-history/<string:symbol_input>') # تغییر نام متغیر به symbol_input
@analysis_ns.param('symbol_input', 'شناسه یا نام نماد (مثال: خودرو)')
class StockHistoryResource(Resource):
    @analysis_ns.doc(security='Bearer Auth', parser=historical_data_parser)
    @jwt_required()
    @analysis_ns.response(200, 'Historical data fetched successfully')
    @analysis_ns.response(400, 'Invalid date format')
    @analysis_ns.response(404, 'No data found for symbol')
    def get(self, symbol_input): # تغییر نام متغیر به symbol_input
        """
        واکشی سابقه معاملات (Historical Data) یک نماد مشخص با قابلیت فیلتر زمانی.
        """
        try:
            args = historical_data_parser.parse_args()
            days = args['days']
            start_date_str = args['start_date']
            end_date_str = args['end_date']
            
            start_date = parse_date(start_date_str)
            end_date = parse_date(end_date_str)

            if (start_date_str and start_date is None) or (end_date_str and end_date is None):
                analysis_ns.abort(400, "Invalid date format. Please use YYYY-MM-DD (Gregorian or Jalali).")

            # 🚀 فراخوانی تابع سرویس
            history_data = get_historical_data_for_symbol(
                symbol_input, # از symbol_input استفاده می‌شود.
                start_date=start_date, 
                end_date=end_date, 
                days=days
            )
            
            if history_data is None:
                current_app.logger.error(f"Service returned None for {symbol_input}")
                analysis_ns.abort(500, "Internal server error during data retrieval. Service returned None.")

            if not history_data:
                # این خط باعث ایجاد 404 می‌شود.
                analysis_ns.abort(404, f"No historical data found for symbol: {symbol_input} in the specified range.")

            return {"history": history_data}, 200
            
        except HTTPException as e:
            # ✅ FIX: اگر خطا یک خطای HTTP (مثل 404 یا 400) باشد، آن را بدون تغییر بالا می‌اندازیم.
            raise e
            
        except Exception as e:
            # برای هر خطای غیرمنتظره دیگر (مثل خطای دیتابیس یا منطقی)
            current_app.logger.error(f"An unexpected critical error occurred for {symbol_input}: {e}", exc_info=True)
            analysis_ns.abort(500, f"An unexpected critical error occurred: {str(e)}")






@analysis_ns.route('/initial-populate')
class InitialPopulationResource(Resource):
    @analysis_ns.doc(security='Bearer Auth', description="اجرای فرآیند کامل برای راه‌اندازی اولیه دیتابیس. این عملیات سنگین است و فقط یک بار باید اجرا شود.")
    @jwt_required()
    def post(self):
        """راه‌اندازی اولیه دیتابیس با تمام نمادها و داده‌های تاریخی"""
        try:
            result = data_fetch_and_process.initial_populate_all_symbols_and_data(db.session)
            return {"message": "Initial population process completed successfully.", "details": result}, 200
        except Exception as e:
            current_app.logger.error(f"Error during initial population: {e}", exc_info=True)
            return {"message": f"An error occurred: {str(e)}"}, 500



# NEW: Endpoint برای پر کردن لیست نمادها
@analysis_ns.route('/populate-symbols')
class PopulateSymbolsResource(Resource):
    @analysis_ns.doc(security='Bearer Auth', 
                      description="فقط لیست نمادها را از بورس دریافت کرده و در دیتابیس درج/به‌روزرسانی می‌کند. این عملیات به صورت دسته‌های 200 تایی اجرا می‌شود.")
    @jwt_required()
    @analysis_ns.expect(populate_symbols_parser)
    def post(self):
        """دریافت و به‌روزرسانی لیست نمادهای بازار"""
        args = populate_symbols_parser.parse_args()
        # 💡 تغییر از limit به batch_size
        batch_size = args.get('batch_size')
        
        try:
            # 💡 فراخوانی همان تابع اصلی با پارامتر batch_size
            result = data_fetch_and_process.populate_comprehensive_symbols(db.session, batch_size=batch_size)
            
            return {
                "message": "Symbol list population completed successfully.", 
                "details": result
            }, 200
            
        except Exception as e:
            current_app.logger.error(f"Error during symbol population: {e}", exc_info=True)
            db.session.rollback()
            # 💡 بهتر است پیام خطا را از سمت بک‌اند (که در آن Read timed out مدیریت شده) به کاربر برگردانید.
            return {"message": f"An error occurred: {str(e)}"}, 500


# --- NEW: BRSAPI EOD Update and Analysis Endpoint (The new daily flow) ---
@analysis_ns.route('/run-brsapi-eod-flow')
class BRSAPIEODFlowResource(Resource):
    @analysis_ns.doc(security='Bearer Auth', 
                     description="فرایند کامل EOD روزانه: فچ داده از BRSAPI، ذخیره، تحلیل تکنیکال و تشخیص الگوهای شمعی. این مسیر وظیفه به‌روزرسانی روزانه Historical/Technical را بر عهده دارد.")
    @jwt_required()
    def post(self):
        """اجرای جریان کامل آپدیت EOD از BRSAPI"""
        today = date.today()
        results = {
            "eod_brsapi": {"count": 0, "message": ""},
            "technical_analysis": {"count": 0, "message": "N/A"},
            "candlestick_detection": {"count": 0, "message": "N/A"},
            "update_status": "Failed"
        }

        try:
            # 1. 🚀 فچ و ذخیره داده‌های EOD از BRSAPI
            # فرض بر این است که update_daily_eod_from_brsapi خروجی (count, message, list_of_updated_symbol_ids) دارد.
            eod_count, eod_msg, updated_symbol_ids = update_daily_eod_from_brsapi(db.session)
            results['eod_brsapi']['count'] = eod_count
            results['eod_brsapi']['message'] = eod_msg
            
            # 2. ⚡️ اجرای مراحل بعدی فقط برای نمادهای به‌روز شده
            if eod_count > 0:
                # 2.1. اجرای تحلیل تکنیکال
                tech_count, tech_msg = run_technical_analysis(
                    db.session,
                    symbols_list=updated_symbol_ids # استفاده از symbol_id داخلی
                )
                results['technical_analysis']['count'] = tech_count
                results['technical_analysis']['message'] = tech_msg

                # 2.2. تشخیص الگوهای شمعی
                candlestick_count = run_candlestick_detection(
                    db.session, 
                    symbols_list=updated_symbol_ids # استفاده از symbol_id داخلی
                )
                results['candlestick_detection']['count'] = candlestick_count
                results['candlestick_detection']['message'] = f"✅ تشخیص الگوهای شمعی برای {candlestick_count} نماد انجام شد."
                
                # 2.3. به‌روزرسانی تاریخ آپدیت در ComprehensiveSymbolData (برای جلوگیری از تکرار اجرا در run-daily-update)
                symbols_to_update = db.session.query(ComprehensiveSymbolData).filter(
                    ComprehensiveSymbolData.symbol_id.in_(updated_symbol_ids)
                ).all()
                
                for symbol in symbols_to_update:
                    symbol.last_historical_update_date = today # تنظیم به امروز
                
                db.session.commit()
                results['update_status'] = f"Success: {len(updated_symbol_ids)} symbol status updated."

            elif "قبلاً برای تمام نمادهای فچ‌شده ثبت شده است" in eod_msg:
                results['update_status'] = "Already Updated"
            
            final_message = (
                f"🏁 BRSAPI EOD Flow completed. "
                f"EOD: {eod_count}, "
                f"Technical: {results['technical_analysis']['count']}, "
                f"Candlestick: {results['candlestick_detection']['count']}."
            )
            current_app.logger.info(final_message)
            
            return results, 200

        except Exception as e:
            current_app.logger.error(f"❌ Critical error during BRSAPI EOD Flow: {e}", exc_info=True)
            db.session.rollback() 
            results['update_status'] = "Failed due to critical error."
            return {"message": f"An error occurred: {str(e)}", "results": results}, 500

# جدید برای پاک کردن کل داده های تاریخی و فاندیمنتال و جایگزینی
@analysis_ns.route('/full-historical-refresh')
class FullHistoricalRefreshResource(Resource):
    @analysis_ns.doc(security='Bearer Auth', parser=full_refresh_parser, description="⚠️ این عملیات شامل **حذف کامل (TRUNCATE)** جدول داده‌های تاریخی و درج مجدد تمام داده‌ها به صورت بچ‌بندی است.")
    @jwt_required()
    @analysis_ns.response(200, 'Full historical data refresh completed successfully.')
    @analysis_ns.response(500, 'Internal server error during refresh operation.')
    def post(self):
        """
        اجرای فرآیند کامل دریافت و به‌روزرسانی داده‌های تاریخی و فاندامنتال (TRUNCATE + Bulk Insert).
        """
        try:
            # ⚠️ مطمئن شوید که تابع به درستی import شده است
            from services.fetch_full_historical_pytse import fetch_full_historical_pytse
            from extensions import db
            from flask import current_app

            args = full_refresh_parser.parse_args()
            symbols_list = args.get('specific_symbols')

            # فراخوانی تابع سرویس
            record_count, message = fetch_full_historical_pytse(
                db.session,
                symbols_to_update=symbols_list
            )
            
            return {
                "message": "Full historical and fundamental data refresh completed.", 
                "details": message,
                "records_inserted": record_count
            }, 200
            
        except Exception as e:
            db.session.rollback()
            current_app.logger.error(f"Error during full historical data refresh: {e}", exc_info=True)
            analysis_ns.abort(500, f"An unexpected critical error occurred: {str(e)}")




# --- NEW: Daily Update Endpoint ---
@analysis_ns.route('/run-daily-update')
class DailyUpdateResource(Resource):
    @analysis_ns.doc(security='Bearer Auth', description="اجرای فرآیند سبک و بهینه برای به‌روزرسانی روزانه داده‌ها پس از پایان بازار.")
    @jwt_required()
    @analysis_ns.expect(update_parser)
    def post(self):
        """اجرای آپدیت سبک روزانه"""
        args = update_parser.parse_args()
        try:
            # 1. دسترسی ایمن به limit
            # استفاده از hasattr برای جلوگیری از AttributeError در صورتی که limit در update_parser تعریف نشده باشد
            limit_val = args.limit if hasattr(args, 'limit') and args.limit is not None else 200
            
            # 2. دسترسی ایمن به specific_symbols_list
            specific_symbols_list_val = args.specific_symbols_list if hasattr(args, 'specific_symbols_list') else None
            
            # ⛔ limit_per_run و update_fundamental از اینجا حذف شدند تا از مقادیر پیش‌فرض تابع استفاده شود
            
            # 🚀 فراخوانی تابع اصلی
            result = data_fetch_and_process.run_daily_update(
                db_session=db.session, 
                limit=limit_val,
                specific_symbols_list=specific_symbols_list_val
                # update_fundamental=True و limit_per_run حذف شدند
            )
            
            # برگرداندن نتیجه
            return {
                "message": "Daily update process completed (Historical, Technical, and Fundamental check).",
                "results": result 
            }, 200
        except Exception as e:
            current_app.logger.error(f"Error during daily update: {e}", exc_info=True)
            db.session.rollback() 
            return {"message": f"An error occurred: {str(e)}"}, 500


# --- REVISED: Maintenance Update Endpoint (Formerly Full Update) ---
@analysis_ns.route('/run-maintenance-update')
class MaintenanceUpdateResource(Resource):
    @analysis_ns.doc(security='Bearer Auth', description="اجرای فرآیند کامل و سنگین برای همگام‌سازی کلی، مناسب برای اجرای هفتگی یا ماهانه.")
    @jwt_required()
    @analysis_ns.expect(update_parser)
    def post(self):
        """اجرای آپدیت کامل برای نگهداری و همگام‌سازی دوره‌ای"""
        args = update_parser.parse_args()
        try:
            result = data_fetch_and_process.run_full_data_update(db.session, limit_per_run=args['limit'])
            return {"message": "Full maintenance update completed.", "details": result}, 200
        except Exception as e:
            current_app.logger.error(f"Error during maintenance update: {e}", exc_info=True)
            return {"message": f"An error occurred: {str(e)}"}, 500


# =================================================================================
# --- Section 2: Maintenance & Status Endpoints ---
# =================================================================================

# --- NEW: Status Report Endpoint ---
@analysis_ns.route('/status-report')
class StatusReportResource(Resource):
    @analysis_ns.doc(security='Bearer Auth', description="دریافت یک گزارش جامع از وضعیت داده‌های موجود در دیتابیس و سازگاری آن‌ها.")
    @jwt_required()
    def get(self):
        """دریافت گزارش وضعیت داده‌ها"""
        success, report = data_fetch_and_process.get_status_report()
        if success:
            return report, 200
        else:
            return {"message": report}, 500

# --- NEW: Data Repair Endpoint ---
@analysis_ns.route('/repair-data')
class RepairDataResource(Resource):
    @analysis_ns.doc(security='Bearer Auth', description="اجرای فرآیند ترمیم برای پیدا کردن و پر کردن داده‌های ناقص یا از دست رفته.")
    @jwt_required()
    @analysis_ns.expect(repair_parser)
    def post(self):
        """ترمیم داده‌های ناقص"""
        args = repair_parser.parse_args()
        success, message = data_fetch_and_process.run_data_repair(data_type=args['data_type'], limit=args['limit'])
        if success:
            return {"message": message}, 200
        else:
            return {"message": message}, 500

# --- NEW: Cleanup Duplicates Endpoint ---
@analysis_ns.route('/cleanup-duplicates')
class CleanupDuplicatesResource(Resource):
    @analysis_ns.doc(security='Bearer Auth', description="اجرای فرآیند پاکسازی برای حذف رکوردهای تکراری از جداول داده.")
    @jwt_required()
    def post(self):
        """پاکسازی داده‌های تکراری"""
        success, message = data_fetch_and_process.run_cleanup_duplicates()
        if success:
            return {"message": message}, 200
        else:
            return {"message": message}, 500


# =================================================================================
# --- Section 3: Data Retrieval Endpoints ---
# (These endpoints are mostly unchanged but kept for completeness)
# =================================================================================

@analysis_ns.route('/historical-data/<string:symbol_input>')
@analysis_ns.param('symbol_input', 'شناسه یا نام نماد (مثال: خودرو)')
class HistoricalDataResource(Resource):
    @jwt_required()
    @analysis_ns.doc(security='Bearer Auth', parser=historical_data_parser)
    @analysis_ns.marshal_list_with(historical_data_model)
    def get(self, symbol_input):
        """دریافت داده‌های تاریخی برای یک نماد مشخص (استفاده از سرویس)"""
        try:
            args = historical_data_parser.parse_args()
            days = args['days']
            start_date_str = args['start_date']
            end_date_str = args['end_date']
            
            start_date = parse_date(start_date_str)
            end_date = parse_date(end_date_str)

            if (start_date_str and start_date is None) or (end_date_str and end_date is None):
                analysis_ns.abort(400, "Invalid date format. Please use YYYY-MM-DD (Gregorian or Jalali).")
                
            # 🚀 استفاده از سرویس جدید برای بازیابی داده‌ها
            history_data = get_historical_data_for_symbol(
                symbol_input, 
                start_date=start_date, 
                end_date=end_date, 
                days=days
            )
            
            if not history_data:
                analysis_ns.abort(404, f"No historical data found for symbol: {symbol_input}")
                
            return history_data
            
        except HTTPException as e:
            raise e
        except Exception as e:
            current_app.logger.error(f"An unexpected critical error occurred in HistoricalDataResource for {symbol_input}: {e}", exc_info=True)
            analysis_ns.abort(500, f"An unexpected critical error occurred: {str(e)}")


@analysis_ns.route('/technical-indicators/<string:symbol_input>')
@analysis_ns.param('symbol_input', 'شناسه یا نام نماد (مثال: خودرو)')
class TechnicalIndicatorsResource(Resource):
    @jwt_required()
    @analysis_ns.doc(security='Bearer Auth')
    @analysis_ns.marshal_list_with(technical_indicator_model)
    def get(self, symbol_input):
        """دریافت اندیکاتورهای تکنیکال برای یک نماد مشخص"""
        # این بخش مستقیماً اندیکاتورهای ذخیره‌شده را می‌خواند و نیاز به تغییر به get_historical_data_for_symbol ندارد.
        # می‌توانید در آینده یک سرویس get_technical_data_for_symbol ایجاد کنید.
        records = TechnicalIndicatorData.query.join(ComprehensiveSymbolData, TechnicalIndicatorData.symbol_id == ComprehensiveSymbolData.id)\
            .filter(ComprehensiveSymbolData.symbol_name == symbol_input)\
            .order_by(TechnicalIndicatorData.jdate.desc()).all()
        if not records:
            analysis_ns.abort(404, f"No technical indicators found for symbol: {symbol_input}")
        return records






@analysis_ns.route('/fundamental_data/<string:symbol_input>')
@analysis_ns.param('symbol_input', 'The stock symbol ID (Persian short name) or ISIN')
class FundamentalDataResource(Resource):
    @jwt_required()
    @analysis_ns.marshal_with(fundamental_data_model)
    @analysis_ns.response(200, 'Fundamental data fetched successfully')
    @analysis_ns.response(404, 'No fundamental data found for the symbol')
    @analysis_ns.doc(security='Bearer Auth')
    def get(self, symbol_input):
        """Fetches fundamental data for a given stock symbol."""
        symbol_id = data_fetch_and_process.get_symbol_id(symbol_input)
        if not symbol_id:
            analysis_ns.abort(404, f"Invalid symbol ID or name: {symbol_input}")

        fundamental_data = FundamentalData.query.filter_by(symbol_id=symbol_id).first()
        if not fundamental_data:
            # Attempt to fetch and save fundamental data if not found in DB
            # This now calls the specific update_fundamental_data function
            success, msg = data_fetch_and_process.update_fundamental_data(symbol_id, symbol_id) 
            if success:
                fundamental_data = FundamentalData.query.filter_by(symbol_id=symbol_id).first()
                if fundamental_data:
                    return fundamental_data, 200
            analysis_ns.abort(404, f"No fundamental data found for symbol_id: {symbol_id} after attempted fetch.")
        return fundamental_data

@analysis_ns.route('/trigger_fundamental_update/<string:symbol_input>')
@analysis_ns.param('symbol_input', 'The stock symbol ID (Persian short name) or ISIN')
class TriggerFundamentalUpdate(Resource):
    @jwt_required()
    @analysis_ns.response(200, 'Fundamental data update triggered successfully')
    @analysis_ns.response(500, 'Error during fundamental data update')
    @analysis_ns.doc(security='Bearer Auth')
    def post(self, symbol_input):
        """Trigger update for fundamental data for a symbol."""
        
        try:
            # 💥 اصلاح 1: ایجاد دیتابیس سشن برای ارسال به توابع سرویس
            with data_fetch_and_process.get_session_local() as db_session:
                
                # 💥 اصلاح 2: ارسال db_session به get_symbol_id (رفع TypeError)
                symbol_id = data_fetch_and_process.get_symbol_id(db_session, symbol_input)
                
                if not symbol_id:
                    analysis_ns.abort(404, f"Invalid symbol ID or name: {symbol_input}")

                current_app.logger.info(f"Triggered fundamental data update for symbol: {symbol_input} (ID: {symbol_id}).")
                
                # 💥 اصلاح 3: فراخوانی صحیح تابع آپدیت بنیادی با فیلتر نماد خاص
                fund_count, fund_msg = data_fetch_and_process.update_symbol_fundamental_data(
                    db_session=db_session,
                    specific_symbols_list=[symbol_input],
                    limit=1
                )
                
                if fund_count > 0:
                    return {"message": fund_msg}, 200
                else:
                    # اگر سرویس آپدیت بنیادی نتوانست آپدیت کند اما پیام خطا داد
                    return {"message": fund_msg}, 500

        except Exception as e:
            current_app.logger.error(f"❌ خطای کلی در API آپدیت بنیادی برای {symbol_input}: {e}", exc_info=True)
            analysis_ns.abort(500, f"Critical error during fundamental data update: {e}")




@analysis_ns.route('/analyze_technical_indicators/<string:symbol_input>')
@analysis_ns.param('symbol_input', 'The stock symbol ID (Persian short name) or ISIN')
@analysis_ns.param('days', 'Number of recent days to fetch and analyze (default: 365)')
class TechnicalIndicatorsResource(Resource):
    @jwt_required()
    @analysis_ns.marshal_list_with(technical_indicator_model)
    @analysis_ns.response(200, 'Technical indicators calculated successfully')
    @analysis_ns.response(404, 'No historical data found for the symbol')
    @analysis_ns.doc(security='Bearer Auth')
    def get(self, symbol_input):
        """
        Fetches historical data, calculates various technical indicators,
        saves them to the database, and returns the recent results.
        """
        symbol_id = data_fetch_and_process.get_symbol_id(symbol_input)
        if not symbol_id:
            analysis_ns.abort(404, f"Invalid symbol ID or name: {symbol_input}")

        # ✅ FIX: تبدیل symbol_id به رشته برای کوئری گرفتن از DB
        # این کار مطابقت با نوع db.String در مدل TechnicalIndicatorData را تضمین می‌کند.
        symbol_id_str = str(symbol_id)

        parser = reqparse.RequestParser()
        parser.add_argument('days', type=int, default=365, help='Number of recent days to fetch and analyze')
        args = parser.parse_args()
        days = args['days']

        # Call the service function to analyze and save technical data
        # symbol_id (integer/original type) برای تابع سرویس فرستاده می‌شود.
        success, msg = data_fetch_and_process.analyze_technical_data_for_symbol(symbol_id, symbol_id, limit_days=days)
        if not success:
            analysis_ns.abort(404, f"Failed to analyze technical data for symbol_id: {symbol_id}. Reason: {msg}")

        # Fetch the newly saved technical data from the database
        # ✅ استفاده از symbol_id_str (مقدار رشته‌ای) در فیلتر
        technical_data_records = TechnicalIndicatorData.query.filter_by(symbol_id=symbol_id_str)\
                                                     .order_by(TechnicalIndicatorData.jdate.desc())\
                                                     .limit(days).all()
        
        if not technical_data_records:
            analysis_ns.abort(404, f"No technical indicator data found for symbol_id: {symbol_id}. This might indicate a saving issue.")

        # Convert records to a list of dictionaries for marshalling
        return [rec.__dict__ for rec in technical_data_records]


# --- NEW API Resource for ML Predictions ---
@analysis_ns.route('/ml-predictions')
class MLPredictionListResource(Resource):
    @analysis_ns.doc(security='Bearer Auth', params={'symbol_id': 'Optional: Filter predictions by symbol ID'})
    @jwt_required()
    @analysis_ns.marshal_list_with(ml_prediction_model)
    @analysis_ns.response(200, 'ML predictions retrieved successfully.')
    @analysis_ns.response(404, 'No ML prediction found for the symbol (if symbol_id provided).')
    @analysis_ns.response(500, 'Error retrieving ML predictions.')
    def get(self):
        """
        Retrieves ML predictions. Can be filtered by symbol_id.
        If no symbol_id is provided, returns all predictions.
        """
        symbol_id = request.args.get('symbol_id')
        if symbol_id:
            current_app.logger.info(f"API request for ML prediction for symbol: {symbol_id}")
            prediction = get_ml_predictions_for_symbol(symbol_id)
            if prediction:
                # get_ml_predictions_for_symbol returns a single dict, marshal_list_with expects a list
                return [prediction], 200 
            else:
                return {'message': f'No ML prediction found for symbol_id: {symbol_id}'}, 404
        else:
            current_app.logger.info("API request for all ML predictions.")
            predictions = get_all_ml_predictions()
            return predictions, 200





#init اولیه نمادها و بروزرسانی دوره‌ای لیست.
@analysis_ns.route('/init-symbols')
class InitSymbolsResource(Resource):
    @jwt_required()
    def post(self):
        """Initialize symbols in database"""
        count, msg = populate_symbols_into_db()
        return {
            "success": True if count > 0 else False,
            "message": msg,
            "count": count
        }, 200

#For Debug
@analysis_ns.route('/debug/tehran-stocks-structure')
class DebugTehranStocksStructureResource(Resource):
    def get(self):
        """بررسی ساختار داده‌های tehran-stocks"""
        from services.symbol_initializer import debug_tehran_stocks_structure
        df = debug_tehran_stocks_structure()
        
        if df is not None and not df.empty:
            return {
                'columns': list(df.columns),
                'row_count': len(df),
                'sample_data': df.iloc[0].to_dict() if not df.empty else {}
            }, 200
        else:
            return {'error': 'Failed to fetch data from tehran-stocks'}, 500




# Market Summary
@analysis_ns.route('/market-summary')
class MarketSummaryResource(Resource):
    def get(self):
        """
        Generates and returns a structured summary of the market analysis.
        Provides a daily or weekly report in JSON format.
        """
        current_app.logger.info("API request for market summary.")
        
        # ✅ این تابع اکنون یک دیکشنری کامل (نه فقط متن) برمی‌گرداند
        summary_data = market_analysis_service.generate_market_summary()
        
        # ✅ دیکشنری را مستقیماً برگردانید. 
        # Flask-RESTX به صورت خودکار آن را به JSON تبدیل می‌کند.
        return summary_data, 200
