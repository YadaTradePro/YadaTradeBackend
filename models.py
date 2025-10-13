# -*- coding: utf-8 -*-
# models.py
from extensions import db
from datetime import datetime, date
import uuid
import json
from sqlalchemy import UniqueConstraint

class User(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(80), unique=True, nullable=False)
    email = db.Column(db.String(120), unique=True, nullable=False)
    hashed_password = db.Column(db.String(128), nullable=False)

    def __repr__(self):
        return f'<User {self.username}>'

class ComprehensiveSymbolData(db.Model):
    __tablename__ = 'comprehensive_symbol_data'
    id = db.Column(db.Integer, primary_key=True)
    symbol_id = db.Column(db.String(50), unique=True, nullable=False)  # e.g., InsCode یا index
    symbol_name = db.Column(db.String(100), nullable=False)  # نام کوتاه نماد
    company_name = db.Column(db.String(255), nullable=True)  # نام کامل شرکت
    isin = db.Column(db.String(50), nullable=True)  # کد ISIN
    tse_index = db.Column(db.String(50), nullable=True)  # شناسه TSETMC
    market_type = db.Column(db.String(50), nullable=True)  # نوع بازار (بورس، فرابورس، ...)
    group_name = db.Column(db.String(100), nullable=True)  # گروه صنعتی
    base_volume = db.Column(db.Float, nullable=True)  # حجم مبنا
    
    # اطلاعات بنیادی
    eps = db.Column(db.Float, nullable=True)  # سود هر سهم
    p_e_ratio = db.Column(db.Float, nullable=True)  # نسبت P/E
    p_s_ratio = db.Column(db.Float, nullable=True)  # نسبت P/S (جدید)
    nav = db.Column(db.Float, nullable=True)  # خالص ارزش دارایی‌ها (جدید)
    float_shares = db.Column(db.Float, nullable=True)  # درصد سهام شناور
    #total_shares = db.Column(db.BigInteger, nullable=True)  # تعداد کل سهام (جدید)
    market_cap = db.Column(db.BigInteger, nullable=True)  # ارزش بازار (جدید)
    
    # اطلاعات تکمیلی
    industry = db.Column(db.String(100), nullable=True)  # صنعت
    capital = db.Column(db.Float, nullable=True)  # سرمایه
    fiscal_year = db.Column(db.String(10), nullable=True)  # سال مالی (جدید)
    flow = db.Column(db.String(50), nullable=True)  # بازار (بورس/فرابورس)
    state = db.Column(db.String(50), nullable=True)  # وضعیت نماد (جدید)
    
    # مدیریت داده‌ها
    last_historical_update_date = db.Column(db.Date, nullable=True)
    last_fundamental_update_date = db.Column(db.Date, nullable=True)
    last_realtime_update = db.Column(db.DateTime, nullable=True)  # آخرین بروزرسانی لحظه‌ای (جدید)
    created_at = db.Column(db.DateTime, default=datetime.now)
    updated_at = db.Column(db.DateTime, default=datetime.now, onupdate=datetime.now)

    def __repr__(self):
        return f'<ComprehensiveSymbolData {self.symbol_name} ({self.symbol_id})>'

class HistoricalData(db.Model):
    __tablename__ = 'stock_data'
    id = db.Column(db.Integer, primary_key=True)
    symbol_id = db.Column(
        db.String(50), 
        db.ForeignKey('comprehensive_symbol_data.symbol_id'), 
        nullable=False
    )
    symbol_name = db.Column(db.String(255), nullable=False)
    date = db.Column(db.Date, nullable=False)
    jdate = db.Column(db.String(10), nullable=False)

    # Prices
    open = db.Column(db.Float)
    high = db.Column(db.Float)
    low = db.Column(db.Float)
    close = db.Column(db.Float)
    final = db.Column(db.Float)
    yesterday_price = db.Column(db.Float)

    # Volume and Value
    volume = db.Column(db.BigInteger)
    value = db.Column(db.BigInteger)
    num_trades = db.Column(db.Integer)

    # Price Changes
    plc = db.Column(db.Float)
    plp = db.Column(db.Float)
    pcc = db.Column(db.Float)
    pcp = db.Column(db.Float)

    # Market Value
    mv = db.Column(db.BigInteger)

    # Real/Legal Shareholder Data
    buy_count_i = db.Column(db.Integer)
    buy_count_n = db.Column(db.Integer)
    sell_count_i = db.Column(db.Integer)
    sell_count_n = db.Column(db.Integer)
    buy_i_volume = db.Column(db.BigInteger)
    buy_n_volume = db.Column(db.BigInteger)
    sell_i_volume = db.Column(db.BigInteger)
    sell_n_volume = db.Column(db.BigInteger)

    # Order Book Data
    zd1 = db.Column(db.Integer)
    qd1 = db.Column(db.BigInteger)
    pd1 = db.Column(db.Float)
    zo1 = db.Column(db.Integer)
    qo1 = db.Column(db.BigInteger)
    po1 = db.Column(db.Float)

    zd2 = db.Column(db.Integer)
    qd2 = db.Column(db.BigInteger)
    pd2 = db.Column(db.Float)
    zo2 = db.Column(db.Integer)
    qo2 = db.Column(db.BigInteger)
    po2 = db.Column(db.Float)

    zd3 = db.Column(db.Integer)
    qd3 = db.Column(db.BigInteger)
    pd3 = db.Column(db.Float)
    zo3 = db.Column(db.Integer)
    qo3 = db.Column(db.BigInteger)
    po3 = db.Column(db.Float)

    zd4 = db.Column(db.Integer)
    qd4 = db.Column(db.BigInteger)
    pd4 = db.Column(db.Float)
    zo4 = db.Column(db.Integer)
    qo4 = db.Column(db.BigInteger)
    po4 = db.Column(db.Float)

    zd5 = db.Column(db.Integer)
    qd5 = db.Column(db.BigInteger)
    pd5 = db.Column(db.Float)
    zo5 = db.Column(db.Integer)
    qo5 = db.Column(db.BigInteger)
    po5 = db.Column(db.Float)
    
    created_at = db.Column(db.DateTime, default=datetime.now)
    updated_at = db.Column(db.DateTime, default=datetime.now, onupdate=datetime.now)

    def __repr__(self):
        return f'<HistoricalData {self.symbol_name} - {self.date}>'

class SignalsPerformance(db.Model):
    __tablename__ = 'signals_performance'
    id = db.Column(db.Integer, primary_key=True)
    
    signal_id = db.Column(db.String(36), unique=True, nullable=False, default=lambda: str(uuid.uuid4()))
    
    # تغییر: استفاده از Integer برای ForeignKey
    symbol_id = db.Column(
        db.String(50), 
        db.ForeignKey('comprehensive_symbol_data.symbol_id'), 
        nullable=False
    )
    symbol_name = db.Column(db.String(255), nullable=False)
    
    signal_source = db.Column(db.String(50), nullable=False)
    
    entry_date = db.Column(db.Date, nullable=False)
    jentry_date = db.Column(db.String(10), nullable=False)
    entry_price = db.Column(db.Float, nullable=False)
    
    outlook = db.Column(db.String(50), nullable=True)
    reason = db.Column(db.Text, nullable=True)
    probability_percent = db.Column(db.Float, nullable=True)

    exit_date = db.Column(db.Date, nullable=True)
    jexit_date = db.Column(db.String(10), nullable=True)
    exit_price = db.Column(db.Float, nullable=True)
    profit_loss_percent = db.Column(db.Float, nullable=True)
    status = db.Column(db.String(50), default='active', nullable=True)
    
    created_at = db.Column(db.DateTime, default=datetime.now)
    evaluated_at = db.Column(db.DateTime, default=datetime.now, onupdate=datetime.now)
    updated_at = db.Column(db.DateTime, default=datetime.now, onupdate=datetime.now)

    symbol_data = db.relationship('ComprehensiveSymbolData', backref=db.backref('signals_performance_records', lazy=True))

    __table_args__ = (db.UniqueConstraint('signal_id', name='_signal_id_uc'),)

    def __repr__(self):
        return f'<SignalsPerformance {self.symbol_name} - {self.signal_source} - {self.jentry_date}>'

class AggregatedPerformance(db.Model):
    __tablename__ = 'aggregated_performance'
    id = db.Column(db.Integer, primary_key=True)
    report_date = db.Column(db.String(10), nullable=False)
    period_type = db.Column(db.String(20), nullable=False)
    signal_source = db.Column(db.String(50), nullable=False)
    total_signals = db.Column(db.Integer, nullable=False, default=0)
    successful_signals = db.Column(db.Integer, nullable=False, default=0)
    win_rate = db.Column(db.Float, nullable=False, default=0.0)
    total_profit_percent = db.Column(db.Float, nullable=False, default=0.0)
    total_loss_percent = db.Column(db.Float, nullable=False, default=0.0)
    
    average_profit_per_win = db.Column(db.Float, nullable=False, default=0.0)
    average_loss_per_loss = db.Column(db.Float, nullable=False, default=0.0)
    net_profit_percent = db.Column(db.Float, nullable=False, default=0.0)

    created_at = db.Column(db.DateTime, default=datetime.now)
    updated_at = db.Column(db.DateTime, default=datetime.now, onupdate=datetime.now)

    __table_args__ = (db.UniqueConstraint('report_date', 'period_type', 'signal_source', name='_report_period_source_uc'),)

    def __repr__(self):
        return f"<AggregatedPerformance {self.signal_source} {self.period_type} {self.report_date}>"

class FundamentalData(db.Model):
    __tablename__ = 'fundamental_data'
    id = db.Column(db.Integer, primary_key=True)
    symbol_id = db.Column(
        db.String(50), 
        db.ForeignKey('comprehensive_symbol_data.symbol_id'), 
        nullable=False
    )
    
    eps = db.Column(db.Float)
    pe = db.Column(db.Float)
    group_pe_ratio = db.Column(db.Float)
    psr = db.Column(db.Float)
    p_s_ratio = db.Column(db.Float)
    market_cap = db.Column(db.BigInteger)
    base_volume = db.Column(db.BigInteger)
    float_shares = db.Column(db.Float)
    created_at = db.Column(db.DateTime, default=datetime.now)
    updated_at = db.Column(db.DateTime, default=datetime.now, onupdate=datetime.now)

    def __repr__(self):
        return f'<FundamentalData {self.symbol_id}>'

class SentimentData(db.Model):
    __tablename__ = 'sentiment_data'
    id = db.Column(db.Integer, primary_key=True)
    symbol_id = db.Column(
        db.String(50), 
        db.ForeignKey('comprehensive_symbol_data.symbol_id'), 
        nullable=False
    )
    date = db.Column(db.String(10), nullable=False)
    
    sentiment_score = db.Column(db.Float)
    news_count = db.Column(db.Integer)

    __table_args__ = (db.UniqueConstraint('symbol_id', 'date', name='_symbol_date_sentiment_uc'),)

    def __repr__(self):
        return f'<SentimentData {self.symbol_id} - {self.date}>'

class TechnicalIndicatorData(db.Model):
    __tablename__ = 'technical_indicator_data'
    id = db.Column(db.Integer, primary_key=True)
    symbol_id = db.Column(
        db.String(50), 
        db.ForeignKey('comprehensive_symbol_data.symbol_id'), 
        nullable=False
    )
    jdate = db.Column(db.String(10), nullable=False)
    close_price = db.Column(db.Float)
    
    # --- اندیکاتورهای استاندارد ---
    RSI = db.Column(db.Float)
    MACD = db.Column(db.Float)
    MACD_Signal = db.Column(db.Float)
    MACD_Hist = db.Column(db.Float)
    SMA_20 = db.Column(db.Float)
    SMA_50 = db.Column(db.Float)
    Bollinger_High = db.Column(db.Float)
    Bollinger_Low = db.Column(db.Float)
    Bollinger_MA = db.Column(db.Float)
    Volume_MA_20 = db.Column(db.Float)
    ATR = db.Column(db.Float)
    
    # --- NEW: ستون‌های جدید برای اندیکاتورهای پیشرفته ---
    Stochastic_K = db.Column(db.Float)
    Stochastic_D = db.Column(db.Float)
    
    squeeze_on = db.Column(db.Boolean) # آیا در حالت فشردگی است؟
    
    halftrend_signal = db.Column(db.Integer) # 1 برای خرید، -1 برای فروش، 0 برای هیچی
    
    resistance_level_50d = db.Column(db.Float) # سطح مقاومت ۵۰ روزه
    resistance_broken = db.Column(db.Boolean) # آیا مقاومت شکسته شده؟

    created_at = db.Column(db.DateTime, default=datetime.now)
    updated_at = db.Column(db.DateTime, default=datetime.now, onupdate=datetime.now)

    __table_args__ = (db.UniqueConstraint('symbol_id', 'jdate', name='_symbol_jdate_tech_uc'),)

    def __repr__(self):
        return f'<TechnicalIndicatorData {self.symbol_id} - {self.jdate}>'

class CandlestickPatternDetection(db.Model):
    __tablename__ = 'candlestick_pattern_detection'
    id = db.Column(db.Integer, primary_key=True)
    symbol_id = db.Column(
        db.String(50), 
        db.ForeignKey('comprehensive_symbol_data.symbol_id'), 
        nullable=False
    )
    jdate = db.Column(db.String(10), nullable=False)
    pattern_name = db.Column(db.String(100), nullable=False)
    
    created_at = db.Column(db.DateTime, default=datetime.now)
    updated_at = db.Column(db.DateTime, default=datetime.now, onupdate=datetime.now)

    __table_args__ = (db.UniqueConstraint('symbol_id', 'jdate', 'pattern_name', name='_symbol_jdate_pattern_uc'),)

    def __repr__(self):
        return f'<CandlestickPatternDetection {self.symbol_id} - {self.jdate} - {self.pattern_name}>'

class TSETMCFilterResult(db.Model):
    __tablename__ = 'tsetmc_filter_result'
    id = db.Column(db.Integer, primary_key=True)
    symbol_id = db.Column(
        db.String(50), 
        db.ForeignKey('comprehensive_symbol_data.symbol_id'), 
        nullable=False
    )
    jdate = db.Column(db.String(10), nullable=False)
    filter_name = db.Column(db.String(100), nullable=False)
    
    created_at = db.Column(db.DateTime, default=datetime.now)
    updated_at = db.Column(db.DateTime, default=datetime.now, onupdate=datetime.now)

    __table_args__ = (db.UniqueConstraint('symbol_id', 'jdate', 'filter_name', name='_symbol_jdate_filter_uc'),)

    def __repr__(self):
        return f'<TSETMCFilterResult {self.symbol_id} - {self.jdate} - {self.filter_name}>'

class FinancialRatiosData(db.Model):
    __tablename__ = 'financial_ratios_data'
    id = db.Column(db.Integer, primary_key=True)
    symbol_id = db.Column(
        db.String(50), 
        db.ForeignKey('comprehensive_symbol_data.symbol_id'), 
        nullable=False
    )
    fiscal_year = db.Column(db.String(10), nullable=False)
    ratio_name = db.Column(db.String(100), nullable=False)
    ratio_value = db.Column(db.Float)
    
    created_at = db.Column(db.DateTime, default=datetime.now)
    updated_at = db.Column(db.DateTime, default=datetime.now, onupdate=datetime.now)

    __table_args__ = (db.UniqueConstraint('symbol_id', 'fiscal_year', 'ratio_name', name='_symbol_fiscal_ratio_uc'),)

    def __repr__(self):
        return f'<FinancialRatiosData {self.symbol_id} - {self.fiscal_year} - {self.ratio_name}>'

class MLPrediction(db.Model):
    __tablename__ = 'ml_predictions'

    id = db.Column(db.Integer, primary_key=True)
    symbol_id = db.Column(
        db.String(50), 
        db.ForeignKey('comprehensive_symbol_data.symbol_id'), 
        nullable=False
    )
    symbol_name = db.Column(db.String(255), nullable=False)
    
    prediction_date = db.Column(db.Date, nullable=False)
    jprediction_date = db.Column(db.String(10), nullable=False)
    
    prediction_period_days = db.Column(db.Integer, default=7)
    
    predicted_trend = db.Column(db.String(50), nullable=False)
    prediction_probability = db.Column(db.Float, nullable=False)
    
    predicted_price_at_period_end = db.Column(db.Float, nullable=True)
    
    actual_price_at_period_end = db.Column(db.Float, nullable=True)
    actual_trend_outcome = db.Column(db.String(50), nullable=True)
    is_prediction_accurate = db.Column(db.Boolean, nullable=True)
    
    signal_source = db.Column(db.String(50), default='ML-Trend')
    model_version = db.Column(db.String(50), nullable=True)
    
    created_at = db.Column(db.DateTime, default=datetime.now)
    updated_at = db.Column(db.DateTime, default=datetime.now, onupdate=datetime.now)

    def __repr__(self):
        return f"<MLPrediction {self.symbol_id} - {self.jprediction_date} - {self.predicted_trend}>"

    def to_dict(self):
        return {
            'id': self.id,
            'symbol_id': self.symbol_id,
            'symbol_name': self.symbol_name,
            'prediction_date': self.prediction_date.strftime('%Y-%m-%d') if self.prediction_date else None,
            'jprediction_date': self.jprediction_date,
            'prediction_period_days': self.prediction_period_days,
            'predicted_trend': self.predicted_trend,
            'prediction_probability': self.prediction_probability,
            'predicted_price_at_period_end': self.predicted_price_at_period_end,
            'actual_price_at_period_end': self.actual_price_at_period_end,
            'actual_trend_outcome': self.actual_trend_outcome,
            'is_prediction_accurate': self.is_prediction_accurate,
            'signal_source': self.signal_source,
            'model_version': self.model_version,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None,
        }

class GoldenKeyResult(db.Model):
    __tablename__ = 'golden_key_results'
    id = db.Column(db.Integer, primary_key=True)
    symbol_id = db.Column(
        db.String(50), 
        db.ForeignKey('comprehensive_symbol_data.symbol_id'), 
        nullable=False
    )
    symbol_name = db.Column(db.String(100), nullable=False)
    jdate = db.Column(db.String(10), nullable=False)
    is_golden_key = db.Column(db.Boolean, default=False)
    score = db.Column(db.Integer, default=0)
    reason = db.Column(db.Text)
    timestamp = db.Column(db.DateTime, default=datetime.now)

    satisfied_filters = db.Column(db.Text)

    recommendation_price = db.Column(db.Float)
    recommendation_jdate = db.Column(db.String(10))


    #final_price = db.Column(db.Float)
    #profit_loss_percentage = db.Column(db.Float)
    
    #weekly_growth = db.Column(db.Float, nullable=True)

    status = db.Column(db.String(50), default='active', nullable=True)
    #probability_percent = db.Column(db.Float, nullable=True)

    #__table_args__ = (db.UniqueConstraint('symbol_id', 'jdate', name='_symbol_jdate_golden_key_uc'),)

    def __repr__(self):
        return f'<GoldenKeyResult {self.symbol_name} {self.jdate} (Score: {self.score})>'

class WeeklyWatchlistResult(db.Model):
    __tablename__ = 'weekly_watchlist_results'
    id = db.Column(db.Integer, primary_key=True)
    signal_unique_id = db.Column(db.String(36), unique=True, nullable=False, default=lambda: str(uuid.uuid4()))
    
    # تغییر: استفاده از Integer برای ForeignKey
    symbol_id = db.Column(
        db.String(50), 
        db.ForeignKey('comprehensive_symbol_data.symbol_id'), 
        nullable=False
    )
    symbol_name = db.Column(db.String(100), nullable=False)
    
    entry_price = db.Column(db.Float, nullable=False)
    entry_date = db.Column(db.Date, nullable=False)
    jentry_date = db.Column(db.String(10), nullable=False)
    outlook = db.Column(db.String(255))
    reason = db.Column(db.Text)
    probability_percent = db.Column(db.Float)
    created_at = db.Column(db.DateTime, default=datetime.now)
    updated_at = db.Column(db.DateTime, default=datetime.now, onupdate=datetime.now)

    status = db.Column(db.String(50), default='active', nullable=False)
    exit_price = db.Column(db.Float, nullable=True)
    exit_date = db.Column(db.Date, nullable=True)
    jexit_date = db.Column(db.String(10), nullable=True)
    profit_loss_percentage = db.Column(db.Float, nullable=True)

    def __repr__(self):
        return f"<WeeklyWatchlistResult {self.symbol_id} on {self.jentry_date}>"

class PotentialBuyQueueResult(db.Model):
    __tablename__ = 'potential_buy_queue_results'
    id = db.Column(db.Integer, primary_key=True)
    
    # تغییر: استفاده از Integer برای ForeignKey
    symbol_id = db.Column(
        db.String(50), 
        db.ForeignKey('comprehensive_symbol_data.symbol_id'), 
        nullable=False
    )
    symbol_name = db.Column(db.String(255), nullable=False)
    
    reason = db.Column(db.Text, nullable=True)
    jdate = db.Column(db.String(10), nullable=False)
    current_price = db.Column(db.Float, nullable=True)
    volume_change_percent = db.Column(db.Float, nullable=True)
    real_buyer_power_ratio = db.Column(db.Float, nullable=True)
    matched_filters = db.Column(db.Text, nullable=True)
    group_type = db.Column(db.String(50), nullable=True)
    timestamp = db.Column(db.DateTime, default=datetime.now)
    
    probability_percent = db.Column(db.Float, nullable=True)

    __table_args__ = (db.UniqueConstraint('symbol_id', 'jdate', name='_symbol_jdate_potential_queue_uc'),)

    def __repr__(self):
        return f'<PotentialBuyQueueResult {self.symbol_name} {self.jdate}>'




class DailySectorPerformance(db.Model):
    __tablename__ = 'daily_sector_performance'
    id = db.Column(db.Integer, primary_key=True)
    jdate = db.Column(db.String(10), nullable=False, index=True)
    sector_name = db.Column(db.String(255), nullable=False)
    total_trade_value = db.Column(db.BigInteger) # مجموع ارزش معاملات
    net_money_flow = db.Column(db.BigInteger) # مجموع ورود/خروج پول هوشمند
    # می‌توانید معیارهای دیگری مانند بازدهی میانگین را هم اضافه کنید
    rank = db.Column(db.Integer) # رتبه نهایی صنعت در آن روز

    __table_args__ = (db.UniqueConstraint('jdate', 'sector_name', name='uq_jdate_sector'),)




# حذف کلاس SymbolModel زیرا دیگر نیازی به آن نیست
# class SymbolModel(db.Model):
#     __tablename__ = 'symbols'
#     
#     id = db.Column(db.Integer, primary_key=True)
#     name = db.Column(db.String(50), unique=True, nullable=False)
#     tse_index = db.Column(db.String(20), nullable=False)
#     market = db.Column(db.String(50))
#     created_at = db.Column(db.DateTime, default=datetime.utcnow)
#     updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
#     
#     historical_data = db.relationship('HistoricalData', backref='symbol', lazy=True)
#     technical_indicators = db.relationship('TechnicalIndicatorData', backref='symbol', lazy=True)
#     fundamental_data = db.relationship('FundamentalData', backref='symbol', lazy=True)
#     
#     def __repr__(self):
#         return f'<Symbol {self.name}>'