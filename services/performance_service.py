# services/performance_service.py

from extensions import db
from models import SignalsPerformance, AggregatedPerformance, WeeklyWatchlistResult
from datetime import datetime, timedelta, date
import jdatetime
import pandas as pd
import logging
from sqlalchemy import func, and_

# Import utility functions
from services.utils import get_today_jdate_str, convert_gregorian_to_jalali

logger = logging.getLogger(__name__)

# --- Helper functions for safe date/datetime formatting ---
def safe_date_format(date_obj, fmt='%Y-%m-%d'):
    """
    Safely formats a date or datetime object to a string.
    Returns None if the object is not a valid date/datetime.
    """
    if isinstance(date_obj, (datetime, date)):
        return date_obj.strftime(fmt)
    return None

def safe_isoformat(datetime_obj):
    """
    Safely converts a datetime object to an ISO 8601 string.
    Returns None if the object is not a valid datetime.
    """
    if isinstance(datetime_obj, datetime):
        return datetime_obj.isoformat()
    return None
# --- End of Helper functions ---


def calculate_and_save_aggregated_performance(period_type='weekly'):
    """
    این تابع بازنویسی شده تا فقط عملکرد WeeklyWatchlistService را محاسبه و ذخیره کند.
    """
    signal_source = 'WeeklyWatchlistService' 
    logger.info(f"Calculating aggregated performance for '{signal_source}' ({period_type}).")

    today_jdate_str = get_today_jdate_str()
    
    start_jdate_str = None
    if period_type == 'weekly':
        try:
            today_greg = jdatetime.date.today().togregorian()
            start_date_greg = today_greg - timedelta(days=7)
            start_jdate_str = convert_gregorian_to_jalali(start_date_greg)
        except Exception as e:
            logger.error(f"Error determining start date for weekly period: {e}", exc_info=True)
            return False, "Error determining start date for weekly period."
    elif period_type == 'monthly':
        try:
            today_greg = jdatetime.date.today().togregorian()
            start_date_greg = today_greg - timedelta(days=30)
            start_jdate_str = convert_gregorian_to_jalali(start_date_greg)
        except Exception as e:
            logger.error(f"Error determining start date for monthly period: {e}", exc_info=True)
            return False, "Error determining start date for monthly period."
    elif period_type == 'annual':
        try:
            today_greg = jdatetime.date.today().togregorian()
            start_date_greg = today_greg - timedelta(days=365)
            start_jdate_str = convert_gregorian_to_jalali(start_date_greg)
        except Exception as e:
            logger.error(f"Error determining start date for annual period: {e}", exc_info=True)
            return False, "Error determining start date for annual period."
    else:
        return False, "Invalid period_type. Must be 'weekly', 'monthly', or 'annual'."

    query_conditions = [
        SignalsPerformance.status.in_(['closed_win', 'closed_loss', 'closed_neutral', 'closed_expired']),
        SignalsPerformance.signal_source == signal_source
    ]
    
    if start_jdate_str:
        query_conditions.append(SignalsPerformance.jentry_date >= start_jdate_str)

    signals_in_period = SignalsPerformance.query.filter(and_(*query_conditions)).all()

    if not signals_in_period:
        message = f"No closed signals found for {signal_source} ({period_type}) in the period starting {start_jdate_str}."
        logger.warning(message)
        total_signals, successful_signals, win_rate, total_profit_percent, total_loss_percent, average_profit_per_win, average_loss_per_loss, net_profit_percent = 0, 0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0
    else:
        total_signals = len(signals_in_period)
        successful_signals = sum(1 for s in signals_in_period if s.profit_loss_percent is not None and s.profit_loss_percent > 0)
        win_rate = (successful_signals / total_signals) * 100 if total_signals > 0 else 0.0
        total_profit_percent = sum(s.profit_loss_percent for s in signals_in_period if s.profit_loss_percent is not None and s.profit_loss_percent > 0)
        total_loss_percent = abs(sum(s.profit_loss_percent for s in signals_in_period if s.profit_loss_percent is not None and s.profit_loss_percent < 0))
        winning_signals = [s for s in signals_in_period if s.profit_loss_percent is not None and s.profit_loss_percent > 0]
        losing_signals = [s for s in signals_in_period if s.profit_loss_percent is not None and s.profit_loss_percent < 0]
        average_profit_per_win = (sum(s.profit_loss_percent for s in winning_signals) / len(winning_signals)) if winning_signals else 0.0
        average_loss_per_loss = (sum(s.profit_loss_percent for s in losing_signals) / len(losing_signals)) if losing_signals else 0.0
        net_profit_percent = total_profit_percent - total_loss_percent

    existing_agg_perf = AggregatedPerformance.query.filter_by(
        report_date=get_today_jdate_str(),
        period_type=period_type,
        signal_source=signal_source
    ).first()

    if existing_agg_perf:
        existing_agg_perf.total_signals = total_signals
        existing_agg_perf.successful_signals = successful_signals
        existing_agg_perf.win_rate = win_rate
        existing_agg_perf.total_profit_percent = total_profit_percent
        existing_agg_perf.total_loss_percent = total_loss_percent
        existing_agg_perf.average_profit_per_win = average_profit_per_win
        existing_agg_perf.average_loss_per_loss = average_loss_per_loss
        existing_agg_perf.net_profit_percent = net_profit_percent
        existing_agg_perf.updated_at = datetime.now()
        db.session.add(existing_agg_perf)
        logger.info(f"Updated aggregated performance for {signal_source} ({period_type}) on {get_today_jdate_str()}.")
    else:
        new_agg_perf = AggregatedPerformance(
            report_date=get_today_jdate_str(),
            period_type=period_type,
            signal_source=signal_source,
            total_signals=total_signals,
            successful_signals=successful_signals,
            win_rate=win_rate,
            total_profit_percent=total_profit_percent,
            total_loss_percent=total_loss_percent,
            average_profit_per_win=average_profit_per_win,
            average_loss_per_loss=average_loss_per_loss,
            net_profit_percent=net_profit_percent,
            created_at=datetime.now()
        )
        db.session.add(new_agg_perf)
        logger.info(f"Created new aggregated performance record for {signal_source} ({period_type}) on {get_today_jdate_str()}.")
    
    try:
        db.session.commit()
        message = f"Aggregated performance for {signal_source} ({period_type}) calculated successfully. Win Rate: {win_rate:.2f}%."
        logger.info(message)
        return True, message
    except Exception as e:
        db.session.rollback()
        error_message = f"Error during aggregated performance calculation: {e}"
        logger.error(error_message, exc_info=True)
        return False, error_message

def get_aggregated_performance_reports(period_type=None, signal_source=None):
    logger.info(f"Retrieving aggregated performance reports (Period: {period_type}, Source: {signal_source}).")
    
    query = AggregatedPerformance.query
    
    if period_type:
        query = query.filter_by(period_type=period_type)
    if signal_source:
        query = query.filter_by(signal_source=signal_source)
        
    reports = query.order_by(AggregatedPerformance.report_date.desc(), AggregatedPerformance.created_at.desc()).all()
    
    output = []
    for r in reports:
        output.append({
            'report_id': r.id,
            'report_date': r.report_date,
            'period_type': r.period_type,
            'signal_source': r.signal_source,
            'total_signals': r.total_signals,
            'successful_signals': r.successful_signals,
            'win_rate': r.win_rate,
            'total_profit_percent': r.total_profit_percent,
            'total_loss_percent': r.total_loss_percent,
            'net_profit_percent': r.net_profit_percent,
            'average_profit_per_win': r.average_profit_per_win,
            'average_loss_per_loss': r.average_loss_per_loss,
            'created_at': safe_date_format(r.created_at, '%Y-%m-%d %H:%M:%S'),
            'updated_at': safe_date_format(r.updated_at, '%Y-%m-%d %H:%M:%S')
        })
    
    logger.info(f"Retrieved {len(output)} aggregated performance reports.")
    return output


def get_overall_performance_summary():
    logger.info("Calculating application performance summary based on WeeklyWatchlistService.")
    
    signal_source = 'WeeklyWatchlistService'

    all_signals = SignalsPerformance.query.filter(
        SignalsPerformance.status.in_(['closed_win', 'closed_loss', 'closed_neutral', 'closed_expired']),
        SignalsPerformance.signal_source == signal_source
    ).all()
    
    overall_summary_data = {
        "total_signals_evaluated": 0,
        "overall_win_rate": 0.0,
        "average_profit_per_win_overall": 0.0,
        "average_loss_per_loss_overall": 0.0,
        "overall_net_profit_percent": 0.0
    }
    signals_by_source_data = {}

    if all_signals:
        df = pd.DataFrame([s.__dict__ for s in all_signals]).drop(columns=['_sa_instance_state'], errors='ignore')
        df['profit_loss_percent'] = pd.to_numeric(df['profit_loss_percent'], errors='coerce').fillna(0)

        overall_summary_data["total_signals_evaluated"] = len(df)
        total_wins = df[df['profit_loss_percent'] > 0].shape[0]
        
        overall_summary_data["overall_win_rate"] = (total_wins / len(df)) * 100 if len(df) > 0 else 0.0
        overall_summary_data["overall_net_profit_percent"] = df['profit_loss_percent'].sum()

        winning_signals_pl = df[df['profit_loss_percent'] > 0]['profit_loss_percent']
        losing_signals_pl = df[df['profit_loss_percent'] < 0]['profit_loss_percent']

        overall_summary_data["average_profit_per_win_overall"] = winning_signals_pl.mean() if not winning_signals_pl.empty else 0.0
        overall_summary_data["average_loss_per_loss_overall"] = losing_signals_pl.mean() if not losing_signals_pl.empty else 0.0
        
        for source in df['signal_source'].unique():
            source_df = df[df['signal_source'] == source]
            source_total = len(source_df)
            source_wins = source_df[source_df['profit_loss_percent'] > 0].shape[0]
            source_losses = source_df[source_df['profit_loss_percent'] < 0].shape[0]
            source_neutral = source_df[source_df['profit_loss_percent'] == 0].shape[0]
            source_win_rate = (source_wins / source_total) * 100 if source_total > 0 else 0.0
            source_net_profit = source_df['profit_loss_percent'].sum()
            
            signals_by_source_data[source] = {
                "total_signals": source_total,
                "wins": source_wins,
                "losses": source_losses,
                "neutral": source_neutral,
                "win_rate": source_win_rate,
                "net_profit_percent": source_net_profit
            }

    today_jdate_str = get_today_jdate_str()

    latest_weekly_report = AggregatedPerformance.query.filter_by(
        report_date=today_jdate_str,
        period_type='weekly',
        signal_source=signal_source
    ).first()

    latest_monthly_report = AggregatedPerformance.query.filter_by(
        report_date=today_jdate_str,
        period_type='monthly',
        signal_source=signal_source
    ).first()

    annual_profit_loss = get_annual_profit_loss_summary()

    summary = {
        "overall_performance": overall_summary_data,
        "signals_by_source": signals_by_source_data,
        "weekly_performance": {
            "win_rate": latest_weekly_report.win_rate if latest_weekly_report else 0.0,
            "net_profit_percent": latest_weekly_report.net_profit_percent if latest_weekly_report else 0.0
        },
        "monthly_performance": {
            "win_rate": latest_monthly_report.win_rate if latest_monthly_report else 0.0,
            "net_profit_percent": latest_monthly_report.net_profit_percent if latest_monthly_report else 0.0
        },
        "annual_profit_loss": annual_profit_loss,
        "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    
    logger.info("Application performance summary calculated successfully.")
    return summary

def get_annual_profit_loss_summary():
    logger.info("Calculating annual profit/loss summary for WeeklyWatchlistService.")
    
    current_jalali_year = jdatetime.date.today().year
    start_of_jalali_year_str = f"{current_jalali_year}-01-01"
    signal_source = 'WeeklyWatchlistService'

    annual_signals = SignalsPerformance.query.filter(
        SignalsPerformance.status.in_(['closed_win', 'closed_loss', 'closed_neutral', 'closed_expired']),
        SignalsPerformance.jentry_date >= start_of_jalali_year_str,
        SignalsPerformance.signal_source == signal_source
    ).all()

    total_annual_profit_loss_percent = 0.0
    if annual_signals:
        df_annual = pd.DataFrame([s.__dict__ for s in annual_signals]).drop(columns=['_sa_instance_state'], errors='ignore')
        df_annual['profit_loss_percent'] = pd.to_numeric(df_annual['profit_loss_percent'], errors='coerce').fillna(0)
        total_annual_profit_loss_percent = df_annual['profit_loss_percent'].sum()
    
    logger.info(f"Annual profit/loss from {start_of_jalali_year_str} to {get_today_jdate_str()}: {total_annual_profit_loss_percent:.2f}%")
    return total_annual_profit_loss_percent


def get_detailed_signals_performance(status_filter=None, period_filter=None):
    logger.info(f"Retrieving detailed SignalsPerformance records (Status: {status_filter}, Period: {period_filter}).")
    query = SignalsPerformance.query

    if status_filter:
        query = query.filter(SignalsPerformance.status == status_filter)

    if period_filter == 'previous_week':
        today_greg = jdatetime.date.today().togregorian()
        end_date_greg = today_greg - timedelta(days=7)
        start_date_greg = today_greg - timedelta(days=14)

        end_jdate_str = convert_gregorian_to_jalali(end_date_greg)
        start_jdate_str = convert_gregorian_to_jalali(start_date_greg)

        if start_jdate_str and end_jdate_str:
            query = query.filter(
                SignalsPerformance.jexit_date >= start_jdate_str,
                SignalsPerformance.jexit_date <= end_jdate_str,
                SignalsPerformance.status.in_(['closed_win', 'closed_loss', 'closed_neutral', 'closed_expired'])
            )
            logger.info(f"Filtering for previous week: jexit_date from {start_jdate_str} to {end_jdate_str}")
        else:
            logger.warning("Could not determine Jalali date range for 'previous_week' filter.")
            return []
    
    # FIX: The line below was causing the SyntaxError. It has been corrected.
    signals = query.order_by(SignalsPerformance.exit_date.desc(), SignalsPerformance.created_at.desc()).all()

    output = []
    for s in signals:
        output.append({
            'signal_id': s.signal_id,
            'symbol_id': s.symbol_id,
            'symbol_name': s.symbol_name,
            'outlook': s.outlook,
            'reason': s.reason,
            'entry_price': s.entry_price,
            'jentry_date': s.jentry_date,
            'entry_date': safe_date_format(s.entry_date),
            'exit_price': s.exit_price,
            'jexit_date': s.jexit_date,
            'exit_date': safe_date_format(s.exit_date),
            'profit_loss_percent': s.profit_loss_percent,
            'status': s.status,
            'signal_source': s.signal_source,
            'probability_percent': s.probability_percent,
            'created_at': safe_date_format(s.created_at, '%Y-%m-%d %H:%M:%S'),
            'updated_at': safe_date_format(s.updated_at, '%Y-%m-%d %H:%M:%S')
        })
    
    logger.info(f"Retrieved {len(output)} detailed SignalsPerformance records.")
    return output