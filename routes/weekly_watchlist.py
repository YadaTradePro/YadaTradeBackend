# routes/weekly_watchlist.py
from flask_restx import Namespace, Resource, fields
from flask_jwt_extended import jwt_required
from flask import current_app
import logging 
import uuid 

# تنظیمات لاگینگ برای این ماژول
logger = logging.getLogger(__name__)

from services import weekly_watchlist_service # Import the service

weekly_watchlist_ns = Namespace('weekly_watchlist', description='Weekly Watchlist operations')
weekly_watchlist_result_model = weekly_watchlist_ns.model('WeeklyWatchlistResultModel', {
    'signal_unique_id': fields.String(description='Unique ID for the signal'),
    #'symbol': fields.String(description='Symbol ID'), # Corrected to use 'symbol' directly from the object
    'company_name': fields.String(description='Company Name'),
    'symbol_name': fields.String(description='Symbol Name'),
    'entry_price': fields.Float(description='Entry Price'),
    'jentry_date': fields.String(description='Jalali Entry Date'),
    'outlook': fields.String(description='Outlook'),
    'reason': fields.String(description='Reason'),
    'probability_percent': fields.Float(description='Probability Percent'),
    'status': fields.String(description='Status (active, closed_win, closed_loss, closed_neutral)'),
    'exit_price': fields.Float(description='Exit Price'),
    'jexit_date': fields.String(description='Jalali Exit Date'),
    'profit_loss_percentage': fields.Float(description='Profit/Loss Percentage'),
    'created_at': fields.String(description='Creation Timestamp'),
    'updated_at': fields.String(description='Last Updated Timestamp')
})
weekly_watchlist_response_model = weekly_watchlist_ns.model('WeeklyWatchlistResponse', {
    'top_watchlist_stocks': fields.List(fields.Nested(weekly_watchlist_result_model), description='List of top Weekly Watchlist stocks'),
    'last_updated': fields.String(description='Timestamp of last update')
})

@weekly_watchlist_ns.route('/run_selection')
class RunWeeklyWatchlistSelectionResource(Resource):
    @weekly_watchlist_ns.doc(security='Bearer Auth')
    @jwt_required() 
    def post(self):
        logger.info("Received manual request to run Weekly Watchlist selection.")
        try:
            selected_symbols, message = weekly_watchlist_service.run_weekly_watchlist_selection() 
            return {"message": message, "selected_symbols": selected_symbols}, 200 
        except Exception as e:
            logger.error(f"Error running Weekly Watchlist selection: {e}", exc_info=True)
            return {"message": f"An error occurred during Weekly Watchlist selection: {str(e)}"}, 500

@weekly_watchlist_ns.route('/evaluate_performance')
class EvaluateWeeklyWatchlistPerformanceResource(Resource):
    @weekly_watchlist_ns.doc(security='Bearer Auth')
    @jwt_required() 
    def post(self):
        logger.info("Received manual request to evaluate Weekly Watchlist performance.")
        try:
            success, message = weekly_watchlist_service.evaluate_weekly_watchlist_performance() 
            if success:
                return {"message": message}, 200
            else:
                return {"message": message}, 500
        except Exception as e:
            logger.error(f"Error evaluating Weekly Watchlist performance: {e}", exc_info=True)
            return {"message": f"An error occurred during Weekly Watchlist performance evaluation: {str(e)}"}, 500
    
@weekly_watchlist_ns.route('/results')
class GetWeeklyWatchlistResultsResource(Resource):
    @weekly_watchlist_ns.doc(security='Bearer Auth')
    @jwt_required() 
    @weekly_watchlist_ns.marshal_with(weekly_watchlist_response_model)
    def get(self):
        logger.info("API call: Retrieving Weekly Watchlist Results.")
        try:
            results = weekly_watchlist_service.get_weekly_watchlist_results()
            return results, 200
        except Exception as e:
            logger.error(f"Error retrieving Weekly Watchlist results: {e}", exc_info=True)
            return {"message": f"An error occurred while retrieving Weekly Watchlist results: {str(e)}"}, 500
