# routes/performance.py
from flask_restx import Namespace, Resource, fields, reqparse
from flask_jwt_extended import jwt_required
from flask import current_app, request
import logging

logger = logging.getLogger(__name__)

from services import performance_service

performance_ns = Namespace('performance', description='Performance metrics operations')

# Define a model for individual signal source performance within the summary
signal_source_performance_model = performance_ns.model('SignalSourcePerformance', {
    'total_signals': fields.Integer,
    'wins': fields.Integer,
    'losses': fields.Integer,
    'neutral': fields.Integer,
    'win_rate': fields.Float,
    'net_profit_percent': fields.Float
})

aggregated_performance_model = performance_ns.model('AggregatedPerformanceModel', {
    'report_date': fields.String(description='Jalali report date'),
    'period_type': fields.String(description='Period type (daily, weekly, monthly, annual)'),
    'signal_source': fields.String(description='Source of the signal (Golden Key, Weekly Watchlist, overall)'),
    'total_signals': fields.Integer(description='Total signals in the period'),
    'successful_signals': fields.Integer(description='Number of successful signals'),
    'win_rate': fields.Float(description='Win rate percentage'),
    'total_profit_percent': fields.Float(description='Total profit percentage from successful signals'),
    'total_loss_percent': fields.Float(description='Total loss percentage from unsuccessful signals'),
    'average_profit_per_win': fields.Float(description='Average profit per successful signal'),
    'average_loss_per_loss': fields.Float(description='Average loss per unsuccessful signal'),
    'net_profit_percent': fields.Float(description='Net profit/loss percentage'),
    'created_at': fields.String(description='Timestamp of record creation'),
    'updated_at': fields.String(description='Timestamp of last update')
})

overall_performance_summary_model = performance_ns.model('OverallPerformanceSummaryModel', {
    'overall_performance': fields.Nested(performance_ns.model('OverallSummaryData', {
        'total_signals_evaluated': fields.Integer(default=0),
        'overall_win_rate': fields.Float(default=0.0),
        'average_profit_per_win_overall': fields.Float(default=0.0),
        'average_loss_per_loss_overall': fields.Float(default=0.0),
        'overall_net_profit_percent': fields.Float(default=0.0)
    }), description='Overall summary of performance', required=True),
    # CORRECTED: Use fields.Nested with a dictionary for dynamic keys
    'signals_by_source': fields.Nested(performance_ns.model('SignalsBySourceData', {}, additional_properties=signal_source_performance_model), description='Performance breakdown by signal source', required=True),
    'last_updated': fields.String(description='Last update timestamp', required=True)
})

detailed_signal_performance_model = performance_ns.model('DetailedSignalPerformanceModel', {
    'signal_id': fields.String, # Added signal_id
    'symbol_id': fields.String,
    'symbol_name': fields.String,
    'signal_source': fields.String,
    'outlook': fields.String,
    'reason': fields.String,
    'entry_price': fields.Float,
    'jentry_date': fields.String,
    'entry_date': fields.String, # Added entry_date
    'exit_price': fields.Float,
    'jexit_date': fields.String,
    'exit_date': fields.String, # Added exit_date
    'profit_loss_percent': fields.Float, 
    'status': fields.String,
    'probability_percent': fields.Float, # Added probability_percent
    'created_at': fields.String,
    'updated_at': fields.String
})

# Parser for aggregated performance reports
aggregated_performance_parser = reqparse.RequestParser()
aggregated_performance_parser.add_argument('period_type', type=str, choices=('weekly', 'annual'), help='Type of period for aggregation (weekly or annual).')
aggregated_performance_parser.add_argument('signal_source', type=str, default='overall', help='Filter by signal source (e.g., weekly_watchlist, golden_key, overall).')


@performance_ns.route('/aggregated')
class AggregatedPerformanceResource(Resource):
    @performance_ns.doc(security='Bearer Auth')
    @jwt_required() 
    @performance_ns.marshal_with(overall_performance_summary_model) 
    @performance_ns.expect(aggregated_performance_parser) # Add parser for query params
    def get(self):
        logger.info("API call: Retrieving Aggregated Performance.")
        args = aggregated_performance_parser.parse_args()
        period_type = args['period_type'] 
        signal_source = args['signal_source'] 
        try:
            # If period_type is None, it means we want the overall summary
            if period_type: 
                # This endpoint is designed for overall summary, not filtered aggregated reports
                # If filtered reports are needed, a separate endpoint or more complex logic is required.
                # For now, if period_type is specified, we'll return an empty summary or an error.
                logger.warning("Attempted to retrieve filtered aggregated reports via /aggregated endpoint. Returning overall summary.")
                performance_data = performance_service.get_overall_performance_summary()
            else: 
                performance_data = performance_service.get_overall_performance_summary()
            return performance_data, 200
        except Exception as e:
            logger.error(f"Error retrieving aggregated performance: {e}", exc_info=True)
            return {"message": f"An error occurred while retrieving aggregated performance: {str(e)}"}, 500

@performance_ns.route('/signals-details') 
class SignalsDetailsResource(Resource):
    @performance_ns.doc(security='Bearer Auth')
    @jwt_required() 
    @performance_ns.marshal_list_with(detailed_signal_performance_model) 
    def get(self):
        logger.info("API call: Retrieving Detailed Signals Performance.")
        try:
            signals_details = performance_service.get_detailed_signals_performance()
            return signals_details, 200
        except Exception as e:
            logger.error(f"Error retrieving detailed signals performance: {e}", exc_info=True)
            return {"message": f"An error occurred while retrieving detailed signals performance: {str(e)}"}, 500

@performance_ns.route('/calculate-aggregated-performance')
class CalculateAggregatedPerformanceResource(Resource):
    @performance_ns.doc(security='Bearer Auth')
    @jwt_required()
    @performance_ns.expect(aggregated_performance_parser) # Reusing parser for period_type
    @performance_ns.response(200, 'Aggregated performance calculation initiated.')
    @performance_ns.response(500, 'Error during calculation.')
    def post(self):
        args = aggregated_performance_parser.parse_args()
        period_type = args['period_type'] 
        signal_source = args['signal_source'] 

        current_app.logger.info(f"API call: Initiating calculation of aggregated {period_type} performance for source: {signal_source}.")
        try:
            success, message = performance_service.calculate_and_save_aggregated_performance(period_type, signal_source)
            if success:
                return {"message": message}, 200
            else:
                return {"message": message}, 500
        except Exception as e:
            current_app.logger.error(f"Error during aggregated performance calculation: {e}", exc_info=True)
            return {"message": f"An error occurred: {str(e)}"}, 500