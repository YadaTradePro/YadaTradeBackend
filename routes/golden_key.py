# routes/golden_key.py
from flask_restx import Namespace, Resource, fields, reqparse
from flask_jwt_extended import jwt_required
from flask import current_app, request
import logging

# تنظیمات لاگینگ برای این ماژول
logger = logging.getLogger(__name__)

from services import golden_key_service

golden_key_ns = Namespace('golden_key', description='Golden Key stock filtering operations')

# NEW: Model for Golden Key Technical Filter Definition (moved from analysis.py)
technical_filter_definition_model = golden_key_ns.model('TechnicalFilterDefinition', {
    'name': fields.String(required=True, description='Name of the technical filter'),
    'description': fields.String(description='Description of the filter'),
    'criteria': fields.String(description='Technical criteria for the filter'),
    'category': fields.String(description='Category of the filter (e.g., Trend, Volume)')
})

# UPDATED: GoldenKeyResult model to match database and frontend needs
golden_key_result_model = golden_key_ns.model('GoldenKeyResultModel', {
    'symbol_id': fields.String(description='Symbol ID'),
    #'symbol_name': fields.String(description='Symbol Name (Persian)'),
    'symbol': fields.String(description='Symbol Name (for compatibility)'), 
    #'name': fields.String(description='Symbol Name (for compatibility)'),   
    'total_score': fields.Integer(description='Total score based on matched filters'),
    'matched_filters': fields.Integer(description='Number of matched filters'), # This is the count
    'reason': fields.String(description='Human-readable reason for the signal'),
    'weekly_growth': fields.Float(description='Weekly growth percentage'),
    'entry_price': fields.Float(description='Recommended entry price'),
    'jentry_date': fields.String(description='Jalali date of recommendation'),
    'exit_price': fields.Float(description='Exit price (if signal closed)'),
    'jexit_date': fields.String(description='Jalali exit date (if signal closed)'),
    'profit_loss_percentage': fields.Float(description='Profit/Loss percentage'),
    'is_golden_key': fields.Boolean(description='True if it is a Golden Key signal'),
    'status': fields.String(description='Status of the signal (active, closed_profit, closed_loss, closed_neutral)'),
    'probability_percent': fields.Float(description='Estimated probability of success'),
    'timestamp': fields.String(description='Timestamp of the result generation'),
    'satisfied_filters_list': fields.List(fields.String, description='List of satisfied filter names')
})

# NEW: Model for the comprehensive Golden Key response
golden_key_response_model = golden_key_ns.model('GoldenKeyResponse', {
    'top_stocks': fields.List(fields.Nested(golden_key_result_model), description='List of top Golden Key stocks'),
    'technical_filters': fields.List(fields.Nested(technical_filter_definition_model), description='List of all available technical filters'),
    'last_updated': fields.String(description='Timestamp of last update')
})

# NEW: Parser for Golden Key filters (moved from analysis.py)
golden_key_filters_parser = reqparse.RequestParser()
golden_key_filters_parser.add_argument('filters', type=str, help='Comma-separated list of filter names to apply (e.g., "RSI بالای 70,MACD کراس صعودی").', location='args')


# NEW: Model for GoldenKeyRequest (specifically for POST body)
# This was implicitly expected by golden_key_request_model in the POST method
golden_key_request_model = golden_key_ns.model('GoldenKeyRequestModelForPost', {
    'filters': fields.String(required=False, description='Comma-separated list of filter names to apply (e.g., "RSI بالای 70,حجم بالا")')
})


# --- API Resource for Golden Key ---
@golden_key_ns.route('/run_filters') 
class RunGoldenKeyFiltersResource(Resource):
    @golden_key_ns.doc(security='Bearer Auth')
    @jwt_required()
    @golden_key_ns.response(200, 'Golden Key filter process initiated for saving.')
    @golden_key_ns.response(500, 'Error during Golden Key process.')
    def post(self):
        """
        Triggers the Golden Key filter process (intended for Cron Job or manual trigger)
        to calculate and save results to the database.
        """
        current_app.logger.info("API call: Initiating Golden Key filter process for saving.")
        try:
            success, message = golden_key_service.run_golden_key_analysis_and_save()
            if success:
                return {"message": message}, 200
            else:
                return {"message": message}, 500
        except Exception as e:
            current_app.logger.error(f"Error during Golden Key filter process: {e}", exc_info=True)
            return {"message": f"An error occurred: {str(e)}"}, 500

@golden_key_ns.route('/results') # This route handles both GET and POST for results
class GoldenKeyResultsResource(Resource):
    @golden_key_ns.doc(security='Bearer Auth')
    @jwt_required()
    @golden_key_ns.expect(golden_key_filters_parser) # Use the new parser for GET filters
    @golden_key_ns.marshal_with(golden_key_response_model)
    @golden_key_ns.response(200, 'Golden Key results retrieved successfully.')
    @golden_key_ns.response(500, 'Error retrieving Golden Key results.')
    def get(self):
        """
        Retrieves the latest Golden Key results, including top stocks and filter definitions.
        Optionally filters top stocks by selected technical filters.
        """
        current_app.logger.info("API call: Retrieving Golden Key Results (GET).")
        args = golden_key_filters_parser.parse_args()
        filters = args['filters'] # This will be a comma-separated string or None

        try:
            results = golden_key_service.get_golden_key_results(filters=filters)
            return results, 200
        except Exception as e:
            current_app.logger.error(f"Error retrieving Golden Key Results: {e}", exc_info=True)
            return {"message": f"An error occurred: {str(e)}"}, 500

    @golden_key_ns.doc(security='Bearer Auth')
    @jwt_required() 
    @golden_key_ns.expect(golden_key_request_model, validate=False) # Use the specific request model for POST body
    @golden_key_ns.marshal_with(golden_key_response_model) 
    def post(self): 
        current_app.logger.info("Received POST request to get Golden Key results (with filters).")
        filters_param = None
        if request.is_json:
            data = request.json
            filters_param = data.get('filters')

        try:
            results = golden_key_service.get_golden_key_results(filters=filters_param) 
            return results, 200
        except Exception as e:
            current_app.logger.error(f"Error retrieving Golden Key results: {e}", exc_info=True)
            return {"message": f"An error occurred while retrieving Golden Key results: {str(e)}"}, 500


@golden_key_ns.route('/calculate_win_rate') 
class CalculateGoldenKeyWinRateResource(Resource):
    @golden_key_ns.doc(security='Bearer Auth')
    @jwt_required() 
    def post(self):
        logger.info("Received manual request to calculate Golden Key Win-Rate.")
        try:
            success, message = golden_key_service.calculate_golden_key_win_rate()
            if success:
                return {"message": message}, 200
            else:
                return {"message": message}, 500
        except Exception as e:
            logger.error(f"Error calculating Golden Key Win-Rate: {e}", exc_info=True)
            return {"message": f"An error occurred during Win-Rate calculation: {str(e)}"}, 500

