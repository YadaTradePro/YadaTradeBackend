# routes/potential_queues.py
from flask_restx import Namespace, Resource, fields, reqparse 
from flask_jwt_extended import jwt_required
from flask import current_app, request 
import logging 

# تنظیمات لاگینگ برای این ماژول
logger = logging.getLogger(__name__)

from services import potential_buy_queues_service # Import the service

potential_queues_ns = Namespace('potential_queues', description='Potential Buy Queues operations')

# ----------------- Models -----------------
potential_queues_result_model = potential_queues_ns.model('PotentialQueueResultModel', {
    'symbol_name': fields.String(description='Symbol Name (Persian)'),
    'symbol_id': fields.String(description='Symbol ID'),
    'reason': fields.String(description='Reason for being in queue'),
    'jdate': fields.String(description='Jalali date of the queue detection'),
    'current_price': fields.Float(description='Current price of the symbol'),
    'real_buyer_power_ratio': fields.Float(description='Real buyer power ratio'), # Changed from volume_change_percent to match service
    'probability_percent': fields.Float(description='Calculated probability in percent'), # Added missing field
    'matched_filters': fields.List(fields.String, description='List of matched filters'), 
    'timestamp': fields.String(description='Timestamp of the result generation'),
    'group_type': fields.String(description='Type of queue (general or fund)') 
})

# مدل برای فیلترها (استفاده از مدل سرویس برای تعریف)
potential_queue_filter_model = potential_queues_ns.model('PotentialQueueFilter', {
    'name': fields.String, 'category': fields.String
})

potential_queues_response_model = potential_queues_ns.model('PotentialQueuesResponse', {
    'top_queues': fields.List(fields.Nested(potential_queues_result_model), description='List of top potential buy queues'),
    'technical_filters': fields.List(fields.Nested(potential_queue_filter_model), description='List of all available filters for potential buy queues'),
    'last_updated': fields.String(description='Jalali date of last successful analysis')
})

# ----------------- Resources -----------------
@potential_queues_ns.route('/results')
class GetPotentialQueuesResource(Resource):
    @potential_queues_ns.doc(security='Bearer Auth')
    @jwt_required() 
    @potential_queues_ns.marshal_with(potential_queues_response_model)
    def get(self):
        logger.info("API call: Retrieving Potential Buy Queues (GET).")
        parser = reqparse.RequestParser()
        parser.add_argument('filters', type=str, help='Comma-separated list of filter names', location='args')
        args = parser.parse_args()
        
        # در این سناریو، فقط به عنوان Dict خالی پاس داده می‌شود یا متناسب با پارامترهای دیگر
        filters_param = {'filters_str': args['filters']} if args['filters'] else {}

        try:
            # ۱. بازیابی لیست نتایج
            results = potential_buy_queues_service.get_potential_buy_queues_data(filters=filters_param)
            
            # ۲. بازیابی لیست فیلترها
            filters_list = potential_buy_queues_service.get_defined_filters()
            
            # ۳. ساخت دیکشنری پاسخ نهایی مطابق با potential_queues_response_model
            response_data = {
                "top_queues": results,
                "technical_filters": filters_list,
                "last_updated": potential_buy_queues_service.get_today_jdate_str()
            }
            
            return response_data, 200
        except Exception as e:
            logger.error(f"Error retrieving potential buy queues: {e}", exc_info=True)
            # استفاده از abort برای خطایابی بهتر توسط flask-restx
            potential_queues_ns.abort(500, f"An error occurred while retrieving potential buy queues: {str(e)}")

    @potential_queues_ns.doc(security='Bearer Auth')
    @jwt_required() 
    @potential_queues_ns.expect(potential_queues_ns.model('PotentialQueuesRequest', { 
        'filters': fields.String(description='Comma-separated list of filter names')
    }), validate=False)
    @potential_queues_ns.marshal_with(potential_queues_response_model)
    def post(self):
        logger.info("Received POST request to get Potential Buy Queues (with filters).")
        filters_param = {}
        if request.is_json:
            data = request.json
            filters_param = {'filters_str': data.get('filters')} if data.get('filters') else {}

        try:
            # ۱. بازیابی لیست نتایج
            results = potential_buy_queues_service.get_potential_buy_queues_data(filters=filters_param)

            # ۲. بازیابی لیست فیلترها
            filters_list = potential_buy_queues_service.get_defined_filters()

            # ۳. ساخت دیکشنری پاسخ نهایی
            response_data = {
                "top_queues": results,
                "technical_filters": filters_list,
                "last_updated": potential_buy_queues_service.get_today_jdate_str()
            }

            return response_data, 200
        except Exception as e:
            logger.error(f"Error retrieving potential buy queues: {e}", exc_info=True)
            potential_queues_ns.abort(500, f"An error occurred while retrieving potential buy queues: {str(e)}")
            
@potential_queues_ns.route('/run-analysis')
class RunPotentialQueuesAnalysisResource(Resource):
    @potential_queues_ns.doc(security='Bearer Auth')
    @jwt_required() 
    def post(self):
        logger.info("API call: Running Potential Buy Queues Analysis.")
        try:
            success, message = potential_buy_queues_service.run_potential_buy_queue_analysis_and_save() 
            if success:
                return {"message": message}, 200
            else:
                return {"message": message}, 500
        except Exception as e:
            logger.error(f"Error running Potential Buy Queues analysis: {e}", exc_info=True)
            return {"message": f"An error occurred: {str(e)}"}, 500