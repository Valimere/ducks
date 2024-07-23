from flask import Blueprint, jsonify, send_from_directory, current_app
from flask_restx import Api, Resource, reqparse
from werkzeug.datastructures import FileStorage
from store import DuckDBStore
from log import Logger
import os

main = Blueprint('main', __name__, static_folder='/app/app/static')
api = Api(main, version='0.1', title='DuckDB API', description='A simple DuckDB API', doc='/api/docs')

upload_parser = reqparse.RequestParser()
upload_parser.add_argument('file', location='files', type=FileStorage, required=True)

ns = api.namespace('api', description='Operations')


@ns.route('/upload')
class Upload(Resource):
    @api.expect(upload_parser)
    def post(self):
        store = DuckDBStore()
        logger = Logger().logger

        args = upload_parser.parse_args()
        file = args['file']

        if not file:
            logger.error('No file part')
            api.abort(400, 'No file part')

        if file.filename == '':
            logger.error('No selected file')
            api.abort(400, 'No selected file')

        try:
            os.makedirs('/app/data', exist_ok=True)
            file_path = os.path.join('/app/data', file.filename)
            file.save(file_path)
            logger.info(f'File saved to {file_path}')
            store.ingest_parquet(file_path)
            return {'message': 'File uploaded and ingested successfully'}, 200
        except Exception as e:
            logger.error(f'Error uploading and ingesting file: {e}')
            return {'message': 'Error uploading and ingesting file'}, 500


@ns.route('/cost/undiscounted/<string:service_code>')
class UndiscountedCost(Resource):
    def get(self, service_code):
        store = DuckDBStore()
        try:
            cost = store.query_undiscounted_cost(service_code)
            return jsonify({'undiscounted_cost': cost})
        except Exception as e:
            Logger().logger.error(f'Error querying undiscounted cost: {e}')
            return {'message': 'Error querying undiscounted cost'}, 500


@ns.route('/cost/discounted/<string:service_code>')
class DiscountedCost(Resource):
    def get(self, service_code):
        store = DuckDBStore()
        discounts = {
            'AmazonS3': 0.88,
            'AmazonEC2': 0.50,
            'AWSDataTransfer': 0.70,
            'AWSGlue': 0.95,
            'AmazonGuardDuty': 0.25
        }
        discount_rate = discounts.get(service_code, 1.0)
        try:
            cost = store.query_discounted_cost(service_code, discount_rate)
            return jsonify({'discounted_cost': cost})
        except Exception as e:
            Logger().logger.error(f'Error querying discounted cost: {e}')
            return {'message': 'Error querying discounted cost'}, 500


@ns.route('/cost/blended-discount-rate')
class BlendedDiscountRate(Resource):
    def get(self):
        store = DuckDBStore()
        try:
            rate = store.query_blended_discount_rate()
            return jsonify({'blended_discount_rate': rate})
        except Exception as e:
            Logger().logger.error(f'Error querying blended discount rate: {e}')
            return {'message': 'Error querying blended discount rate'}, 500


@ns.route('/cost/all')
class AllCosts(Resource):
    def get(self):
        store = DuckDBStore()
        try:
            costs = store.query_all_costs()
            return jsonify({'costs': costs})
        except Exception as e:
            Logger().logger.error(f'Error querying all costs: {e}')
            return {'message': 'Error querying all costs'}, 500


api.add_namespace(ns)
