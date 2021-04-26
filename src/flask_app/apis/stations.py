from flask_restx import Namespace, Resource, fields, reqparse, abort
from flask_restx.api import url_for
from flask import jsonify, make_response

from ..core import minibasin, stations

api = Namespace('stations', description='Stations related operations. Stations are virtual POI connected to minibasin data')

str_duration_help = 'Time lapse to retrieve. Should correspond to postgresql\'s time interval (https://www.postgresql.org/docs/9.1/datatype-datetime.html), e.g. \'1 year 30 days\''


@api.route('')
class Stations(Resource):
    def get(self):
        '''Retrieve stations list'''
        st_rec = stations.get_stations()
        return jsonify(st_rec)


@api.produces(["application/geojson"])
@api.route('/as_geojson')
class StationsAsGeojson(Resource):
    def get(self):
        '''Retrieve stations as geojson feature collection'''
        st_rec = stations.get_stations_as_geojson()
        response = make_response(st_rec)
        response.headers.set("Content-Type", "application/geojson")
        return response



@api.route('/<int:id>', endpoint='station_by_id')
@api.param('id', 'The station identifier')
class Station(Resource):
    @api.doc(responses={
        200: 'Success',
        400: 'Validation Error',
        404: 'Station not found'
    })
    def get(self, id):
        '''Describe a station'''
        st_rec = stations.get_station(id)
        if not st_rec:
            api.abort(404)
        st_rec['dataseries'] = {
                'mgbstandard':
                    {
                        'description': 'MGB simple flow modeling',
                        'url': '{}/data/mgbstandard'.format(url_for('api_v1.station_by_id', id=id)),
                    },
                'assimilated':
                    {
                        'description': 'MGB/HYFAA Data computed with assimilation',
                        'url': '{}/data/assimilated'.format(url_for('api_v1.station_by_id', id=id)),
                    },
                'forecast':
                    {
                        'description': 'MGB/HYFAA forecasts (10 days)',
                        'url': '{}/data/forecast'.format(url_for('api_v1.station_by_id', id=id)),
                    },
                'all':
                    {
                        'description': 'Combines the above datasets',
                        'url': '{}/data/all'.format(url_for('api_v1.station_by_id', id=id)),
                    }
            }
        return jsonify(st_rec)


@api.route('/<int:id>/data/<dataserie>')
@api.param('id', 'The station identifier')
@api.param('dataserie', 'The data serie to retrieve', enum=['all', 'assimilated', 'mgbstandard', 'forecast'])
class StationData(Resource):
    @api.param('duration', str_duration_help )
    @api.doc(responses={
        200: 'Success',
        400: 'Validation Error',
        404: 'Station not found'
    })
    def get(self, id, dataserie):
        '''
        Retrieve MGB/HYFAA data for a station, given its identifier and a dataserie name (or `all` to get all available dataseries.
        The returned object provides, for the demanded dataserie:
        * "date": the date for which the value has been computed
        * "flow": (m³/s) values representing the median for assimilated and forecast dataseries, the mean for mgbstandard serie
        * "flow_mad": [assimilated and forecast dataseries only] median absolute deviation
        * "expected": [assimilated and forecast dataseries only] (m³/s) the expected value, based on the mean values on this same day over the years
        '''
        parser = reqparse.RequestParser()
        parser.add_argument('duration', type=pg_time_interval, location='args', help=str_duration_help)
        args = parser.parse_args()
        data = stations.get_data(id, dataserie, args)
        if data:
            return data
        api.abort(404)



def pg_time_interval(value):
    '''Parse my type'''
    import re
    if not re.compile('(([0-9]+) (year|month|week|day)(s)?( )?)+').fullmatch(value):
        abort(400)

    return value

# Swagger documentation
pg_time_interval.__schema__ = {'type': 'string', 'format': 'pg-time-interval'}



class InvalidUsage(Exception):
    status_code = 400

    def __init__(self, message, status_code=None, payload=None):
        Exception.__init__(self)
        self.message = message
        if status_code is not None:
            self.status_code = status_code
        self.payload = message

    def to_dict(self):
        rv = dict(self.payload or ())
        rv['message'] = self.message
        return rv

@api.errorhandler(InvalidUsage)
def handle_invalid_usage(error):
    response = jsonify(error.to_dict())
    response.status_code = error.status_code
    return response