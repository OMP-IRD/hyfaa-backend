from flask import Blueprint
from flask_restx import Api

from .stations import api as stations_api

blueprint = Blueprint('api_v1', __name__)
api_v1 = Api(
    app=blueprint,
    title='HYFAA backend API',
    version='1.0',
    description='Provides access to MGB/HYFAA-related data',
    contact='Jean Pommier',
    contact_email='jean.pommier@pi-geosolutions.fr',
)

api_v1.add_namespace(stations_api)
