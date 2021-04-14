# encoding: utf-8
"""
Functions related to minibasins
"""
from .database import engine
from sqlalchemy import text

_accepted_datatypes = ['all', 'assimilated', 'mgbstandard', 'forecast']
defaults = {
    'duration': '1 year'
}


def get_data(id, datatype, opts):
    """
    Retrieve data for the given minibasin.
    Params:
      * id: minibasin identifier. Also known as cell_id in the DB
      * datatype: should be one of _accepted_datatypes values
      * opts: filtering options
        * duration: Time lapse to retrieve. Should be consistent with the textual representation of a PostgreSQL date/time interval (https://www.postgresql.org/docs/9.1/datatype-datetime.html). Default is '1 year'
    """
    # read options
    duration = opts.get('duration') or defaults['duration']

    values = dict()
    if datatype not in _accepted_datatypes:
        return {
                   'minibasin_id': id,
                   'data': dict(),
                   'error': 'datatype not recognized. Should be one of `{}`'.format(', '.join(_accepted_datatypes))
               }

    if datatype in ['all', 'assimilated']:
        values['assimilated'] = _get_assimilated_data(id, duration)
    if datatype in ['all', 'mgbstandard']:
        values['mgbstandard'] = _get_mgbstandard_data(id, duration)
    if datatype in ['all', 'forecast']:
        values['forecast'] = _get_forecast_data(id, duration)
    return {'id': id, 'data': values}


def _get_mgbstandard_data(minibasin_id, duration='1 year'):
    json_output = {'error': 'no result'}
    with engine.connect() as conn:
        query = text("SELECT hyfaa.get_mgbstandard_values_for_minibasin(:id, :duration)")
        rs = conn.execute(query, id=minibasin_id, duration=duration)
        mini_record = rs.fetchone()
        if mini_record:
            json_output = mini_record[0]
    return json_output


def _get_forecast_data(minibasin_id, duration='1 year'):
    """
    """
    json_output = {'error': 'no result'}
    with engine.connect() as conn:
        query = text("SELECT hyfaa.get_forecast_values_for_minibasin(:id, :duration)")
        rs = conn.execute(query, id=minibasin_id, duration=duration)
        mini_record = rs.fetchone()
        if mini_record:
            json_output = mini_record[0]
    return json_output


def _get_assimilated_data(minibasin_id, duration='1 year'):
    """
    """
    json_output = {'error': 'no result'}
    with engine.connect() as conn:
        query = text("SELECT hyfaa.get_assimilated_values_for_minibasin(:id, :duration)")
        rs = conn.execute(query, id=minibasin_id, duration=duration)
        mini_record = rs.fetchone()
        if mini_record:
            json_output = mini_record[0]
    return json_output
