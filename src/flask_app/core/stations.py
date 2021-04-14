# encoding: utf-8
"""
Functions related to stations
"""
from sqlalchemy import text

from .database import engine
from .minibasin import get_data as get_minibasin_data

def get_stations():
    """
    Retrieve stations records from geospatial.stations table
    Returns a list of dicts
    """
    stations=[]
    with engine.connect() as conn:
        query = text("SELECT * FROM geospatial.stations")
        rs = conn.execute(query)
        records = rs.fetchall()
        for row in records:
            st = {
                'id': row['id'],
                'minibasin': row['minibasin'],
                'city': row['city']
            }
            stations.append(st)
    return stations


def get_stations_as_geojson():
    """
    Retrieve stations records from geospatial.stations_geo view
    Returns geojson feature collection
    """
    stations=[]
    with engine.connect() as conn:
        sql_query = """SELECT jsonb_build_object(
              'type',     'FeatureCollection',
              'features', jsonb_agg(feature)
            )
            FROM (
              SELECT jsonb_build_object(
                'type',       'Feature',
                'id',         id,
                'geometry',   ST_AsGeoJSON(wkb_geometry)::jsonb,
                'properties', to_jsonb(inputs) - 'id' - 'wkb_geometry'
              ) AS feature
              FROM (
                SELECT * FROM geospatial.stations_geo
              ) inputs
            ) features;
            """
        query = text(sql_query)
        rs = conn.execute(query)
        record = rs.fetchone()
        return record[0]
    return None


def get_station(id):
    """
    Retrieve a station record from geospatial.stations table
    Params:
        * id: id of the station (*not the minibasin id*)
    Returns a dict
    """
    with engine.connect() as conn:
        query = text("SELECT * FROM geospatial.stations WHERE id = :id")
        rs = conn.execute(query, id=id)
        record = rs.fetchone()
        if record:
            st = {
                'id': record['id'],
                'minibasin': record['minibasin'],
                'city': record['city']
            }
            return st
    return None


def get_data(id, datatype, opts):
    """
    Retrieve data for the given station id: retrieve the minibasin ID for this station, then calls minibasin.get_data
    Params:
      * id: station identifier. (note: this is *not* the minibasin id)
      * datatype: should be one of _accepted_datatypes values
      * opts: filtering options
        * duration: Time lapse to retrieve. Should be consistent with the textual representation of a PostgreSQL date/time interval (https://www.postgresql.org/docs/9.1/datatype-datetime.html). Default is '1 year'
    """
    with engine.connect() as conn:
        query = text("SELECT * FROM geospatial.stations WHERE id = :id")
        rs = conn.execute(query, id=id)
        record = rs.fetchone()
        if record:
            st = {
                'id': record['id'],
                'minibasin': record['minibasin'],
                'city': record['city']
            }
            minibasin_data = get_minibasin_data(st['minibasin'], datatype, opts)
            st['data'] = minibasin_data['data']
            return st
    return None

