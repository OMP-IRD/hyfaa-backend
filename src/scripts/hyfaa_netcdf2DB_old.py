import argparse
from datetime import datetime,timedelta

from netCDF4 import Dataset
import numpy as np
import pandas as pd
import psycopg2
import psycopg2.extras as extras
from ..core.database import engine

nc = None
db = {
    'connect_url': "dbname='mgb_hyfaa' user='hyfaa_publisher' host='localhost' password='publisher_pass'",
    'tablename': 'data_with_assim',
    'schema' : 'hyfaa'
}

# Convert NaN to null values when pushing to PostgreSQL DB
def nan_to_null(f, _NULL=psycopg2.extensions.AsIs('NULL'), _Float=psycopg2.extensions.Float):
    if f != f:
        return _NULL
    else:
        return _Float(f)
psycopg2.extensions.register_adapter(float, nan_to_null)


# lambda function, CNES Julian days to Gregorian date and vice-versa
julianday_to_datetime = lambda t: datetime(1950, 1, 1) + timedelta(int(t))
datetime_to_julianday = lambda t: (t - datetime(1950, 1, 1)).total_seconds() / (24. * 3600.)
vfunc_jd_to_dt = np.vectorize(julianday_to_datetime)


def _retrieve_times_to_update():
    """
    Extract the list of times where the data have been updated.
    Returns: a 3-tuple list: (time index, time value, time at which the data --for this time-frame-- was last updated)
    """
    update_times = None
    conn = None
    last_published_day = 0
    last_updated_without_errors_jd = 0
    try:
        conn = psycopg2.connect(db['connect_url'])
        # retrieve state information from the DB, about the table we are about to update
        cursor = conn.cursor()
        cursor.execute("SELECT last_updated_jd, last_updated_without_errors_jd FROM {}.state WHERE tablename=%s".format(db['schema']),
                       (db['tablename'],))
        state_records = cursor.fetchall()
        if len(state_records) == 1:
            last_published_day = state_records[0][0]
            last_updated_without_errors_jd = state_records[0][1]
        else:
            raise Exception("Could not retrieve state for table " + db['tablename'])
        # Build a list of all indices-time value (Julian Day)
        times_array = list(zip(
            np.arange(start=0, stop=nc.dimensions['n_time'].size, dtype='i4'),
            nc.variables['time'][:],
            nc.variables['time_added_to_hydb'][:]
        ))
        # Filter to only the times posterior to last update (fetching the time the data was added
        # -> handles updates on old data if needs be
        update_times = list(filter(lambda t: t[2] > last_updated_without_errors_jd, times_array))
    except (Exception, psycopg2.Error) as error:
        print("Error fetching state from PostgreSQL table:\n", error)
    finally:
        # close database connection
        if conn:
            cursor.close()
            conn.close()
        return update_times, last_updated_without_errors_jd


def _extract_data_to_dataframe_at_time(t):
    """
    Retrieves data from netcdf variables, for the given time value. Organize it into a Pandas dataframe with proper
    layout, to optimize publication into PostgreSQL DB
    """
    print("Publishing data for day {} (index {})".format(t[1], t[0]))
    itime = t[0]
    nb_cells = nc.dimensions['n_cells'].size
    npst = np.ma.column_stack((
        np.arange(start=1, stop=nb_cells + 1, dtype='i4'),
        vfunc_jd_to_dt(np.full((nb_cells), nc.variables['time'][itime])),
        nc.variables['water_elevation_catchment_mean'][itime, :],
        nc.variables['water_elevation_catchment_median'][itime, :],
        nc.variables['water_elevation_catchment_std'][itime, :],
        nc.variables['water_elevation_catchment_mad'][itime, :],
        nc.variables['streamflow_catchment_mean'][itime, :],
        nc.variables['streamflow_catchment_median'][itime, :],
        nc.variables['streamflow_catchment_std'][itime, :],
        nc.variables['streamflow_catchment_mad'][itime, :],
        vfunc_jd_to_dt(np.full((nb_cells), nc.variables['time_added_to_hydb'][itime])),
        np.full((nb_cells), nc.variables['is_analysis'][itime])
    ))

    df = pd.DataFrame(npst,
                      index=np.arange(start=1, stop=nb_cells + 1, dtype='i4'),
                      columns=['cell_id', 'date', 'elevation_mean', 'elevation_median', 'elevation_stddev', 'elevation_mad',
                               'flow_mean', 'flow_median', 'flow_stddev', 'flow_mad', 'update_time', 'is_analysis']
                      )

    # force cell_id type to smallint
    df = df.astype({
        'cell_id': 'int16',
        'is_analysis': 'boolean'
    })
    print(df)
    return df


def _publish_dataframe_to_db(df):
    """
    Publish the provided Pandas DataFrame into the DB. Using psycopg2.extras.execute_values() to insert the dataframe
    Returns: - nb of errors if there were (0 if everything went well)
    """
    conn = None
    try:
        conn = psycopg2.connect(db['connect_url'])
        # Create a list of tupples from the dataframe values
        tuples = [tuple(x) for x in df.to_numpy()]
        # Comma-separated dataframe columns
        cols = ','.join(list(df.columns))
        # SQL query to execute
        # Simple INSERT statement. We'll rather use an upsert, see below
        # query  = "INSERT INTO %s(%s) VALUES %%s" % (table, cols)
        # print(query)
        # Run an upsert command (on conflict etc)
        # Considers that the pkey is composed of the 2 first fields:
        updatable_cols = list(df.columns)[2:]
        # Write the update statement (internal part). EXCLUDED is a PG internal table contained rejected rows from the insert
        # see https://www.postgresql.org/docs/10/sql-insert.html#SQL-ON-CONFLICT
        externals = lambda n: "{n}=EXCLUDED.{n}".format(n=n)
        update_stmt = ','.join(["%s" % (externals(name)) for name in updatable_cols])
        query = "INSERT INTO {schema}.{table}({cols}) VALUES %s ON CONFLICT ON CONSTRAINT  {table}_pk DO UPDATE SET {updt_stmt};".format(
            schema=db['schema'], table=db['tablename'], cols=cols, updt_stmt=update_stmt)
        # print(query)
        cursor = conn.cursor()
        try:
            extras.execute_values(cursor, query, tuples)
            conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error: %s" % error)
            conn.rollback()
            cursor.close()
            return 1
        print("execute_values() done")
    except (Exception, psycopg2.Error) as error:
        print("Error publishing data to PostgreSQL table", error)
        return 1
    finally:
        # closing database connection
        if cursor:
            cursor.close()
        if conn:
            conn.close()
            # no error
        return 0


def _update_state( errors, last_published_day_jd, last_updated_without_errors_jd):
    """
    Update the state entry in the DB
    Returns:
    """
    try:
        conn = psycopg2.connect(db['connect_url'])
        # retrieve state information from the DB, about the table we are about to update
        cursor = conn.cursor()
        state_updt_query = """
                    UPDATE {}.state
                    SET last_updated = %s,
                        last_updated_jd = %s,
                        update_errors = %s,
                        last_updated_without_errors = %s,
                        last_updated_without_errors_jd = %s
                    WHERE tablename = %s
                """.format(db['schema'])
        cursor.execute(state_updt_query, (
            julianday_to_datetime(last_published_day_jd), last_published_day_jd, errors,
            julianday_to_datetime(last_updated_without_errors_jd), last_updated_without_errors_jd, db['tablename']))
    except (Exception, psycopg2.Error) as error:
        print("Error fetching data from PostgreSQL table", error)
    finally:
        # closing database connection
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def publish(filepath, db_connect_url, tablename, schema='hyfaa', only_last_n_days=None):
    """
    Parses a netCDF4 file produced with data from HYFAA-MGB algorithm.
    Publishes it to a pigeosolutions/hyfaa-postgis database.

    First run publishes the whole serie of data (might take a long time). Unless only_last_n_days is set to a number of days (defaults to None), on which case only n last days will be published
    Subsequent runs only perform an update (UPSERT) on data modified or added since the previous run.
    """
    global nc
    nc = Dataset(filepath, "r", format="netCDF4")
    global db
    db = {
        'connect_url': db_connect_url,
        'tablename': tablename,
        'schema': schema
    }
    update_times, last_updated_without_errors_jd = _retrieve_times_to_update()
    # Iterate and publish all recent times
    if only_last_n_days:
        update_times = update_times[-only_last_n_days:]
    errors = 0
    for t in update_times:
        df = _extract_data_to_dataframe_at_time(t)
        e = _publish_dataframe_to_db(df)
        # count errors if there are
        errors += e
    last_published_day_jd = max(list(zip(*update_times))[1])
    if not errors:
        # increment last update time without error
        last_updated_without_errors_jd = max(list(zip(*update_times))[2])
    _update_state(errors, last_published_day_jd, last_updated_without_errors_jd)


def main():
    # Input arguments
    parser = argparse.ArgumentParser(description='''
    Parses a netCDF4 file produced with data from HYFAA-MGB algorithm. 
    Publishes it to a pigeosolutions/hyfaa-postgis database. 
    
    First run publishes the whole serie of data (might take a long time). 
    Subsequent runs only perform an update (UPSERT) on data modified or added since the previous run.
    ''')
    parser.add_argument('filepath', help='NetCDF4 file path')
    parser.add_argument('db_connect_url',
                        help='The connection URL for the DB. (Default: "dbname=\'mgb_hyfaa\' user=\'postgres\' host=\'localhost\' password=\'pass\'")')
    parser.add_argument('-s', '--schema',
                        default='hyfaa',
                        help='DB schema (Default: "hyfaa")')
    parser.add_argument('table_name',
                        help='Table name to publish into')
    parser.add_argument('--only_last_n_days',
                        type = int,
                        default=None,
                        help='if set, only the only_last_n_days days will be published (useful for publishing only a sample of data. Default: None)')
    args = parser.parse_args()

    FILE = args.filepath
    DB_URL = args.db_connect_url
    TABLENAME = args.table_name
    SCHEMA = args.schema
    only_last_n_days = args.only_last_n_days

    publish(FILE, DB_URL, TABLENAME, SCHEMA, only_last_n_days)

    # TODO
    # - add tests
    # - use logger + verbose logging (debug) instead of print
    # - report errors / support prometheus ?
    # - integrate into hyfaa scheduler (makes more sense to run it just after the scheduler's computed the data)
    # - Document
    # - add views in DB (last 10d) --  materialized ?
    # - parametrize mat. view to get n last days
    # - add pg_tileserv
    # - set up a dev environment for Florent, with DB initialized with sample data + pg_tileserv
    # - Flask API

if __name__ == '__main__':
    main()