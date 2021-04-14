import argparse
from datetime import datetime,timedelta
from os import environ, path
from netCDF4 import Dataset
import numpy as np
import pandas as pd
import psycopg2
import psycopg2.extras as extras
import time
import logging
logging.basicConfig(level=logging.INFO)

DATABASE_URI='postgresql://hyfaa_publisher:hyfaa_publisher@localhost:5432/mgb_hyfaa'
DATABASE_SCHEMA='hyfaa'
# Global psycopg2 connection
conn=None

sources = [
    {
        'name': 'mgbstandard',
        'file': 'mgbstandard_solution_databases/post_processing_portal.nc',
        'nc_data_vars': [
            'water_elevation_catchment_mean',
            'streamflow_catchment_mean',
         ],
        'tablename': 'data_mgbstandard'
    },
    {
        'name': 'forecast',
        'file': 'assimilated_solution_databases/prevision_using_previous_years/post_processing_portal.nc',
        'nc_data_vars': [
            'water_elevation_catchment_mean',
            'water_elevation_catchment_median',
            'water_elevation_catchment_std',
            'water_elevation_catchment_mad',
            'streamflow_catchment_mean',
            'streamflow_catchment_median',
            'streamflow_catchment_std',
            'streamflow_catchment_mad',
         ],
        'tablename': 'data_forecast'
    },
    {
        'name': 'assimilated',
        'file': 'assimilated_solution_databases/post_processing_portal.nc',
        'nc_data_vars': [
            'water_elevation_catchment_mean',
            'water_elevation_catchment_median',
            'water_elevation_catchment_std',
            'water_elevation_catchment_mad',
            'streamflow_catchment_mean',
            'streamflow_catchment_median',
            'streamflow_catchment_std',
            'streamflow_catchment_mad',
         ],
        'tablename': 'data_assimilated'
    },
]

short_names = {
    'water_elevation_catchment_mean': 'elevation_mean',
    'water_elevation_catchment_median': 'elevation_median',
    'water_elevation_catchment_std': 'elevation_stddev',
    'water_elevation_catchment_mad': 'elevation_mad',
    'streamflow_catchment_mean': 'flow_mean',
    'streamflow_catchment_median': 'flow_median',
    'streamflow_catchment_std': 'flow_stddev',
    'streamflow_catchment_mad': 'flow_mad',
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


def _retrieve_times_to_update(nc, tablename):
    """
    Extract the list of times where the data have been updated.
    Returns: a 3-tuple list: (time index, time value, time at which the data --for this time-frame-- was last updated)
    """
    update_times = None
    last_published_day = 0
    last_updated_without_errors_jd = 0
    try:
        # retrieve state information from the DB, about the table we are about to update
        cursor = conn.cursor()
        cursor.execute("SELECT last_updated_jd, last_updated_without_errors_jd FROM {}.state WHERE tablename=%s".format(DATABASE_SCHEMA),
                       (tablename,))
        state_records = cursor.fetchone()
        if state_records:
            last_published_day = state_records[0]
            last_updated_without_errors_jd = state_records[1]
        else:
            last_published_day = 0
            last_updated_without_errors_jd = 0
            #raise Exception("Could not retrieve state for table " + tablename)
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
        logging.error("Error fetching state from PostgreSQL table:\n", error)
    finally:
        # close database connection
        if cursor:
            cursor.close()
        return update_times, last_updated_without_errors_jd


def _extract_data_to_dataframe_at_time(nc, ds, t):
    """
    Retrieves data from netcdf variables, for the given time value. Organizes it into a Pandas dataframe with proper
    layout, to optimize publication into PostgreSQL DB
    Params:
      * nc: netcdf4.Dataset input file
      * ds: dataserie definition (one element of global `sources`list)
      * t: time to look for
    """
    logging.debug("Preparing data for day {} (index {})".format(t[1], t[0]))
    itime = t[0]
    nb_cells = nc.dimensions['n_cells'].size
    # We will group the netcdf variables as columns of a 2D matrix (the pandas dataframe)
    # Common columns
    columns_dict = {
            'cell_id': np.arange(start=1, stop=nb_cells + 1, dtype='i2'),
            'date': vfunc_jd_to_dt(np.full((nb_cells), nc.variables['time'][itime])),
            'update_time': vfunc_jd_to_dt(np.full((nb_cells), nc.variables['time_added_to_hydb'][itime])),
            'is_analysis': np.full((nb_cells), nc.variables['is_analysis'][itime], dtype='?')
    }
    # dynamic columns: depend on the dataserie considered
    for j in ds['nc_data_vars']:
        columns_dict[short_names[j]] = nc.variables[j][itime, :]

    df = pd.DataFrame.from_dict(columns_dict)

    # force cell_id type to smallint
    df = df.astype({
        'cell_id': 'int16',
        'is_analysis': 'boolean'
    })
    logging.debug(df)
    return df


def _publish_dataframe_to_db(df, ds):
    """
    Publish the provided Pandas DataFrame into the DB. Using psycopg2.extras.execute_values() to insert the dataframe
    Returns: - nb of errors if there were (0 if everything went well)
    Params:
      * df: pandas dataframe to publish
      * ds: dataserie definition (one element of global `sources`list)
    """
    try:
        # Create a list of tupples from the dataframe values
        tuples = [tuple(x) for x in df.to_numpy()]
        # tuples = df.to_records(index=False).tolist() # seems faster but breaks the datetimes
        # Comma-separated dataframe columns
        cols = ','.join(list(df.columns))

        # Run an upsert command (on conflict etc)
        # Considers that the pkey is composed of the 2 first fields:
        updatable_cols = list(df.columns)[2:]

        # Write the update statement (internal part). EXCLUDED is a PG internal table contained rejected rows from the insert
        # see https://www.postgresql.org/docs/10/sql-insert.html#SQL-ON-CONFLICT
        externals = lambda n: "{n}=EXCLUDED.{n}".format(n=n)
        update_stmt = ','.join(["%s" % (externals(name)) for name in updatable_cols])
        query = "INSERT INTO {schema}.{table}({cols}) VALUES %s ON CONFLICT ON CONSTRAINT  {table}_pk DO UPDATE SET {updt_stmt};".format(
            schema=DATABASE_SCHEMA, table=ds['tablename'], cols=cols, updt_stmt=update_stmt)

        # Execute the uery
        cursor = conn.cursor()
        try:
            extras.execute_values(cursor, query, tuples)
            conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            logging.error("Error: %s" % error)
            conn.rollback()
            cursor.close()
            return 1
        logging.debug("execute_values() done")
    except (Exception, psycopg2.Error) as error:
        logging.error("Error publishing data to PostgreSQL table", error)
        return 1
    finally:
        if cursor:
            cursor.close()
            # no error
        return 0


def _update_state( ds, errors, last_published_day_jd, last_updated_without_errors_jd):
    """
    Update the state entry in the DB
    """
    try:
        # retrieve state information from the DB, about the table we are about to update
        cursor = conn.cursor()
        # state_updt_query = """
        #             UPDATE {}.state
        #             SET last_updated = %s,
        #                 last_updated_jd = %s,
        #                 update_errors = %s,
        #                 last_updated_without_errors = %s,
        #                 last_updated_without_errors_jd = %s
        #             WHERE tablename = %s
        #         """.format(DATABASE_SCHEMA)

        state_updt_query = """
                    INSERT INTO {schema}.state (tablename, last_updated, last_updated_jd, update_errors, last_updated_without_errors, last_updated_without_errors_jd)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT ON CONSTRAINT state_pk
                    DO UPDATE SET 
                        last_updated = EXCLUDED.last_updated,
                        last_updated_jd = EXCLUDED.last_updated_jd,
                        update_errors = EXCLUDED.update_errors,
                        last_updated_without_errors = EXCLUDED.last_updated_without_errors,
                        last_updated_without_errors_jd = EXCLUDED.last_updated_without_errors_jd
                    WHERE state."tablename" = '{table}'
                """.format(schema=DATABASE_SCHEMA, table=ds['tablename'])

        cursor.execute(state_updt_query, (
            ds['tablename'],
            julianday_to_datetime(last_published_day_jd),
            last_published_day_jd,
            errors,
            julianday_to_datetime(last_updated_without_errors_jd),
            last_updated_without_errors_jd
        ))

        conn.commit()
    except (Exception, psycopg2.Error) as error:
        logging.error("Error updating data on hyfaa.state table", error)
    finally:
        if cursor:
            cursor.close()


def publish_nc(ds, only_last_n_days):
    """
    Publish a netcdf4 dataset.
    First extracts the state information from the database, to publish/.update only the records that need it
    Then publishes them using an UPSERT command.
    Finally updates the `state` table
    Params:
      * ds: dataserie definition (one element of global `sources` list, see above)
      * only_last_n_days: int: allows to truncate the extraction to the last n days (useful when you are in a hurry)
    """
    nc = Dataset(ds['file'], "r", format="netCDF4")
    # Check when was the last data publication (only publish data that need to be)
    update_times, last_updated_without_errors_jd = _retrieve_times_to_update(nc, ds['tablename'])

    # truncate the extraction to the last n days (useful when you are in a hurry)
    if only_last_n_days:
        update_times = update_times[-only_last_n_days:]

    if not update_times:
        logging.info("DB is up to date")
        return

    # Iterate and publish all recent times
    errors = 0
    for t in update_times:
        tic = time.perf_counter()
        # netcdf to dataframe
        df = _extract_data_to_dataframe_at_time(nc, ds, t)
        # dataframe to DB
        e = _publish_dataframe_to_db(df, ds)
        if not e:
            logging.info("Published data for time {} (index {}, greg. time {})".format(t[1], t[0], julianday_to_datetime(t[1])))
        else:
            logging.warning("Encountered a DB error when publishing data for time {} ({}). Please watch your logs".format(t[0], t[1]))
        # count errors if there are
        errors += e

        tac = time.perf_counter()
        logging.info("processing time: {}".format(tac - tic))

    last_published_day_jd = max(list(zip(*update_times))[1])
    if not errors:
        # increment last update time without error
        last_updated_without_errors_jd = max(list(zip(*update_times))[2])
    # update state table
    _update_state(ds, errors, last_published_day_jd, last_updated_without_errors_jd)


def publish(rootpath, only_last_n_days=None):
    """
    Parses a netCDF4 file produced with data from HYFAA-MGB algorithm.
    Publishes it to a pigeosolutions/hyfaa-postgis database.

    First run publishes the whole serie of data (might take a long time). Unless only_last_n_days is set to a number of days (defaults to None), on which case only n last days will be published
    Subsequent runs only perform an update (UPSERT) on data modified or added since the previous run.
    """
    try:
        global conn
        conn = psycopg2.connect(DATABASE_URI)

        tic = time.perf_counter()
        for ds in sources:
            nc_path = path.join(rootpath,ds['file'])
            logging.info('Publishing {} data from {} to DB {} table'.format(ds['name'], nc_path, ds['tablename']))
            ds['file'] = nc_path
            publish_nc(ds, only_last_n_days)
        tac = time.perf_counter()
        logging.info("Total processing time: {}".format(tac-tic))

    except (Exception, psycopg2.Error) as error:
        logging.error("Error establishing connection to PostgreSQL table", error)
    finally:
        # closing database connection
        if conn:
            conn.close()


def main():
    # Input arguments
    parser = argparse.ArgumentParser(description='''
    Parses a bunch of netCDF4 files produced with data from HYFAA-MGB algorithm. 
    Publishes it to a pigeosolutions/hyfaa-postgis database. 
    
    First run publishes the whole serie of data (might take a long time). 
    Subsequent runs only perform an update (UPSERT) on data modified or added since the previous run.
    The connection parameters can be provided as argument or as an environment variable (DATABASE_URI)
    ''')
    parser.add_argument('rootpath', help='NetCDF4 files root path (i.e. the path of the folder containing the assimilated_solution_databases, mgbstandard_solution_databases folders)')
    parser.add_argument('-d', '--db_connect_url',
                        default=None,
                        help='The connection URL for the DB. (Default: "postgresql://hyfaa_backend:hyfaa_backend@localhost:5432/mgb_hyfaa")')
    parser.add_argument('-s', '--schema',
                        default='hyfaa',
                        help='DB schema (Default: "hyfaa")')
    parser.add_argument('--only_last_n_days',
                        type = int,
                        default=None,
                        help='if set, only the only_last_n_days days will be published (useful for publishing only a sample of data. Default: None)')
    args = parser.parse_args()

    ROOTPATH = args.rootpath
    global DATABASE_URI
    db_uri = args.db_connect_url or environ.get('DATABASE_URI')
    if db_uri:
        DATABASE_URI = db_uri
    global DATABASE_SCHEMA
    DATABASE_SCHEMA = args.schema
    only_last_n_days = args.only_last_n_days

    publish(ROOTPATH, only_last_n_days)

    # TODO
    # - add tests
    # - report errors / support prometheus ?
    # - integrate into hyfaa scheduler ? (makes more sense to run it just after the scheduler's computed the data)

if __name__ == '__main__':
    main()