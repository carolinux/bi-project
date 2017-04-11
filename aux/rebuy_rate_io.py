""" Methods that help reading and writing from the results database"""
#from collections import OrderedDict
import os

import pandas as pd
from sqlalchemy import create_engine, MetaData
from sqlalchemy.sql import select, text
#from sqlalchemy.orm import sessionmaker, scoped_session

from aux.datetools import get_date_range_list
import config

#def get_session():
#    if 'Session' not in globals():
#        global Session
#        engine = create_engine(os.environ["DB_URL_DATA_PUDDLE"])
#        Session = scoped_session(sessionmaker(bind=engine))
#    return Session()


def get_connection():
    if 'data_puddle_engine' not in globals():
        global data_puddle_engine
        data_puddle_engine = create_engine(os.environ["DB_URL_DATA_PUDDLE"])
    Connection = data_puddle_engine.connect()
    return Connection
        
        
def get_table(table_name, connection):
    """Get the full table description from the database (column names and types)"""
    m = MetaData()
    m.reflect(connection)
    return m.tables[table_name]

    
#def write_result_long(date, version, series, table_name):
#    """ Write every value in the pandas series as keys and values in the database
#    """
#    connection = get_connection()
#    table = get_table(table_name, connection)
#    for i, (key, value) in enumerate(series.iteritems()):
#        connection.execute(table.insert().values(
#            {"date":date, "version":version, "key":key, "value": str(value), "index":i}))
#    connection.close()


def write_result(date, version, data, table_name):
    """ Write every value in the pandas series as keys and values in the database
        Also adds an entry in the algorithm_runs
    """
    connection = get_connection()
    table = get_table(table_name, connection)
    if isinstance(data, pd.DataFrame):
        row_dicts = data.to_dict(orient='record')
    else:
        row_dicts = [data.to_dict()]
    trans = connection.begin()
    try:
        for row_dict in row_dicts:
            row_dict.update({'date': date, 'version': version})
            connection.execute(table.insert().values(row_dict))

    except:
        trans.rollback()
        connection.close()
        raise
    trans.commit()
 
    connection.close()


def get_dates_that_have_not_run_yet(start_date, current_date, version, table_name):
    """ Get the dates that have no recorded runs from a reference date until
    the current_date. If no backfilling is necessary, this will return just
    the current_date passed """

    dates = get_date_range_list(start_date, current_date)
    
    available_dates = get_list_of_available_dates(table_name)
    return set(dates).difference(available_dates)


def get_list_of_available_dates(table_name):
    """Returns a list of strings corresponding to the dates for which data is
    available in the table"""
    
    connection = get_connection()
    date_query = 'SELECT DISTINCT(date) FROM {0}'.format(table_name)
    results = connection.execute(date_query)
    connection.close()

    return sorted([entry[0].strftime('%Y-%m-%d') for entry in results])


def have_result_for_date_and_version(date_str, version, table_name):
    connection = get_connection()
    
    result = connection.execute(
        select(columns = ['*'], from_obj = text(table_name))
            .where(text("date = '{0}' AND version = '{1}'".format(date_str, version)))
    )

    rows = [row for row in result]
    connection.close()
    return len(rows) > 0


def can_cast_as_float(value):
    try:
        float(value)
        return True
    except:
        return False
        
def sql_from_file(filepath):
    """Returns a list of sql-commands that are stored in filepath
    """
    f = open(filepath)
    sql_commands = [command.strip() for command in f.read().split(';')]
    f.close()
    return sql_commands

def check_create_table(table_name):
    """Checks if a table exists in the database and creates it if it does not 
    The table description is given in the sql-file under the corresponding path"""
    connection = get_connection()
    m = MetaData()
    m.reflect(connection)
    if table_name not in m.tables:
        sql_file = open(config.filepaths[table_name]['table_creation'])
        connection.execute(sql_file.read().split(';')[0])
    connection.close()
    return True