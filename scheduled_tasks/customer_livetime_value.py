# -*- coding: utf-8 -*-
"""
This module defines the DAG necessary for the daily export of the customer
lifetime value
"""
from __future__ import print_function
from airflow.operators.python_operator import PythonOperator
from airflow.models import DAG
import datetime
import os

import pandas as pd
import sqlalchemy

from aux import rebuy_rate_io
from algorithms.customer_lifetime_value_algorithm import get_daily_customer_value

VERSION = '1.5'

args = {
    'owner': 'airflow',
    'start_date': datetime.datetime.today() - datetime.timedelta(1),
    'email': "henrik.grundmann@siroop.ch"
}

dag = DAG(
    dag_id='customer_lifetime_value', default_args=args,
    schedule_interval="35 11 * * * ", catchup=False)

def get_sales_data(presto_engine, current_date):
    """ Get input data
    """
    presto_query = """
        SELECT
            date,
            email,
            abstract_sku,
            CAST(price_to_pay AS DECIMAL(15,2)) / 100 AS price_to_pay,
            quantity 
        FROM
            zedsales"""
    return pd.read_sql(presto_query, presto_engine)

def upload_customer_lifetime_value():
    """calculate and upload the values for the customer lifetime value"""
    presto_engine = sqlalchemy.create_engine(
        'presto://{0}/hive/default'.format(os.environ['PRESTO_ADDRESS']))

    rebuy_rate_io.check_create_table('customer_lifetime_value')

    start_report_date = '2016-01-01'#todo:replace by non-hardcoded date
    current_date = (datetime.datetime.today() - datetime.timedelta(1)).strftime('%Y-%m-%d')

    dates = rebuy_rate_io.get_dates_that_have_not_run_yet(start_report_date, 
                                                          current_date, 
                                                          VERSION,
                                                          'customer_lifetime_value')

    sales_df = get_sales_data(presto_engine, current_date)    
    for date in dates:
        daily_clv_df = get_daily_customer_value(date, sales_df)
        rebuy_rate_io.write_result(date, VERSION, daily_clv_df, 'customer_lifetime_value')

    
final_task = PythonOperator(
    task_id='customer_lifetime_value',
    python_callable=upload_customer_lifetime_value,
    retry_delay=datetime.timedelta(seconds=5),
    retries=3,
    trigger_rule="all_done",
    dag=dag)