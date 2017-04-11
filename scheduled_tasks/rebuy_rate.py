# -*- coding: utf-8 -*-
"""
This file is defining the DAG necessary to schedule the export of the rebuy rate
"""

# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
from airflow.operators.python_operator import PythonOperator
from airflow.models import DAG

import datetime
import os

import pandas as pd
import sqlalchemy

from aux import rebuy_rate_io
from algorithms.rebuy_rates_algorithm import get_daily_rebuy_rate_values

args = {
    'owner': 'airflow',
    'start_date': datetime.datetime.today() - datetime.timedelta(1),
    'email': "henrik.grundmann@siroop.ch"
}

dag = DAG(
    dag_id='rebuy_rate', default_args=args,
    schedule_interval="0 0 * * * ", catchup=False)



VERSION = "1.5"


def get_sales_data(presto_engine, current_date):
    """ Get input data
    """
    sql = "SELECT date, email FROM zedsales WHERE date <= '{0}' GROUP BY email, date".format(current_date)
    return pd.read_sql(sql, presto_engine)

def rebuy_rate_calculate_and_export():
    
    start_report_date = '2016-01-01'#todo:replace by non-hardcoded date
    current_date = (datetime.datetime.today() - datetime.timedelta(1)).strftime('%Y-%m-%d')
    
    rebuy_rate_io.check_create_table('rebuy_rate')
    presto_engine = sqlalchemy.create_engine(
        'presto://{0}/hive/default'.format(os.environ['PRESTO_ADDRESS']))
    df = get_sales_data(presto_engine, current_date)
    dates = rebuy_rate_io.get_dates_that_have_not_run_yet(start_report_date, 
                                                          current_date,
                                                          VERSION,
                                                          'rebuy_rate')

    for date in dates:
        rebuy_rate_series = get_daily_rebuy_rate_values(date, df)
        rebuy_rate_io.write_result(date, VERSION, rebuy_rate_series, 'rebuy_rate')

    
final_task = PythonOperator(
    task_id='upload_clv',
    python_callable=rebuy_rate_calculate_and_export,
    retry_delay=datetime.timedelta(seconds=5),
    retries=3,
    trigger_rule="all_done",
    dag=dag)