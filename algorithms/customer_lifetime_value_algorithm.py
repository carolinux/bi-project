# -*- coding: utf-8 -*-
"""
This module provides algorithms to calculate customer lifetime values for
cohorts based on pandas dataframes
"""

import pandas as pd

from aux.datetools import to_py_date

UNIQUE_USER_IDENTIFIER = 'email'


def get_daily_customer_value(date, df):
    """ Calulate the customer_lifetime for the members of the group defined by
    the groupby_key function for the given day
    
    Expects a dataframe df with the columns: 
    [u'date', u'email', u'abstract_sku', u'price_to_pay', u'quantity']
    
    dates is a list of dates for which the customer lifetime value should be
    calculated
    """
    #get the date_string corresponding to current_date
    date = to_py_date(date).strftime('%Y-%m-%d')

    #calculate the GMV for each row
    df['accumulated_gmv'] = df['price_to_pay'].astype(float) * df['quantity'].astype(int)

    #calculate the total gmv for each day for the customers (for the case of 
    #several items or several sales on per day
    df = df.groupby(['date', 'email'])['accumulated_gmv'].sum().reset_index()
    #calculate the month of first contact
    df['month_of_first_contact'] = df.groupby('email')['date'].transform(min).apply(lambda x:x [:7])
    df = df.groupby(by=['date', 'month_of_first_contact']).sum().groupby(level=[0]).cumsum().reset_index()

    return df[df['date'] == date]