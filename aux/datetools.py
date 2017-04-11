#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
This module gathers some data manipulation functions for convenienceused.
"""

from __future__ import division

import datetime
import re
import argparse


ZED_DATE_FORMAT = '(\d{1,2})[.](\d{1,2})[.](\d{4})$'

def to_py_date(date):
    """
    Convert date as string or int to python datetime.date. If
    already datetime.date, then leave untouched.
    TODO: This method should always return datetime.date, currently
    it returns datetime.datetime for two cases. Change after proper
    testing.
    """
    try:
        if isinstance(date, datetime.date):
            return date
        elif isinstance(date, int):
            return datetime.datetime.strptime(str(date), '%Y%m%d')
        else:
            if re.match(ZED_DATE_FORMAT, date) is not None:
                return datetime.datetime.strptime(
                    re.match(ZED_DATE_FORMAT, date).group(), '%d.%m.%Y')
            else:
                return datetime.datetime.strptime(date, '%Y-%m-%d')
    except ValueError as error:
        msg = "parsing of date '%s' failed: %s" % (date, error)
        raise argparse.ArgumentTypeError(msg)
        
        
def get_date_range_list(start_date, end_date, as_string=True,
                        is_hour_granularity=False, reverse=False):
    """
        Args:
            start_date: Python date
            end_date: Python date
            as_string: converts output dates to Y-m-d(-H) format.
            user_hour_granularity: return python dates separated by one hour.
        Returns:
            list of dates between start and end date (both inclusive)
    """
    import pandas as pd

    start_date = to_py_date(start_date)
    end_date = to_py_date(end_date)

    if is_hour_granularity:
        start_date = datetime.datetime.combine(
            start_date, datetime.time())
        end_date = datetime.datetime.combine(
            end_date, datetime.time(23, 0))
        freq = "H"
    else:
        freq = "D"

    date_range_list = pd.date_range(
        start_date, end_date, freq=freq).to_pydatetime().tolist()

    if not is_hour_granularity:
        date_range_list = map(lambda x: x.date(), date_range_list)

    date_range_list = sorted(date_range_list, reverse=reverse)

    if as_string:
        time_format = '%Y-%m-%d'
        if is_hour_granularity:
            time_format += '-%H'
        return [d.strftime(time_format) for d in date_range_list]
    else:
        return date_range_list
        
def get_shifted_date_str(date_string, shifted_n_days,
                         date_format='%Y-%m-%d', shift_in_future=True):
    """ Returns the shifted date
    :date_string: The date as string e.g. '2016-01-01'
    :shifted_n_days: The number of dates of shift
    :shift_in_future: Bollean flag, if True/False we shift in future/past
    :date_format: The string format of the date
    """
    datetime_object = datetime.datetime.strptime(date_string, date_format)
    if shift_in_future:
        shifted_date = datetime_object + datetime.timedelta(days=shifted_n_days)
    else:
        shifted_date = datetime_object - datetime.timedelta(days=shifted_n_days)
    return datetime.datetime.strftime(shifted_date, date_format)