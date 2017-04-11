import pandas as pd
from collections import OrderedDict

from aux.datetools import get_shifted_date_str

UNIQUE_USER_IDENTIFIER = 'email'
WINDOW_LAG_DAYS = [7, 28, 91, 182, 364]


def get_window_repeated_buyers(df, current_date, reference_buyers_set,
                               window_n_days):
    """
    The window is defined using the FTB from the current date and checking
    if there are repated buyers within the next days including the last day of
    the window.
    :df: the sales DataFrame
    :reference_buyers_set: The reference buyers set to intersect with.
    It can be the "First Time Buyers on this date"
    Or the "Total Buyers on this date"
    :window_n_days: The days window of the rebuy rate calculation
    """

    next_date = get_shifted_date_str(current_date, 1)
    window_date = get_shifted_date_str(current_date, window_n_days)
    last_data_date = df['date'].values[-1]

    # check that we do not ask for a date that does not exist in our data
    if pd.to_datetime(last_data_date) < pd.to_datetime(window_date):
        return None

    # compute the intersection of the reference buyers the window ones
    window_buyers_set = \
        df[next_date:window_date].apply(set)[UNIQUE_USER_IDENTIFIER]
    return len(reference_buyers_set.intersection(window_buyers_set))


def get_daily_rebuy_rate_values(current_date, df):
    """ Calculate the rebuy rate information for the current date
    :df: is the input pandas DataFrame keeping the information of the
        date and the emails of any users bought something.
    """
    df.index = pd.to_datetime(df['date']).values
    df = df.sort_index()

    output_d = OrderedDict()

    previous_date = get_shifted_date_str(current_date, 1, shift_in_future=False)
    previous_buyers_set = df[:previous_date].apply(set)[UNIQUE_USER_IDENTIFIER]
    current_buyers_set = df[current_date:current_date]\
        .apply(set)[UNIQUE_USER_IDENTIFIER]
    first_time_buyers_set = current_buyers_set - previous_buyers_set

    output_d['date'] = current_date

    calendar_week = pd.to_datetime(current_date).isocalendar()[1]
    output_d['calendar_week'] = calendar_week

    month = pd.to_datetime(current_date).strftime("%B")
    output_d['month'] = month

    n_total_buyers_all_time = \
        len(df[:current_date].apply(set)[UNIQUE_USER_IDENTIFIER])
    output_d['total_buyers_all_time'] = n_total_buyers_all_time

    n_first_time_buyers = len(first_time_buyers_set)
    output_d['first_time_buyers'] = n_first_time_buyers

    for window_n_days in WINDOW_LAG_DAYS:
        output_d['first_time_buyers_repeated_within_' + \
                 str(window_n_days) + '_days'] = \
            get_window_repeated_buyers(
                df, current_date, first_time_buyers_set, window_n_days)

    n_total_buyers = len(df[current_date:current_date]\
                         .apply(set)[UNIQUE_USER_IDENTIFIER])
    output_d['total_buyers'] = n_total_buyers

    for window_n_days in WINDOW_LAG_DAYS:
        output_d['total_buyers_repeated_within_' + \
                 str(window_n_days) + '_days'] = \
            get_window_repeated_buyers(
                df, current_date, current_buyers_set, window_n_days)

    return pd.Series(output_d)
