from __future__ import print_function

import os
from datetime import timedelta

import pandas as pd

_PD_DATETIME_FACTOR = 1000000000

ROOT_PATH = '/Users/asapehrsson/dev/learn/hadoop_spark_jupyter/'

Y_W_TIMESTAMP_LT_COL_NAME = 'lt_timestamp'
Y_W_TIMESTAMP_GE_COL_NAME = 'ge_timestamp'

RATINGS_COL_NAME = 'ratings'
DATETIME_COL_NAME = 'datetime'
TIMESTAMP_COL_NAME = 'timestamp'
YEAR_WEEK_COL_NAME = 'y_w'

YEAR_WEEK_FORMAT_STR = "%d%02d"

MOVIE_LENS_DATA_RELATIVE_PATH = 'data/ml-latest-small'


def year_week_from_date_time(datetime):
    # days in week 53 in january will be market with last year
    if datetime.month == 1 and datetime.week > 51:
        result = (datetime.year - 1) * 100 + datetime.week
    else:
        result = datetime.year * 100 + datetime.week

    return result


def year_week_from_timestamp(timestamp):
    datetime = pd.to_datetime(timestamp * _PD_DATETIME_FACTOR)
    return year_week_from_date_time(datetime)


def add_year_week(df):
    # df[DATETIME_COL_NAME] = df[TIMESTAMP_COL_NAME].apply(lambda x: datetime.fromtimestamp(x))

    df[YEAR_WEEK_COL_NAME] = df[TIMESTAMP_COL_NAME].apply(lambda x: year_week_from_timestamp(x))

    print(df.dtypes)
    print(df[YEAR_WEEK_COL_NAME].max())
    return df


def fill_in_the_blanks(data_df, all_weeks_df):
    start_index = all_weeks_df[all_weeks_df[YEAR_WEEK_COL_NAME] == data_df[YEAR_WEEK_COL_NAME].min()].index.item()
    stop_index = all_weeks_df[all_weeks_df[YEAR_WEEK_COL_NAME] == data_df[YEAR_WEEK_COL_NAME].max()].index.item()

    all_weeks_for_movie = all_weeks_df[start_index: stop_index + 1]

    # https://data-lessons.github.io/library-python/04-merging-data/
    # this is a left outer join
    result = pd.merge(all_weeks_for_movie, data_df, how='left', left_on=YEAR_WEEK_COL_NAME, right_on=YEAR_WEEK_COL_NAME)
    result.reset_index(inplace=True, drop=True)
    result.fillna(value=0, inplace=True)
    return result


def _prepare_all_year_weeks_df(date_time_min, date_time_max):
    one_day = timedelta(hours=24)
    one_week = timedelta(days=7)

    current = date_time_min - (one_week * 2)

    if current.dayofweek > 0:
        current = current - timedelta(days=current.dayofweek)

    last_date_in_series = date_time_max + timedelta(weeks=2)

    pending = current
    weeks = []

    while current <= last_date_in_series:
        if (current.week > pending.week and current.year == pending.year) or \
                (current.year > pending.year and current.week == 1):
            row = [year_week_from_date_time(pending), "%d" % pending.timestamp(), "%d" % current.timestamp()]
            weeks.append(row)
            pending = current

        current = current + one_day

    all_year_weeks = pd.DataFrame(weeks)
    all_year_weeks.columns = [YEAR_WEEK_COL_NAME, Y_W_TIMESTAMP_GE_COL_NAME, Y_W_TIMESTAMP_LT_COL_NAME]

    return all_year_weeks


def get_year_weeks_datetime_df(root_path, data_df=None):
    path = os.path.join(root_path, MOVIE_LENS_DATA_RELATIVE_PATH)
    if not os.path.exists(path):
        os.makedirs(path)

    path = os.path.join(path, "year_week.csv")
    if os.path.isfile(path):
        print('exist')
        data = pd.read_csv(path)
    else:
        if data_df is None:
            data_df = get_ratings_df(root_path)

        date_time_min = pd.to_datetime(data_df[TIMESTAMP_COL_NAME].min() * _PD_DATETIME_FACTOR)
        date_time_max = pd.to_datetime(data_df[TIMESTAMP_COL_NAME].max() * _PD_DATETIME_FACTOR)

        data = _prepare_all_year_weeks_df(date_time_min, date_time_max)

        data.to_csv(path, index=False)

    return data


def get_ratings_df(root_path):
    path = get_ratings_path(root_path)

    if os.path.exists(path):
        data = pd.read_csv(path)
    else:
        data = pd.DataFrame([[0, 0, 0, 0]])
        print("no such file " + path)

    data.columns = ['userId', 'movieId', 'rating', 'timestamp']
    return data


def get_ratings_path(root_path):
    path = os.path.join(root_path, MOVIE_LENS_DATA_RELATIVE_PATH)
    path = os.path.join(path, "ratings.csv")
    return path


def _test():
    # a small data set, close to each side of year end
    # 1545609600 2018-12-24, 1546300800 2019-01-01
    data_set_one = {TIMESTAMP_COL_NAME: [1545609600, 1546300800], 'color': ['red', 'green']}

    # a small data set, close to beginning of two consecutive years
    # 1522244593 2017-03-28, 1546300800 2019-01-01
    data_set_two = {TIMESTAMP_COL_NAME: [1490659200, 1546300800], 'color': ['red', 'green']}

    data_df = pd.DataFrame(data_set_two)
    print(data_df)

    data_df = add_year_week(data_df)
    print(data_df)

    date_time_min = pd.to_datetime(data_df[TIMESTAMP_COL_NAME].min() * _PD_DATETIME_FACTOR)
    date_time_max = pd.to_datetime(data_df[TIMESTAMP_COL_NAME].max() * _PD_DATETIME_FACTOR)

    all_year_weeks = _prepare_all_year_weeks_df(date_time_min, date_time_max)
    print(all_year_weeks)

    data_df = fill_in_the_blanks(data_df, all_year_weeks)
    print(data_df)


def _test_weeks_with_53():
    # 1102938409 2004-12-13, 1168861609 2007-01-15
    date_time_min = pd.to_datetime(1102938409 * _PD_DATETIME_FACTOR)
    date_time_max = pd.to_datetime(1168861609 * _PD_DATETIME_FACTOR)

    all_year_weeks = _prepare_all_year_weeks_df(date_time_min, date_time_max)
    print(all_year_weeks)


def main():
    _test()


if __name__ == '__main__':
    main()
