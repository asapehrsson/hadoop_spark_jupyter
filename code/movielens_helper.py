from __future__ import print_function

from datetime import datetime, timedelta

import pandas as pd

RATINGS_COL_NAME = 'ratings'
DATETIME_COL_NAME = 'datetime'
TIMESTAMP_COL_NAME = 'timestamp'
YEAR_WEEK_COL_NAME = 'y_w'

YEAR_WEEK_FORMAT_STR = "%d%02d"


def add_year_week(data):
    data[DATETIME_COL_NAME] = data[TIMESTAMP_COL_NAME].apply(lambda x: datetime.fromtimestamp(x))
    data[YEAR_WEEK_COL_NAME] = data[DATETIME_COL_NAME].apply(lambda x: YEAR_WEEK_FORMAT_STR % (x.year, x.week))
    return data


def fill_in_the_blanks(data, all_weeks):
    start_index = all_weeks[all_weeks[YEAR_WEEK_COL_NAME] == data[YEAR_WEEK_COL_NAME].min()].index.item()
    stop_index = all_weeks[all_weeks[YEAR_WEEK_COL_NAME] == data[YEAR_WEEK_COL_NAME].max()].index.item()

    all_weeks_for_movie = all_weeks[start_index: stop_index]

    # https://data-lessons.github.io/library-python/04-merging-data/
    # this is a left outer join
    result = pd.merge(all_weeks_for_movie, data, how='left', left_on=YEAR_WEEK_COL_NAME, right_on=YEAR_WEEK_COL_NAME)

    return result


def prepare_all_year_weeks(data):
    one_day = timedelta(hours=24)
    one_week = timedelta(days=7)

    current = data[DATETIME_COL_NAME].min() - one_week

    if current.dayofweek > 0:
        current = current - timedelta(days=current.dayofweek)

    latest = data[DATETIME_COL_NAME].max() + one_week

    # was facing a strange bug here - got 201852, then 201801 and then 201901 when using
    # subtracting 'current' from last saved timestamp and comparing with 'one_week'

    year = 0
    week = 0

    weeks = []

    while current <= latest:
        if current.week > week and current.year == year or current.year > year:
            weeks.append(YEAR_WEEK_FORMAT_STR % (current.year, current.week))
            week = current.week
            year = current.year

        current = current + one_day

    all_year_weeks = pd.DataFrame({YEAR_WEEK_COL_NAME: weeks})
    all_year_weeks[RATINGS_COL_NAME] = 0
    return all_year_weeks


def main():
    # test with a small data set that passes year end
    data = pd.DataFrame({TIMESTAMP_COL_NAME: [1545609600, 1546300800]})
    print(data)

    data = add_year_week(data)
    print(data)

    all_year_weeks = prepare_all_year_weeks(data)
    print(all_year_weeks)

    data = fill_in_the_blanks(data, all_year_weeks)
    print(data)


if __name__ == '__main__':
    main()
