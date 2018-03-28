import pandas as pd

import movielens_helper as mh

pd.options.display.max_rows = 10

ROOT_PATH = '/Users/asapehrsson/dev/learn/hadoop_spark_jupyter/'

data = pd.read_csv(ROOT_PATH + '/data/ml-latest-small/ratings.csv')
data = mh.add_year_week(data)

ratings = data \
    .groupby(mh.YEAR_WEEK_COL_NAME) \
    .count() \
    .sort_values(ascending=False, by="rating")

ratings.reset_index(inplace=True, drop=False)


all_year_weeks = mh.prepare_all_year_weeks(data)

foo = mh.fill_in_the_blanks(ratings, all_year_weeks)
# foo.head()
foo = data \
    .groupby(mh.YEAR_WEEK_COL_NAME) \
    .count()

print(foo)