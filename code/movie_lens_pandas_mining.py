import pandas as pd

import movielens_helper as mh

pd.options.display.max_rows = 10

data = mh.get_ratings_df(mh.ROOT_PATH)
print(data)
data = mh.add_year_week(data)
print(data)
ratings = data \
    .groupby(mh.YEAR_WEEK_COL_NAME) \
    .count() \
    .sort_values(ascending=False, by="rating")

ratings.reset_index(inplace=True, drop=False)
print(ratings)


all_year_weeks = mh.get_year_weeks_datetime_df(mh.ROOT_PATH, data)
print(all_year_weeks)

foo = mh.fill_in_the_blanks(ratings, all_year_weeks)
# foo.head()
foo = data \
    .groupby(mh.YEAR_WEEK_COL_NAME) \
    .count()

print(foo)