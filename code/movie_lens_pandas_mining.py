import pandas as pd

import movielens_helper as mh

pd.options.display.max_rows = 10


def main():
    data_df = mh.get_ratings_df(mh.ROOT_PATH)
    print(data_df)

    data_df = mh.add_year_week(data_df)
    print(data_df)

    no_of_rating_per_week_df = data_df \
        .groupby(mh.YEAR_WEEK_COL_NAME) \
        .count() \
        .sort_values(ascending=False, by="rating")

    no_of_rating_per_week_df.reset_index(inplace=True, drop=False)
    print(no_of_rating_per_week_df)

    no_of_rating_per_week_df = mh.fill_in_the_blanks(no_of_rating_per_week_df,
                                                     mh.get_year_weeks_datetime_df(mh.ROOT_PATH, data_df))
    print(no_of_rating_per_week_df)

    foo = no_of_rating_per_week_df.sort_values(ascending=False, by="rating")

    print(foo)


if __name__ == "__main__":
    main()
