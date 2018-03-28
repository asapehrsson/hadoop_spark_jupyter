from __future__ import print_function

from pyspark.sql.functions import *
from pyspark.sql.types import *

if sys.version >= '3':
    long = int

from pyspark.sql import SparkSession


def useCustomSchema(spark):
    # tried to parse timestamp into a
    #
    # userId,movieId,rating,timestamp
    # 1,31,2.5,1260759144
    schema = StructType() \
        .add("userId", IntegerType(), True) \
        .add("movieId", IntegerType(), True) \
        .add("rating", DoubleType(), True) \
        .add("timestamp", StringType(), True)

    movie_lens_data = spark \
        .read \
        .option("header", "true") \
        .schema(schema) \
        .csv("/Users/asapehrsson/dev/learn/hadoop_spark_jupyter/data/ml-latest-small/ratings.csv")

    return movie_lens_data


def infer_schema(spark):
    movie_lens_data = spark \
        .read \
        .option("inferSchema", "true") \
        .option("header", "true") \
        .csv("/Users/asapehrsson/dev/learn/hadoop_spark_jupyter/data/ml-latest-small/ratings.csv")
    return movie_lens_data


def no_of_ratings_per_movie(movie_lens_data):
    movie_lens_data \
        .groupBy("movieId") \
        .count() \
        .sort(desc("count")) \
        .limit(5) \
        .explain()


def avg_ratings_per_movie(movie_lens_data):
    movie_lens_data \
        .groupBy("movieId") \
        .avg("rating") \
        .limit(5) \
        .show()


def avg_ratings_per_weekday(movie_lens_data):
    movie_lens_data \
        .groupBy(date_format(from_unixtime("timestamp"), 'EEEE')) \
        .avg("rating") \
        .sort("avg(rating)") \
        .show()


def avg_ratings_per_hour(movie_lens_data):
    movie_lens_data \
        .groupBy(date_format(from_unixtime("timestamp"), 'HH')) \
        .avg("rating") \
        .sort("avg(rating)") \
        .show()


def main():
    spark = SparkSession \
        .builder \
        .appName("ALSExample") \
        .getOrCreate()

    spark.conf.set("spark.sql.shuffle.partitions", "5")

    movie_lens_data = infer_schema(spark)
    no_of_ratings_per_movie(movie_lens_data)

    print("done")


if __name__ == "__main__":
    main()
