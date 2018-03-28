# https://spark.apache.org/docs/latest/mllib-collaborative-filtering.html
# https://spark.apache.org/docs/2.1.0/ml-collaborative-filtering.html#examples
# https://github.com/apache/spark/blob/master/examples/src/main/python/ml/als_example.py

from __future__ import print_function

import sys

if sys.version >= '3':
    long = int

from pyspark.sql import SparkSession
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.sql import Row


def extract(part):
    try:
        return Row(userId=int(part[0]), movieId=int(part[1]), rating=float(part[2]), timestamp=long(part[3]))
    except:
        return None


def main():
    spark = SparkSession \
        .builder \
        .appName("ALSExample") \
        .getOrCreate()

    # ratings_list = [i.strip().split(",") for i in open('/Users/asapehrsson/dev/learn/hadoop_spark_jupyter/data/ml-latest-small/ratings.csv', 'r').readlines()]
    # stream = open('/Users/asapehrsson/dev/learn/hadoop_spark_jupyter/data/ml-latest-small/movies.csv')

    # $example on$
    lines = spark.read.text("/Users/asapehrsson/dev/learn/hadoop_spark_jupyter/data/ml-latest-small/ratings.csv").rdd
    parts = lines.map(lambda row: row.value.split(","))
    ratingsRDD = parts.map(extract).filter(lambda x: x is not None)

    ratings = spark.createDataFrame(ratingsRDD)
    (training, test) = ratings.randomSplit([0.8, 0.2])

    # Build the recommendation model using ALS on the training data
    # Note we set cold start strategy to 'drop' to ensure we don't get NaN evaluation metrics
    als = ALS(maxIter=5, regParam=0.01, userCol="userId", itemCol="movieId", ratingCol="rating",
              coldStartStrategy="drop")
    model = als.fit(training)

    # Evaluate the model by computing the RMSE on the test data
    predictions = model.transform(test)
    evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating",
                                    predictionCol="prediction")
    rmse = evaluator.evaluate(predictions)
    print("Root-mean-square error = " + str(rmse))

    # Generate top 10 movie recommendations for each user
    userRecs = model.recommendForAllUsers(10)
    # Generate top 10 user recommendations for each movie
    movieRecs = model.recommendForAllItems(10)

    # Generate top 10 movie recommendations for a specified set of users
    users = ratings.select(als.getUserCol()).distinct().limit(3)
    userSubsetRecs = model.recommendForUserSubset(users, 10)
    # Generate top 10 user recommendations for a specified set of movies
    movies = ratings.select(als.getItemCol()).distinct().limit(3)
    movieSubSetRecs = model.recommendForItemSubset(movies, 10)
    # $example off$
    userRecs.show()
    movieRecs.show()
    userSubsetRecs.show()
    movieSubSetRecs.show()

    p = movieRecs.toPandas()

    spark.stop()


if __name__ == "__main__":
    main()
