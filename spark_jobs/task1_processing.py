from pyspark.sql import Row
from pyspark.sql.functions import avg, col, desc


def process_task1(spark, filepath):
    # Read movies data
    movies_df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(filepath)

    # Read ratings data
    ratings_df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv("data/ml-latest/ratings.csv")

    # Calculate average ratings
    avg_ratings = ratings_df.groupBy("movieId") \
        .agg(avg("rating").alias("avg_rating"))

    # Join with movies and sort
    result_df = movies_df.join(avg_ratings, "movieId") \
        .select("title", "avg_rating") \
        .orderBy(desc("avg_rating")) \
        .limit(20)

    # Convert to list of dictionaries for the template
    result = [row.asDict() for row in result_df.collect()]
    return result