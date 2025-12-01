from pyspark.sql.functions import count, desc


def process_task2(spark):
    # Read data from MySQL
    tags_df = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:mysql://localhost:3306/movielens") \
        .option("dbtable", "tags") \
        .option("user", "movielens") \
        .option("password", "movielens123") \
        .load()

    users_df = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:mysql://localhost:3306/movielens") \
        .option("dbtable", "users") \
        .option("user", "movielens") \
        .option("password", "movielens123") \
        .load()

    # Join and process data
    joined_df = tags_df.join(users_df, "userId")

    # Group by tag and gender
    tag_gender_counts = joined_df.groupBy("tag", "gender") \
        .agg(count("*").alias("count")) \
        .orderBy(desc("count"))

    # Separate male and female results
    male_tags = tag_gender_counts.filter(col("gender") == "M") \
        .limit(10) \
        .collect()

    female_tags = tag_gender_counts.filter(col("gender") == "F") \
        .limit(10) \
        .collect()

    # Format results
    result = {
        "male": [{"tag": row.tag, "count": row.count} for row in male_tags],
        "female": [{"tag": row.tag, "count": row.count} for row in female_tags]
    }

    return result