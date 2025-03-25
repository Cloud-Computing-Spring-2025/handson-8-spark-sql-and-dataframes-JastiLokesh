from pyspark.sql import SparkSession
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col, count, trim

# Initialize Spark Session
spark = SparkSession.builder.appName("HashtagTrends").getOrCreate()

# Load posts data
posts_df = spark.read.option("header", True).csv("input/posts.csv")

# Split the Hashtags column into individual hashtags and count frequency
# First, split the hashtags and explode them into separate rows
hashtags_df = posts_df.select(
    explode(split(col("Hashtags"), ",")).alias("Hashtag")
)

# Clean hashtags (remove any extra spaces and empty values)
hashtags_df = hashtags_df.filter(col("Hashtag").isNotNull() & (col("Hashtag") != ""))
hashtags_df = hashtags_df.withColumn("Hashtag", trim(col("Hashtag")))

# Count hashtag frequency and sort in descending order
hashtag_counts = hashtags_df.groupBy("Hashtag") \
    .agg(count("*").alias("Count")) \
    .orderBy(col("Count").desc())

# Limit to top hashtags (optional)
top_hashtags = hashtag_counts.limit(10)

# Save result
hashtag_counts.coalesce(1).write.mode("overwrite").csv("outputs/hashtag_trends.csv", header=True)