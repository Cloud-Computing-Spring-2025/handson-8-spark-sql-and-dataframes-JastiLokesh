from pyspark.sql import SparkSession
from pyspark.sql.functions import when, avg, col, round

# Initialize Spark Session
spark = SparkSession.builder.appName("SentimentVsEngagement").getOrCreate()

# Load posts data
posts_df = spark.read.option("header", True).csv("input/posts.csv", inferSchema=True)

# Categorize sentiment: Positive (> 0.3), Neutral (-0.3 to 0.3), Negative (< -0.3)
sentiment_df = posts_df.withColumn(
    "Sentiment",
    when(col("SentimentScore") > 0.3, "Positive")
    .when(col("SentimentScore") < -0.3, "Negative")
    .otherwise("Neutral")
)

# Group by sentiment category and calculate average likes and retweets
sentiment_stats = sentiment_df.groupBy("Sentiment") \
    .agg(
        avg("Likes").alias("Avg_Likes"),
        avg("Retweets").alias("Avg_Retweets")
    )

# Round the averages to 1 decimal place
sentiment_stats = sentiment_stats.withColumn("Avg_Likes", round(col("Avg_Likes"), 1))
sentiment_stats = sentiment_stats.withColumn("Avg_Retweets", round(col("Avg_Retweets"), 1))

# Calculate total engagement for sorting
sentiment_stats = sentiment_stats.withColumn(
    "Total_Engagement", 
    col("Avg_Likes") + col("Avg_Retweets")
)

# Order by total engagement descending
sentiment_stats = sentiment_stats.orderBy(col("Total_Engagement").desc())

# Select final columns for output
sentiment_stats = sentiment_stats.select("Sentiment", "Avg_Likes", "Avg_Retweets")

# Save result
sentiment_stats.coalesce(1).write.mode("overwrite").csv("outputs/sentiment_engagement.csv", header=True)