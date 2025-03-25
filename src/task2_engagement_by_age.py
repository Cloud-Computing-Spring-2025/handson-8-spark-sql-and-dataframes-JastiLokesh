from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, round

# Initialize Spark Session
spark = SparkSession.builder.appName("EngagementByAgeGroup").getOrCreate()

# Load datasets
posts_df = spark.read.option("header", True).csv("input/posts.csv", inferSchema=True)
users_df = spark.read.option("header", True).csv("input/users.csv", inferSchema=True)

# Join posts with users data on UserID
joined_df = posts_df.join(users_df, "UserID")

# Group by age group and calculate average likes and retweets
engagement_df = joined_df.groupBy("AgeGroup") \
    .agg(
        avg("Likes").alias("Avg_Likes"),
        avg("Retweets").alias("Avg_Retweets")
    )

# Round the averages to 1 decimal place
engagement_df = engagement_df.withColumn("Avg_Likes", round(col("Avg_Likes"), 1))
engagement_df = engagement_df.withColumn("Avg_Retweets", round(col("Avg_Retweets"), 1))

# Calculate total engagement (avg likes + avg retweets) for ranking
engagement_df = engagement_df.withColumn(
    "Total_Engagement", 
    col("Avg_Likes") + col("Avg_Retweets")
)

# Order by total engagement descending
engagement_df = engagement_df.orderBy(col("Total_Engagement").desc())

# Select final columns for output
engagement_df = engagement_df.select("AgeGroup", "Avg_Likes", "Avg_Retweets")

# Save result
engagement_df.coalesce(1).write.mode("overwrite").csv("outputs/engagement_by_age.csv", header=True)