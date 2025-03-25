from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum

spark = SparkSession.builder.appName("TopVerifiedUsers").getOrCreate()

# Load datasets
posts_df = spark.read.option("header", True).csv("input/posts.csv", inferSchema=True)
users_df = spark.read.option("header", True).csv("input/users.csv", inferSchema=True)

# TODO: Implement the task here
# Filter for verified users only
verified_users_df = users_df.filter(col("Verified") == True)

# Join verified users with their posts
verified_posts_df = posts_df.join(verified_users_df, "UserID")

# Calculate total reach (likes + retweets) for each user
user_reach_df = verified_posts_df.groupBy("UserID", "Username") \
    .agg(
        _sum(col("Likes") + col("Retweets")).alias("Total_Reach")
    )

# Sort by total reach descending and get top 5
top_verified = user_reach_df.orderBy(col("Total_Reach").desc()).limit(5)

# Select final columns for output
top_verified = top_verified.select("Username", "Total_Reach")

# Save result
top_verified.coalesce(1).write.mode("overwrite").csv("outputs/top_verified_users.csv", header=True)
