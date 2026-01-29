from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("Practice").getOrCreate()

# Reading the raw CSV
raw_df = spark.read.csv("/Users/khushalghathalia/Desktop/data-lakehouse/raw/synthetic_nyc_taxi.csv", header=True, inferSchema=True)

# 1. Drop any rows with NA values
cleaned_df = raw_df.dropna()

# Partitioning data
silver_df = cleaned_df.withColumn("pickup_date", F.to_date("pickup_datetime"))

# Writing Partition Data to Silver Folder
silver_path = "/Users/khushalghathalia/Desktop/data-lakehouse/silver/trips"
silver_df.write.mode("overwrite").partitionBy("pickup_date").parquet(silver_path)

print(f"Silver data written to {silver_path}")

trips_silver = spark.read.parquet("/Users/khushalghathalia/Desktop/data-lakehouse/data-lakehouse/silver/trips")

# 2. Loading data
payments_lookup = spark.read.csv("/Users/khushalghathalia/Desktop/data-lakehouse/raw/payment_lookup.csv", header=True, inferSchema=True)

# 3. Join Data
enriched_df = trips_silver.join(payments_lookup, on="payment_type", how="inner")

# 4. Aggregation on Total Revenue and Avg Tip per Payment Method
gold_df = enriched_df.groupBy("payment_description") \
    .agg(
        F.sum("total_amount").alias("total_revenue"),
        F.avg("tip_amount").alias("avg_tip"),
        F.count("vendor_id").alias("trip_count")
    )

# 5. Writing to Gold folder
gold_path = "/Users/khushalghathalia/Desktop/data-lakehouse/gold/revenue_summary"
gold_df.write.mode("overwrite").parquet(gold_path)

print(f"Gold data written to {gold_path}")
gold_df.show()