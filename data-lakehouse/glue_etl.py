import sys
import logging
import argparse
from datetime import datetime
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# Logging Setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

def get_args():
    """Parses CLI arguments."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", required=True, help="Base path for raw data")
    parser.add_argument("--target", required=True, help="Base path for output data")
    parser.add_argument("--run_date", required=False, help="Date to process (YYYY-MM-DD)")
    return parser.parse_known_args()[0]

# Local Bookmark Mechanism
BOOKMARK_FILE = "last_processed_date.txt"

def get_last_run_date():
    try:
        with open(BOOKMARK_FILE, "r") as f:
            return f.read().strip()
    except FileNotFoundError:
        return "1900-01-01"

def update_bookmark(date_str):
    with open(BOOKMARK_FILE, "w") as f:
        f.write(date_str)

def main():
    args = get_args()
    spark = SparkSession.builder.appName("NYC_Taxi_ETL").getOrCreate()
    
    # Use provided run_date or fallback to bookmark
    process_date = args.run_date if args.run_date else get_last_run_date()
    logger.info(f"Starting ETL for run_date: {process_date}")

    try:
        # Extract
        logger.info(f"Reading raw data from {args.source}")
        raw_df = spark.read.csv(f"{args.source}/synthetic_nyc_taxi.csv", header=True, inferSchema=True)
        payments_lookup = spark.read.csv(f"{args.source}/payment_lookup.csv", header=True, inferSchema=True)

        # Transform (Silver Layer)
        logger.info("Cleaning data and creating Silver layer...")
        cleaned_df = raw_df.dropna()
        silver_df = cleaned_df.withColumn("pickup_date", F.to_date("pickup_datetime"))
        
        # Filtering based on bookmark/run_date
        silver_df = silver_df.filter(F.col("pickup_date") >= process_date)

        # Load (Silver Layer)
        silver_path = f"{args.target}/silver/trips"
        logger.info(f"Writing Silver data to {silver_path}")
        silver_df.write.mode("overwrite").partitionBy("pickup_date").parquet(silver_path)

        # Transforming (Gold Layer)
        logger.info("Joining data and calculating aggregates for Gold layer...")
        # Re-reading to simulate a typical decoupled pipeline
        trips_silver = spark.read.parquet(silver_path)
        enriched_df = trips_silver.join(payments_lookup, on="payment_type", how="inner")

        gold_df = enriched_df.groupBy("payment_description") \
            .agg(
                F.sum("total_amount").alias("total_revenue"),
                F.avg("tip_amount").alias("avg_tip"),
                F.count("vendor_id").alias("trip_count")
            )

        #Load Gold Layer
        gold_path = f"{args.target}/gold/revenue_summary"
        logger.info(f"Writing Gold data to {gold_path}")
        gold_df.write.mode("overwrite").parquet(gold_path)

        # Update bookmark with current date for next run
        current_run_date = datetime.now().strftime("%Y-%m-%d")
        update_bookmark(current_run_date)
        logger.info(f"ETL Complete. Bookmark updated to {current_run_date}")

    except Exception as e:
        logger.error(f"ETL Job Failed: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()