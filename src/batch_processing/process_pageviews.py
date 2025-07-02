import argparse
import logging
from datetime import date, timedelta
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, input_file_name, regexp_extract
from pyspark.sql.types import StructType, StructField, StringType, LongType

from src.utils.config_loader import config

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def process_wikimedia_pageviews(spark: SparkSession, processing_date: date, is_dev: bool = False):
    """
    Reads raw gzipped pageview data, processes it, and writes the result
    to a partitioned Parquet dataset.

    :param spark: The SparkSession object.
    :param processing_date: The date to process.
    :param is_dev: If True, process only a single file for development.
    """
    year, month, day = processing_date.strftime("%Y"), processing_date.strftime("%m"), processing_date.strftime("%d")

    schema = StructType([
        StructField("project", StringType(), True),
        StructField("page_title", StringType(), True),
        StructField("view_counts", LongType(), True),
        StructField("response_size", LongType(), True),
    ])

    # This is the new logic that uses the is_dev flag
    if is_dev:
        # For local development, process only one hour's data for speed.
        input_path = str(Path(config['data']['pageviews_path']) / year / month / day / "pageviews-*-100000.gz")
        logging.info("Running in development mode, processing a single file.")
    else:
        # In production, process all files for the given day.
        input_path = str(Path(config['data']['pageviews_path']) / year / month / day / "*.gz")
        logging.info("Running in production mode, processing all files.")

    logging.info(f"Reading data from: {input_path}")

    df = spark.read.csv(input_path, schema=schema, sep=" ", header=False)

    if df.rdd.isEmpty():
        logging.warning(f"No raw data found for date {processing_date.strftime('%Y-%m-%d')}. Skipping processing.")
        return

    # The filter is currently removed for our test, but can be re-enabled.
    pageviews_df = df.withColumn("filename", input_file_name()) \
        .withColumn("view_date", to_date(regexp_extract(col("filename"), r'(\d{8})', 1), 'yyyyMMdd')) \
        .select(
        col("view_date"),
        col("project"),
        col("page_title"),
        col("view_counts")
    )  # .filter(col("project") == "en.wikipedia")

    pageviews_df.cache()

    final_count = pageviews_df.count()
    logging.info(
        f"Found {final_count:,} total rows for date {processing_date.strftime('%Y-%m-%d')}.")

    if final_count == 0:
        logging.warning("No data found. The output will be empty.")
    else:
        logging.info("Sample of transformed data (all projects):")
        pageviews_df.show(10, truncate=False)

    output_path = str(Path(config['data']['processed_path']) / "pageviews.parquet")
    logging.info(f"Writing {final_count:,} rows to Parquet format at: {output_path}")

    try:
        pageviews_df.write.mode("overwrite").partitionBy("view_date").parquet(output_path)
        logging.info(f"Successfully wrote data for {processing_date.strftime('%Y-%m-%d')} to Parquet.")
    except Exception as e:
        logging.error(f"Failed to write data to Parquet. Error: {e}", exc_info=True)
        raise
    finally:
        pageviews_df.unpersist()


def main():
    """Main function to initialize Spark and trigger the processing."""
    parser = argparse.ArgumentParser(description="Process Wikimedia pageview data for a specific date.")
    parser.add_argument(
        '--date',
        type=str,
        help="The date to process data for in YYYY-MM-DD format. Defaults to two days ago."
    )
    # Add a flag for development mode
    parser.add_argument(
        '--dev',
        action='store_true',  # This makes it a flag, no value needed
        help="Run in development mode, processing only a small subset of data."
    )
    args = parser.parse_args()

    if args.date:
        try:
            target_date = date.fromisoformat(args.date)
        except ValueError:
            logging.error(f"Invalid date format: '{args.date}'. Please use YYYY-MM-DD.")
            return
    else:
        target_date = date.today() - timedelta(days=2)

    spark = None
    try:
        spark = SparkSession.builder \
            .appName(f"WikimediaPageviewsProcessing-{target_date.strftime('%Y-%m-%d')}") \
            .getOrCreate()

        # Pass the new flag to the processing function
        process_wikimedia_pageviews(spark, target_date, is_dev=args.dev)

    except Exception as e:
        logging.critical(f"An unexpected error occurred in the Spark job: {e}", exc_info=True)
    finally:
        if spark:
            logging.info("Stopping SparkSession.")
            spark.stop()


if __name__ == "__main__":
    main()