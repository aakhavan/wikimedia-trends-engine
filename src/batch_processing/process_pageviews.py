# src/batch_processor/process_pageviews.py
import argparse
import logging
import os
from datetime import date, timedelta

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, split
from pyspark.sql.types import (IntegerType, StringType, StructField,
                               StructType)

# --- Import the centralized config object ---
from src.utils.config_loader import config

# --- Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Load JAR paths from config ---
# This is much cleaner and more maintainable.
spark_cfg = config['spark_config']
POSTGRES_JAR_PATH = spark_cfg['jar_paths']['postgres']
SNOWFLAKE_JAR_PATH = spark_cfg['jar_paths']['snowflake']
ALL_JARS = f"{POSTGRES_JAR_PATH},{SNOWFLAKE_JAR_PATH}"


def create_spark_session() -> SparkSession:
    """Creates and configures a SparkSession with the necessary connectors."""
    logger.info("Creating SparkSession...")
    try:
        spark = (
            SparkSession.builder.appName("WikimediaPageviewsProcessing")
            .config("spark.jars", ALL_JARS)
            .getOrCreate()
        )
        logger.info("SparkSession created successfully.")
        return spark
    except Exception as e:
        logger.error(f"Failed to create SparkSession: {e}")
        raise


def process_pageviews(spark: SparkSession, target_date: date) -> DataFrame:
    """
    Reads raw pageview data from the local filesystem, cleans it,
    and returns a transformed DataFrame.
    """
    logger.info(f"Starting pageview processing for date: {target_date}")

    # Construct the input path from the configuration
    raw_data_path = config['data_paths']['raw_pageviews']
    year = target_date.strftime("%Y")
    month = target_date.strftime("%m")
    day = target_date.strftime("%d")
    input_path = f"{raw_data_path}/{year}/{month}/{day}/*.gz"

    logger.info(f"Reading data from: {input_path}")

    # Spark's text reader creates a single column named "value" by default
    raw_df = spark.read.format("text").load(input_path)

    logger.info("Cleaning and transforming data...")
    cleaned_df = (
        raw_df
        # Split the single text column into multiple columns based on spaces
        .withColumn("split_cols", split(col("value"), " "))
        # Filter out malformed rows that don't have at least 4 parts
        .filter(col("split_cols").isNotNull() & (col("split_cols").getItem(3).isNotNull()))
        # Create the final columns from the split array
        .withColumn("project", col("split_cols").getItem(0))
        .withColumn("page_title", col("split_cols").getItem(1))
        .withColumn("view_counts", col("split_cols").getItem(2).cast(IntegerType()))
        # Filter for English Wikipedia only, as per the project plan
        .filter(col("project") == "en.wikipedia")
        # Add the date of the data as a partition-friendly column
        .withColumn("view_date", col(f"'{target_date}'").cast("date"))
        # Select and order the final columns for a clean schema
        .select("view_date", "project", "page_title", "view_counts")
    )
    logger.info("Data transformation complete.")
    return cleaned_df


def write_to_postgres(df: DataFrame, pg_config: dict):
    """Writes a DataFrame to a PostgreSQL table using provided config."""
    table_name = pg_config['table']
    logger.info(f"Writing data to PostgreSQL table: {pg_config['database']}.public.{table_name}")

    jdbc_url = f"jdbc:postgresql://{pg_config['host']}:{pg_config['port']}/{pg_config['database']}"

    # Credentials are read securely from environment variables
    properties = {
        "user": os.getenv("PG_USER"),
        "password": os.getenv("PG_PASSWORD"),
        "driver": "org.postgresql.Driver"
    }

    try:
        # Use mode "overwrite" to make the job idempotent
        df.write.jdbc(url=jdbc_url, table=table_name, mode="overwrite", properties=properties)
        logger.info(f"Successfully wrote data to PostgreSQL table: {table_name}")
    except Exception as e:
        logger.error(f"Failed to write data to PostgreSQL: {e}")
        raise


def write_to_snowflake(df: DataFrame, sf_config: dict):
    """Writes a DataFrame to a Snowflake table using provided config."""
    target_table = f"{sf_config['database']}.{sf_config['schema']}.{sf_config['table']}"
    logger.info(f"Writing data to Snowflake table: {target_table}")

    # Credentials and account info are read securely from environment variables
    sf_options = {
        "sfURL": f"{os.getenv('SNOWFLAKE_ACCOUNT')}.snowflakecomputing.com",
        "sfUser": os.getenv("SNOWFLAKE_USER"),
        "sfPassword": os.getenv("SNOWFLAKE_PASSWORD"),
        "sfDatabase": sf_config['database'],
        "sfSchema": sf_config['schema'],
        "sfWarehouse": sf_config['warehouse'],
        "dbtable": sf_config['table'],
    }

    try:
        (
            df.write.format("snowflake")
            .options(**sf_options)
            .mode("overwrite")
            .save()
        )
        logger.info(f"Successfully wrote data to {target_table}")
    except Exception as e:
        logger.error(f"Failed to write data to Snowflake: {e}")
        raise


def main():
    """Main ETL script execution, parameterized for different targets."""
    parser = argparse.ArgumentParser(description="Spark ETL job for Wikimedia pageviews.")
    parser.add_argument(
        '--target',
        type=str,
        choices=['postgres', 'snowflake'],
        default='postgres',
        help="The destination for the processed data. Defaults to 'postgres'."
    )
    args = parser.parse_args()

    # Process data for 2 days ago to ensure all hourly files are available and complete.
    target_process_date = date.today() - timedelta(days=2)

    spark = None
    try:
        spark = create_spark_session()
        pageviews_df = process_pageviews(spark, target_process_date)

        logger.info("Sample of transformed data:")
        pageviews_df.show(10, truncate=False)

        if args.target == 'postgres':
            pg_config = config['data_warehouse']['postgres']
            write_to_postgres(pageviews_df, pg_config)
        elif args.target == 'snowflake':
            sf_config = config['data_warehouse']['snowflake']
            write_to_snowflake(pageviews_df, sf_config)
        else:
            # This case is technically handled by argparse 'choices', but it's good practice
            logger.error(f"Invalid target specified: {args.target}")
            raise ValueError("Invalid target specified.")

    except Exception as e:
        logger.critical(f"ETL job failed: {e}", exc_info=True)
        # In a real orchestrator, this would trigger an alert
    finally:
        if spark:
            logger.info("Stopping SparkSession.")
            spark.stop()


if __name__ == "__main__":
    main()