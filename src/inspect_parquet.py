import logging
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

from src.utils.config_loader import config

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def inspect_output_parquet(spark: SparkSession):
    """
    Reads the processed Parquet data and performs validation checks.
    """
    output_path = str(Path(config['data']['processed_path']) / "pageviews.parquet")
    logging.info(f"Reading processed data from: {output_path}")

    try:
        df = spark.read.parquet(output_path)
        logging.info("Successfully read Parquet data.")
    except Exception as e:
        logging.error(f"Could not read Parquet file. Does it exist at '{output_path}'? Error: {e}")
        return

    # --- CHECK 1: Verify the Schema ---
    # Are the columns and data types what we expect?
    logging.info("1. Verifying schema:")
    df.printSchema()

    # --- CHECK 2: Look at a Sample of the Data ---
    # Does the data look reasonable?
    logging.info("2. Showing a sample of the data:")
    df.show(10, truncate=False)

    # --- CHECK 3: Check for Project Diversity ---
    # Since we removed the filter, we should see multiple projects.
    # This confirms we processed the raw data correctly.
    logging.info("3. Checking for project diversity (top 10 projects by row count):")
    project_counts = df.groupBy("project").agg(count("*").alias("row_count")).orderBy(col("row_count").desc())
    project_counts.show()

    # --- CHECK 4: Check for Nulls in Key Columns ---
    # Are there any unexpected nulls?
    null_project_count = df.filter(col("project").isNull()).count()
    logging.info(f"4. Found {null_project_count} rows with a null 'project' column.")


if __name__ == "__main__":
    spark_session = None
    try:
        spark_session = SparkSession.builder.appName("InspectParquet").getOrCreate()
        inspect_output_parquet(spark_session)
    except Exception as e:
        logging.critical(f"An error occurred during inspection: {e}", exc_info=True)
    finally:
        if spark_session:
            spark_session.stop()
