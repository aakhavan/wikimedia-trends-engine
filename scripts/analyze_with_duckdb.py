# scripts/analyze_with_duckdb.py
import duckdb
import pandas as pd
from pathlib import Path
import sys
import argparse

# This allows the script to be run from the project root
# and find the 'src' module to load the configuration.
sys.path.append(str(Path(__file__).parent.parent))
from src.utils.config_loader import config


def analyze_output_with_duckdb(date: str | None = None):
    """
    Uses DuckDB to run SQL queries on the processed Parquet file for fast,
    local analysis without needing Spark or Docker.

    Args:
        date: An optional date string in 'YYYY-MM-DD' format to analyze a specific partition.
              If None, all partitions will be analyzed.
    """
    base_processed_dir = Path(config['data']['processed_path']) / "pageviews.parquet"

    # --- Dynamically build the path based on the provided date ---
    if date:
        print(f"🦆 Analyzing data for a specific date: {date}\n")
        target_dir = base_processed_dir / f"view_date={date}"
        parquet_path_pattern = str(target_dir / "*.parquet")
    else:
        print(f"🦆 Analyzing data for all available dates...\n")
        target_dir = base_processed_dir
        parquet_path_pattern = str(target_dir / "*/*.parquet")

    print(f"Reading Parquet files from: {parquet_path_pattern}\n")

    try:
        # --- CORRECTED and more robust check for file existence ---
        # This version works on all operating systems and is more efficient.
        if not target_dir.exists() or not any(target_dir.glob("*.parquet")):
            print("❌ Error: No processed Parquet files found for the specified criteria.")
            if date:
                print(f"   Please ensure the batch job for '{date}' has run successfully.")
            else:
                print(f"   Please run the batch processing job first to generate output.")
            return

        # Connect to an in-memory DuckDB database
        con = duckdb.connect(database=':memory:')

        # Use a variable for the FROM clause to avoid repetition
        from_clause = f"FROM read_parquet('{parquet_path_pattern}')"

        print("--- CHECK 1: Verifying Schema ---")
        schema_df = con.execute(f"DESCRIBE SELECT * {from_clause};").df()
        print(schema_df)
        print("\n" + "=" * 50 + "\n")

        print("--- CHECK 2: Showing a Sample of the Data ---")
        sample_df = con.execute(f"SELECT * {from_clause} LIMIT 10;").df()
        print(sample_df)
        print("\n" + "=" * 50 + "\n")

        print("--- CHECK 3: Checking for Project Diversity (Top 10) ---")
        project_counts_df = con.execute(f"""
            SELECT
                project,
                COUNT(*) AS row_count
            {from_clause}
            GROUP BY project
            ORDER BY row_count DESC
            LIMIT 10;
        """).df()
        print(project_counts_df)
        print("\n" + "=" * 50 + "\n")

        print("--- CHECK 4: Verifying No Nulls in Key Columns ---")
        null_check_df = con.execute(f"""
            SELECT
                SUM(CASE WHEN project IS NULL THEN 1 ELSE 0 END) AS null_project_count,
                SUM(CASE WHEN page_title IS NULL THEN 1 ELSE 0 END) AS null_title_count
            {from_clause};
        """).df()
        print(null_check_df)

    except duckdb.IOException as e:
        print(f"❌ DuckDB Error: Could not read the Parquet file.")
        print(f"   Please ensure the file exists and is not corrupted.")
        print(f"   Details: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")


if __name__ == "__main__":
    # --- Command-Line Argument Parsing ---
    parser = argparse.ArgumentParser(
        description="Analyze processed Wikimedia pageview data using DuckDB.",
        formatter_class=argparse.RawTextHelpFormatter  # For better help text formatting
    )
    parser.add_argument(
        "--date",
        type=str,
        help="Optional: The date of the data to analyze in YYYY-MM-DD format.\n"
             "If not provided, the script will analyze all available date partitions."
    )
    args = parser.parse_args()

    # Configure pandas for better console output
    pd.set_option('display.max_colwidth', None)
    pd.set_option('display.width', 1000)

    print("This script analyzes the processed Parquet data using DuckDB.")
    print("It runs locally and does not require Docker.\n")

    analyze_output_with_duckdb(date=args.date)