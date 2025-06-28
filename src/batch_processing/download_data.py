import logging
import os
from datetime import date, timedelta
from pathlib import Path

import requests

# --- Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Base URL for Wikimedia pageview statistics
BASE_URL = "https://dumps.wikimedia.org/other/pageviews"

# The local directory where raw data will be stored.
# We assume the script is run from the project root.
OUTPUT_DIR = Path("data/pageviews")


# --- End Configuration ---


def download_pageviews_for_day(target_date: date):
    """
    Downloads all 24 hourly pageview files for a given date.

    The file format is: pageviews-YYYYMMDD-HH0000.gz
    """
    year = target_date.strftime("%Y")
    month = target_date.strftime("%m")
    day = target_date.strftime("%d")

    # Create the specific subdirectory for the given date
    date_output_dir = OUTPUT_DIR / year / month / day
    date_output_dir.mkdir(parents=True, exist_ok=True)
    logger.info(f"Output directory created: {date_output_dir}")

    for hour in range(24):
        # Format the hour to be two digits (e.g., 01, 02, ..., 23)
        hour_str = f"{hour:02d}"
        filename = f"pageviews-{year}{month}{day}-{hour_str}0000.gz"
        file_url = f"{BASE_URL}/{year}/{year}-{month}/{filename}"
        output_path = date_output_dir / filename

        if output_path.exists():
            logger.info(f"File already exists, skipping: {filename}")
            continue

        try:
            logger.info(f"Downloading: {file_url}")
            response = requests.get(file_url, stream=True, timeout=30)
            response.raise_for_status()  # Raise an exception for bad status codes (4xx or 5xx)

            with open(output_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            logger.info(f"Successfully downloaded to: {output_path}")

        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to download {file_url}: {e}")
            # Optionally, you could clean up a partially downloaded file
            if output_path.exists():
                os.remove(output_path)


if __name__ == "__main__":
    # We will download data for 'yesterday' as it's guaranteed to be complete.
    # You can change this to any date you want to process.
    target_download_date = date.today() - timedelta(days=1)

    logger.info(f"--- Starting Wikimedia Pageviews Download for {target_download_date} ---")
    download_pageviews_for_day(target_download_date)
    logger.info("--- Download process finished ---")
