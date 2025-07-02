import argparse
import logging
from datetime import date, timedelta
from pathlib import Path

import requests

from src.utils.config_loader import config

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def download_pageviews_data(target_date: date):
    """Downloads all 24 hourly pageview files for a specific date from Wikimedia."""
    year, month, day = target_date.strftime("%Y"), target_date.strftime("%m"), target_date.strftime("%d")

    base_url = config['wikimedia']['base_url']
    output_dir = Path(config['data']['pageviews_path']) / year / month / day
    output_dir.mkdir(parents=True, exist_ok=True)
    logging.info(f"Output directory set to: {output_dir}")

    for hour in range(24):
        filename = f"pageviews-{year}{month}{day}-{hour:02d}0000.gz"
        file_url = f"{base_url}/{year}/{year}-{month}/{filename}"
        output_path = output_dir / filename

        if output_path.exists():
            logging.info(f"File already exists, skipping: {filename}")
            continue

        try:
            logging.info(f"Downloading: {file_url}")
            response = requests.get(file_url, stream=True, timeout=30)
            response.raise_for_status()

            with open(output_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            logging.info(f"Successfully downloaded to: {output_path}")

        except requests.exceptions.RequestException as e:
            logging.error(f"Failed to download {file_url}. Error: {e}")

def main():
    """Main function to parse arguments and trigger the download."""
    parser = argparse.ArgumentParser(description="Download Wikimedia pageview data for a specific date.")
    parser.add_argument(
        '--date',
        type=str,
        help="The date to download data for in YYYY-MM-DD format. Defaults to yesterday."
    )
    args = parser.parse_args()

    if args.date:
        try:
            target_date = date.fromisoformat(args.date)
        except ValueError:
            logging.error(f"Invalid date format: '{args.date}'. Please use YYYY-MM-DD.")
            return
    else:
        target_date = date.today() - timedelta(days=1)

    download_pageviews_data(target_date)

if __name__ == "__main__":
    main()