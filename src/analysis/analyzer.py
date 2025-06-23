# src/analysis/analyzer.py
import json
import logging
from collections import Counter

# --- Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# This should point to the output file from your consumer
# Note: It assumes you run this script from the project's root directory.
INPUT_FILE = 'edit_events.jsonl'
TOP_N = 10


# --- End Configuration ---

def analyze_trends():
    """
    Reads the collected event data from the NDJSON file and performs
    a simple trend analysis to find the most frequently edited pages.
    """
    logger.info(f"Starting analysis on {INPUT_FILE}...")
    page_edit_counter = Counter()
    total_events = 0

    try:
        with open(INPUT_FILE, 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    # Each line is a separate JSON object
                    event = json.loads(line)

                    # We are interested in the 'title' of the page edited
                    if 'title' in event:
                        page_edit_counter[event['title']] += 1

                    total_events += 1
                except json.JSONDecodeError:
                    logger.warning(f"Skipping malformed line: {line.strip()}")
                    continue

        logger.info(f"Analysis complete. Processed {total_events} events.")

        # --- Print the Report ---
        print("\n" + "=" * 50)
        print("    Wikimedia Edit Trends Report")
        print("=" * 50)
        print(f"\nTop {TOP_N} Most Edited Pages:")
        print("-" * 40)

        # The .most_common(N) method is a convenient feature of collections.Counter
        for i, (page, count) in enumerate(page_edit_counter.most_common(TOP_N), 1):
            print(f"{i:2}. {page:<60} | Edits: {count}")

        print("-" * 40)

    except FileNotFoundError:
        logger.error(f"Error: The input file '{INPUT_FILE}' was not found.")
        logger.error("Please run the consumer first to generate the data file.")
    except Exception as e:
        logger.error(f"An unexpected error occurred during analysis: {e}", exc_info=True)


if __name__ == "__main__":
    analyze_trends()