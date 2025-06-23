# src/utils/kafka_writer.py
import csv
import json
import logging
import os
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)

class BaseWriter(ABC):
    """Abstract base class for all data writers."""
    @abstractmethod
    def save(self, event_data: dict):
        """Saves a single event."""
        pass

    @abstractmethod
    def close(self):
        """Perform any cleanup, like closing file handles."""
        pass

class JSONWriter(BaseWriter):
    """Writes events to a newline-delimited JSON (NDJSON) file."""
    def __init__(self, file_path: str):
        self.file_path = file_path
        # Open the file in append mode and keep it open
        self._file = open(self.file_path, 'a', encoding='utf-8')
        logger.info(f"JSONWriter initialized. Writing to {self.file_path}")

    def save(self, event_data: dict):
        try:
            # json.dumps converts the dict to a JSON string
            json_record = json.dumps(event_data)
            # Write the string followed by a newline character
            self._file.write(json_record + '\n')
        except Exception as e:
            logger.error(f"Error writing JSON record: {e}")

    def close(self):
        if self._file:
            self._file.close()
            logger.info(f"Closed JSON file: {self.file_path}")

class CSVWriter(BaseWriter):
    """Writes events to a CSV file."""
    def __init__(self, file_path: str, header: list):
        self.file_path = file_path
        self.header = header
        self._setup_file()
        # Open the file in append mode and keep it open
        self._file = open(self.file_path, 'a', newline='', encoding='utf-8')
        self._writer = csv.writer(self._file)
        logger.info(f"CSVWriter initialized. Writing to {self.file_path}")

    def _setup_file(self):
        """Creates the CSV file and writes the header if it doesn't exist."""
        if not os.path.exists(self.file_path):
            with open(self.file_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(self.header)
            logger.info(f"Created and initialized {self.file_path}")

    def save(self, event_data: dict):
        try:
            # Extract data in the order of the header
            # This is a bit naive for nested data like 'meta.dt'
            # A more robust version would handle nested keys.
            row = [event_data.get(h) for h in self.header]
            self._writer.writerow(row)
        except Exception as e:
            logger.error(f"Error writing CSV row: {e}")

    def close(self):
        if self._file:
            self._file.close()
            logger.info(f"Closed CSV file: {self.file_path}")