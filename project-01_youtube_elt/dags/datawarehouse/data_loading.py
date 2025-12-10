import json
from datetime import date
import logging

logger = logging.getLogger(__name__)


def load_data():
    file_path = f"./data/YT_data_{date.today()}.json"

    try:
        logger.info(f"Processing file: YT_data_{date.today()}")

        with open(file_path, "r", encoding="utf-8") as raw_data:

            for line in raw_data:
                try:
                    # Each line is loaded into memory one at a time
                    record = json.load(line)
                    # 'yield' returns one record and then pauses, waiting for the next call.
                    # It doesn't store all records in memory.
                    yield record
                except json.JSONDecodeError:
                    logger.warning(f"Skipping malformed JSON line: {line.strip()}")
                    continue  # Go to the next line if this one is bad

    except FileNotFoundError:
        logger.error(f"File not found: {file_path}")
        raise
    except json.JSONDecodeError:
        logger.error(f"Invalid JSON in file: {file_path}")
        raise
