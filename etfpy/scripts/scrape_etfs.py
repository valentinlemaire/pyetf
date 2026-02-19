#!/bin/bash

import json
import os
from pathlib import Path
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

from etfpy.client.etf_client import ETFDBClient
from etfpy.client._etfs_scraper import get_all_etfs
from etfpy.log import get_logger


ETFS_DATA_PATH = os.path.join(Path(__file__).parent.parent, "data", "etfs")
DEFAULT_FILE_NAME = "etfs_list.json"

logger = get_logger(__name__)


def all_etfs_json(file_path: str = None) -> None:
    """Scrape all ETFs data from etfdb.com and save it to a json file to a location specified by file_path.

    Args:
        file_path (str, optional): Path to save the json file.
        If None, the json file will be saved to the project root directory.
    """
    # If file_path is None, set display_path to "project root folder"
    display_path = file_path

    if file_path is None:
        # Get the project root directory
        root_dir = Path(__file__).resolve().parents[1]
        file_path = os.path.join(root_dir, "etfs_list.json")
        display_path = "project root folder"

    page_size = 250
    logger.info("Scraping all ETFs data from etfdb.com")

    etfs = get_all_etfs(page_size)
    progress_lock = Lock()
    completed = 0

    def _fetch_description(etf: dict) -> None:
        nonlocal completed
        symbol = etf.get("symbol")
        if not symbol:
            etf["description"] = ""
        else:
            try:
                etf["description"] = ETFDBClient(symbol)._description()
            except Exception:
                etf["description"] = ""
        with progress_lock:
            completed += 1
            if completed % 50 == 0:
                logger.info("retrieved descriptions for %s ETFs", completed)

    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = [executor.submit(_fetch_description, etf) for etf in etfs]
        for future in as_completed(futures):
            future.result()

    with open(file_path, "w") as f:
        json.dump(etfs, f)
    logger.debug("ETFs data saved to %s", display_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--file-path",
        "-fp",
        dest="file_path",
        type=str,
        required=False,
        help="path to output json file",
    )
    parser.add_argument(
        "-u",
        "--update",
        action="store_true",
        default=False,
        required=False,
        help="update json file",
        dest="update",
    )
    args = parser.parse_args()
    fp = ETFS_DATA_PATH if args.update is True else args.file_path
    if fp is not None:
        if not fp.endswith(".json"):
            fp = os.path.join(fp, DEFAULT_FILE_NAME)
    logger.info("application args: %s", args)
    all_etfs_json(file_path=fp)
