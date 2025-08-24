#!/usr/bin/env python3
"""
run_scrape_and_import.py

Usage: python3 run_scrape_and_import.py

This script coordinates scraping product option groups (using `option_scraper.py`),
appends a minimal service row to `services.csv` for each scraped URL, and then
invokes `scrapper_db.main()` to import both CSVs into MySQL.

Notes:
- It does not change `option_scraper.py` or `scrapper_db.py`.
- Service rows are minimal (many fields empty) but include the required columns.
"""

import os
import csv
from typing import List
from urllib.parse import urlparse

import option_scraper
import scrapper_db

SERVICES_CSV = scrapper_db.SERVICES_CSV
SERVICE_OPTIONS_CSV = scrapper_db.SERVICE_OPTIONS_CSV


def ensure_services_csv(filename: str = SERVICES_CSV):
    """Create `services.csv` with the expected header if it doesn't exist."""
    header = [
        "service_id",
        "game_id",
        "name",
        "description",
        "price_per_unit",
        "sale_price",
        "icon_url",
        "category",
        "game_name",
    ]
    if not os.path.exists(filename):
        with open(filename, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(header)
        print(f"Created {filename} with header")


def get_next_service_id_from_services_csv(filename: str = SERVICES_CSV) -> int:
    """Return next integer service_id (max + 1) based on existing `services.csv` rows."""
    if not os.path.exists(filename):
        return 1
    max_id = 0
    try:
        with open(filename, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    sid = int(row.get("service_id") or 0)
                    if sid > max_id:
                        max_id = sid
                except Exception:
                    continue
    except Exception:
        return 1
    return max_id + 1


def append_service_row(service_id: int, name: str, description: str = "", price_per_unit: str = "",
                       sale_price: str = "", icon_url: str = "", category: str = "", game_id: str = "", game_name: str = "",
                       filename: str = SERVICES_CSV) -> None:
    """Append a minimal service row to `services.csv`.

    All columns expected by `scrapper_db.main()` will be present (may be empty strings).
    """
    ensure_services_csv(filename)
    row = {
        "service_id": service_id,
        "game_id": game_id,
        "name": name,
        "description": description,
        "price_per_unit": price_per_unit,
        "sale_price": sale_price,
        "icon_url": icon_url,
        "category": category,
        "game_name": game_name,
    }
    with open(filename, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(row.keys()))
        writer.writerow(row)
    print(f"Appended service row: id={service_id} name='{name}' to {filename}")


def pretty_name_from_url(url: str) -> str:
    """Make a readable service name from the URL path."""
    parsed = urlparse(url)
    path = parsed.path.strip("/")
    if not path:
        return url
    last = path.split("/")[-1]
    # replace dashes/underscores and title-case
    return last.replace("-", " ").replace("_", " ").title()


def run(urls: List[str], import_to_db: bool = True, pause_between: float = 0.8):
    ensure_services_csv(SERVICES_CSV)
    results = []

    for url in urls:
        # Determine next service_id from services.csv and pass to scraper so both CSVs align
        sid = get_next_service_id_from_services_csv(SERVICES_CSV)
        print(f"\n-> Scraping {url} as service_id={sid}")

        # Scrape and write options; pass the explicit service_id so option rows use same id
        options = option_scraper.scrape_service_options(url, service_id=sid, append_to_csv=True)

        # Create a minimal service row in services.csv so `scrapper_db` can import it.
        svc_name = pretty_name_from_url(url)
        append_service_row(service_id=sid, name=svc_name)

        results.append((sid, len(options) if options else 0, url))

    print("\nScrape summary:")
    for sid, count, url in results:
        print(f" service_id={sid} options={count} url={url}")

    if import_to_db:
        print("\n-> Importing CSVs into DB via scrapper_db.main()")
        # scrapper_db.main will read SERVICES_CSV and SERVICE_OPTIONS_CSV and insert into DB
        scrapper_db.main()


if __name__ == "__main__":
    # Example run (replace/add URLs as needed)
    example_urls = [
        "/destiny-boost/products/intercalary-8615",
        "/destiny-boost/products/the-when-and-where-8617",
    ]
    run(example_urls, import_to_db=False)
