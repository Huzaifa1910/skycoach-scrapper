# --- Imports & setup ---
import os
import re
import csv
import time
import threading
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.edge.options import Options as EdgeOptions

BASE_URL = "https://skycoach.gg"
INPUT_DIR = "gamesTwo"                  # where your *_services.csv live (e.g., Destiny_2_services.csv)
SERVICES_CSV = "services.csv"           # output for services schema
SERVICE_OPTIONS_CSV = "service_options.csv"  # output for options schema

# Selenium options
SEL_OPTS = EdgeOptions()
SEL_OPTS.page_load_strategy = "eager"
# SEL_OPTS.add_argument("--headless=new")
SEL_OPTS.add_argument("--no-sandbox")
SEL_OPTS.add_argument("--disable-dev-shm-usage")

# Thread safety for file writes and ID allocation
services_lock = threading.Lock()
options_lock = threading.Lock()
id_lock = threading.Lock()

# --- Helpers ---
def normalize_url(href: str) -> str:
    if not href:
        return href
    href = href.strip()
    if href.startswith("/"):
        return BASE_URL + href
    return href

def clean_text(s: str) -> str:
    return re.sub(r"\s+", " ", s or "").strip()

def parse_currency_to_decimal(txt: str) -> str:
    """
    Return a string like '1234.56' suitable for CSV/DB decimal fields.
    Handles symbols, thousand separators and commas as decimals.
    Returns '' if not parseable.
    """
    if not txt:
        return ""
    t = txt.strip().lower()
    if t in ["free", "basic"]:
        return "0.00"
    # keep digits, commas, dots, minus
    t = re.sub(r"[^\d,.\-]", "", t)
    # if there are both comma and dot, assume comma is thousands -> remove commas
    if "," in t and "." in t:
        t = t.replace(",", "")
    else:
        # if only comma, treat as decimal separator
        if "," in t and "." not in t:
            t = t.replace(",", ".")
    try:
        return f"{float(t):.2f}"
    except Exception:
        return ""

def ensure_services_csv():
    if not os.path.exists(SERVICES_CSV):
        with open(SERVICES_CSV, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            # schema columns + extra game_name for mapping later
            w.writerow([
                "service_id", "game_id", "name", "description",
                "price_per_unit", "sale_price", "icon_url", "category",
                "game_name"  # extra helper column, keep or drop during import
            ])

def ensure_options_csv():
    if not os.path.exists(SERVICE_OPTIONS_CSV):
        with open(SERVICE_OPTIONS_CSV, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow([
                'option_id', 'service_id', 'parent_option_id', 'option_type', 'option_name',
                'option_label', 'option_value', 'price_modifier', 'min_value', 'max_value',
                'default_value', 'is_required', 'display_order', 'is_active', 'created_at', 'updated_at'
            ])

def get_next_service_id_from_services_csv() -> int:
    ensure_services_csv()
    with services_lock, open(SERVICES_CSV, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        max_id = 0
        for row in reader:
            try:
                sid = int(row["service_id"])
                if sid > max_id:
                    max_id = sid
            except Exception:
                continue
    return max_id + 1

def get_next_option_id_from_options_csv() -> int:
    ensure_options_csv()
    with options_lock, open(SERVICE_OPTIONS_CSV, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        max_id = 0
        for row in reader:
            try:
                oid = int(row["option_id"])
                if oid > max_id:
                    max_id = oid
            except Exception:
                continue
    return max_id + 1

# --- Option extraction (from your 2nd cell, adapted to be pure-HTML based) ---
def parse_price_modifier(price_text):
    if not price_text or price_text.lower() in ['free', 'basic']:
        return 0.00
    num = parse_currency_to_decimal(price_text)
    try:
        return float(num) if num else 0.00
    except Exception:
        return 0.00

def extract_service_options_from_html(html_content, fixed_service_id=None, start_option_id=None):
    soup = BeautifulSoup(html_content, 'html.parser')
    options = []

    service_id = fixed_service_id if fixed_service_id is not None else 0  # must be provided to keep referential integrity

    option_id_counter = start_option_id if start_option_id is not None else get_next_option_id_from_options_csv()
    display_order = 1

    options_container = soup.find('div', class_='product-detail-calculator__options')
    if not options_container:
        return options

    option_groups = options_container.find_all('div', class_='option-group')

    for group in option_groups:
        product_option = group.find('div', class_='product-option')
        if not product_option:
            continue

        # Get option label
        option_head = product_option.find('div', class_='product-option__head')
        option_label = ""
        if option_head:
            label_div = option_head.find('div', class_='product-option__label')
            if label_div:
                option_label = clean_text(label_div.get_text()).replace(':', '')

        # Slider / Range
        range_cluster = product_option.find('div', class_='product-option-cluster-range')
        if range_cluster:
            input_containers = range_cluster.find_all('div', class_='input-container')
            min_val, max_val = None, None
            range_container = range_cluster.find('div', class_='range-container')
            if range_container:
                scale_items = range_container.find_all('div', class_='range__scale-item')
                if scale_items:
                    try:
                        min_val = int(clean_text(scale_items[0].get_text()))
                        max_val = int(clean_text(scale_items[-1].get_text()))
                    except Exception:
                        pass

            for container in input_containers:
                label_div = container.find('div', class_='label')
                input_tag = container.find('input')

                if label_div and input_tag:
                    input_label = clean_text(label_div.get_text())
                    default_val = input_tag.get('value', '')

                    options.append({
                        'option_id': option_id_counter,
                        'service_id': service_id,
                        'parent_option_id': None,
                        'option_type': 'slider',
                        'option_name': f"{option_label.lower().replace(' ', '_')}_{input_label.lower().replace(' ', '_')}",
                        'option_label': input_label,
                        'option_value': default_val,
                        'price_modifier': 0.00,
                        'min_value': min_val,
                        'max_value': max_val,
                        'default_value': default_val,
                        'is_required': 1,
                        'display_order': display_order,
                        'is_active': 1,
                        'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        'updated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    })
                    option_id_counter += 1
                    display_order += 1

        # Dropdown
        select_cluster = product_option.find('div', class_='product-option-cluster-select')
        if select_cluster:
            select_tag = select_cluster.find('select')
            if select_tag:
                parent_id = option_id_counter
                options.append({
                    'option_id': parent_id,
                    'service_id': service_id,
                    'parent_option_id': None,
                    'option_type': 'dropdown',
                    'option_name': option_label.lower().replace(' ', '_').replace(':', ''),
                    'option_label': option_label,
                    'option_value': None,
                    'price_modifier': 0.00,
                    'min_value': None,
                    'max_value': None,
                    'default_value': None,
                    'is_required': 1,
                    'display_order': display_order,
                    'is_active': 1,
                    'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'updated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                })
                option_id_counter += 1
                display_order += 1

                for opt in select_tag.find_all('option'):
                    option_text = clean_text(opt.get_text())
                    option_val = opt.get('value', '')
                    options.append({
                        'option_id': option_id_counter,
                        'service_id': service_id,
                        'parent_option_id': parent_id,
                        'option_type': 'dropdown',
                        'option_name': f"{option_label.lower().replace(' ', '_').replace(':', '')}_{option_text.lower().replace(' ', '_')}",
                        'option_label': option_text,
                        'option_value': option_val,
                        'price_modifier': 0.00,
                        'min_value': None,
                        'max_value': None,
                        'default_value': option_val if opt.get('selected') else None,
                        'is_required': 0,
                        'display_order': display_order,
                        'is_active': 1,
                        'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        'updated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    })
                    option_id_counter += 1
                    display_order += 1

        # Radios
        radio_cluster = product_option.find('div', class_='product-option-cluster-radios')
        if radio_cluster:
            parent_id = option_id_counter
            options.append({
                'option_id': parent_id,
                'service_id': service_id,
                'parent_option_id': None,
                'option_type': 'radio',
                'option_name': option_label.lower().replace(' ', '_').replace(':', ''),
                'option_label': option_label,
                'option_value': None,
                'price_modifier': 0.00,
                'min_value': None,
                'max_value': None,
                'default_value': None,
                'is_required': 1,
                'display_order': display_order,
                'is_active': 1,
                'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'updated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            })
            option_id_counter += 1
            display_order += 1

            radio_options = radio_cluster.find_all('div', class_='radio-option')
            for ro in radio_options:
                input_tag = ro.find('input', {'type': 'radio'})
                label_span = ro.find('span', class_='radio-check__label')
                price_div = ro.find('div', class_='radio-option__price')

                if input_tag and label_span:
                    # conservative label extraction
                    label_text = ""
                    for t in label_span.stripped_strings:
                        t = t.strip()
                        if t and not t.startswith('+') and t.lower() != 'free':
                            label_text = t
                            break
                    label_text = label_text or clean_text(label_span.get_text()).split('\n')[0].strip()
                    option_val = input_tag.get('value', '')
                    is_checked = input_tag.get('checked') is not None
                    price_modifier = parse_price_modifier(price_div.get_text(strip=True)) if price_div else 0.00

                    options.append({
                        'option_id': option_id_counter,
                        'service_id': service_id,
                        'parent_option_id': parent_id,
                        'option_type': 'radio',
                        'option_name': f"{option_label.lower().replace(' ', '_').replace(':', '')}_{label_text.lower().replace(' ', '_')}",
                        'option_label': label_text,
                        'option_value': option_val,
                        'price_modifier': price_modifier,
                        'min_value': None,
                        'max_value': None,
                        'default_value': option_val if is_checked else None,
                        'is_required': 0,
                        'display_order': display_order,
                        'is_active': 1,
                        'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        'updated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    })
                    option_id_counter += 1
                    display_order += 1

        # Checkboxes
        checkbox_cluster = product_option.find('div', class_='product-option-cluster-checkboxes')
        if checkbox_cluster:
            parent_id = option_id_counter
            options.append({
                'option_id': parent_id,
                'service_id': service_id,
                'parent_option_id': None,
                'option_type': 'checkbox',
                'option_name': option_label.lower().replace(' ', '_').replace(':', ''),
                'option_label': option_label,
                'option_value': None,
                'price_modifier': 0.00,
                'min_value': None,
                'max_value': None,
                'default_value': None,
                'is_required': 0,
                'display_order': display_order,
                'is_active': 1,
                'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'updated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            })
            option_id_counter += 1
            display_order += 1

            for co in checkbox_cluster.find_all('div', class_='checkbox-option'):
                input_tag = co.find('input', {'type': 'checkbox'})
                label_span = co.find('span', class_='radio-check__label')
                price_div = co.find('div', class_='checkbox-option__price')
                if input_tag and label_span:
                    option_text = clean_text(label_span.get_text())
                    option_val = input_tag.get('value', '')
                    is_checked = input_tag.get('checked') is not None
                    price_modifier = parse_price_modifier(price_div.get_text(strip=True)) if price_div else 0.00
                    options.append({
                        'option_id': option_id_counter,
                        'service_id': service_id,
                        'parent_option_id': parent_id,
                        'option_type': 'checkbox',
                        'option_name': f"{option_label.lower().replace(' ', '_').replace(':', '')}_{option_text.lower().replace(' ', '_').replace('%','percent')}",
                        'option_label': option_text,
                        'option_value': option_val,
                        'price_modifier': price_modifier,
                        'min_value': None,
                        'max_value': None,
                        'default_value': option_val if is_checked else None,
                        'is_required': 0,
                        'display_order': display_order,
                        'is_active': 1,
                        'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        'updated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    })
                    option_id_counter += 1
                    display_order += 1

    return options

def append_options(options):
    if not options:
        return 0
    ensure_options_csv()
    with options_lock, open(SERVICE_OPTIONS_CSV, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        for o in options:
            w.writerow([
                o['option_id'], o['service_id'], o['parent_option_id'], o['option_type'], o['option_name'],
                o['option_label'], o['option_value'], f"{o['price_modifier']:.2f}", 
                o['min_value'], o['max_value'], o['default_value'], o['is_required'],
                o['display_order'], o['is_active'], o['created_at'], o['updated_at']
            ])
    return len(options)

# --- Service page extraction (from your 1st cell, merged & improved) ---
def extract_service_info_and_options(nested_url: str, category: str, game_name: str):
    """
    Scrape a single service page:
      - allocate a unique service_id
      - extract service fields
      - extract options linked to the same service_id
      - write service row and options rows
    """
    driver = webdriver.Edge(options=SEL_OPTS)
    try:
        url = normalize_url(nested_url)
        driver.get(url)
        time.sleep(2.0)
        html = driver.page_source
        soup = BeautifulSoup(html, "html.parser")

        # Allocate service_id once for this page
        with id_lock:
            service_id = get_next_service_id_from_services_csv()

        # Name (from header)
        name = None
        name_tag = soup.find("div", class_="game-header")
        if name_tag:
            h1 = name_tag.find("h1")
            if h1:
                name = clean_text(h1.get_text())
        if not name:
            # fallback: page title h1
            h1 = soup.find("h1")
            if h1:
                name = clean_text(h1.get_text())

        # Description
        desc = None
        desc_section = soup.find("div", class_="product-info-section__html")
        if desc_section:
            desc = clean_text(desc_section.get_text(separator="\n"))

        # Icon / image (prefer og:image)
        icon_url = None
        og_img = soup.find("meta", property="og:image")
        if og_img and og_img.get("content"):
            icon_url = normalize_url(og_img["content"])
        if not icon_url:
            image_container = soup.find("div", class_="offer-card__image-container")
            if image_container:
                picture_tag = image_container.find("picture", class_="responsive-image offer-card__image")
                if picture_tag:
                    src = None
                    source_tag = picture_tag.find("source")
                    img_tag = picture_tag.find("img")
                    if source_tag and source_tag.has_attr("srcset"):
                        src = source_tag["srcset"].split()[0]
                    elif img_tag and img_tag.has_attr("src"):
                        src = img_tag["src"]
                    icon_url = normalize_url(src) if src else None

        # Price (displayed total)
        price_per_unit = ""
        sale_price = ""
        price_span = soup.find("span", class_="payment-summary__price-column-total")
        if price_span:
            price_per_unit = parse_currency_to_decimal(price_span.get_text(strip=True))

        # Write service row
        ensure_services_csv()
        with services_lock, open(SERVICES_CSV, "a", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            # game_id left blank; game_name included for later mapping
            w.writerow([
                service_id, "", name or "", desc or "", price_per_unit, sale_price,
                icon_url or "", category or "", game_name
            ])

        # Extract and append options (share same service_id)
        options = extract_service_options_from_html(html, fixed_service_id=service_id)
        appended = append_options(options)

        return {
            "service_id": service_id,
            "name": name,
            "options_count": appended,
            "url": url
        }

    except Exception as e:
        return {"error": str(e), "url": nested_url}
    finally:
        driver.quit()

# --- Crawl each game CSV, find nested service links, scrape them ---
def get_nested_links_from_listing(listing_url: str):
    driver = webdriver.Edge(options=SEL_OPTS)
    try:
        url = normalize_url(listing_url)
        driver.get(url)
        time.sleep(1.5)
        html = driver.page_source
        soup = BeautifulSoup(html, "html.parser")
        container = soup.find("div", class_="card-list game-tag-page__container game-tag-page__products-list")
        nested = []
        if container:
            for li in container.find_all("li"):
                a = li.find("a", href=True)
                if a:
                    nested.append(normalize_url(a["href"]))
        return nested
    except Exception:
        return []
    finally:
        driver.quit()

def derive_game_name_from_filename(file_name: str) -> str:
    # e.g., "Destiny_2_services.csv" -> "Destiny 2"
    base = file_name.replace("_services.csv", "").replace(".csv", "")
    return base.replace("_", " ").strip()

def process_game_file(file_name: str, max_workers: int = 4):
    path = os.path.join(INPUT_DIR, file_name)
    df = pd.read_csv(path)
    category = derive_game_name_from_filename(file_name)  # you can change this if category differs from game
    game_name = category  # using same; you can change if needed

    results = []
    futures = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for _, row in df.iterrows():
            link = str(row.get("Link", "")).strip()
            if not link or not link.startswith(BASE_URL):
                continue
            nested_links = get_nested_links_from_listing(link)
            for nurl in nested_links:
                futures.append(executor.submit(extract_service_info_and_options, nurl, category, game_name))

        for fut in as_completed(futures):
            results.append(fut.result())
    return results

def main():
    ensure_services_csv()
    ensure_options_csv()

    gamesTwo_files = set(os.listdir(INPUT_DIR))
    # skip if an output for this game already done? (original code had per-file outputs)
    # Here we always append to global CSVs; dedup is out-of-scope by design.

    for file_name in sorted(gamesTwo_files):
        if not file_name.endswith(".csv"):
            continue
        print(f"\n=== Processing game file: {file_name} ===")
        res = process_game_file(file_name, max_workers=4)
        ok = sum(1 for r in res if r and not r.get("error"))
        print(f"Done {file_name}: {ok} services scraped ({len(res)} attempts).")

if __name__ == "__main__":
    main()
