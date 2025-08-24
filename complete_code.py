#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Unified Skycoach Scraper and Database Importer - UPDATED (slider vs range)
- Distinguishes single-knob slider vs dual-knob range
- Returns option_type = 'slider' or 'range' (children use '*_value', normalized later)
Reads CSV files, extracts nested product links, scrapes each product, and imports to database
"""

import os
import re
import csv
import time
import math
import hashlib
import pandas as pd
import requests
import mysql.connector
from datetime import datetime
from typing import Dict, Tuple, List
from mysql.connector import errorcode

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.edge.options import Options as EdgeOptions
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC

# ========================= CONFIG =========================
GAMES_DIRECTORY = "gamesTwo"
SERVICES_CSV = "services2.csv"
SERVICE_OPTIONS_CSV = "service_options2.csv"

# Database Configuration
DB_CONFIG = {
    "host": "localhost",
    "port": 3306,
    "user": "root",
    "password": "",
    "database": "boostgg",
}

# Selenium Configuration
SEL_OPTS = EdgeOptions()
SEL_OPTS.page_load_strategy = "eager"
# SEL_OPTS.add_argument("--headless=new")  # Uncomment for headless mode
SEL_OPTS.add_argument("--no-sandbox")
SEL_OPTS.add_argument("--disable-dev-shm-usage")

# Database Behavior
FORCE_GAME_ID = 21
REUSE_EXISTING_SERVICE_BY_NAME = True

# ========================= CSV INITIALIZATION =========================
def reset_csv_files():
    """Reset CSV files to only contain headers"""
    # Reset services.csv
    with open(SERVICES_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([
            "service_id", "game_id", "name", "description",
            "price_per_unit", "sale_price", "icon_url", "category",
            "game_name"
        ])
    
    # Reset service_options.csv
    with open(SERVICE_OPTIONS_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([
            'option_id', 'service_id', 'parent_option_id', 'option_type', 'option_name',
            'option_label', 'option_value', 'price_modifier', 'min_value', 'max_value',
            'default_value', 'is_required', 'display_order', 'is_active', 'created_at', 'updated_at'
        ])
    print("✔ CSV files reset with headers only")

def ensure_services_csv():
    if not os.path.exists(SERVICES_CSV):
        reset_csv_files()

def ensure_options_csv():
    if not os.path.exists(SERVICE_OPTIONS_CSV):
        reset_csv_files()

def get_next_service_id_from_services_csv() -> int:
    ensure_services_csv()
    with open(SERVICES_CSV, "r", encoding="utf-8") as f:
        rdr = csv.DictReader(f)
        mx = 0
        for row in rdr:
            try:
                mx = max(mx, int(row["service_id"]))
            except:
                pass
    return mx + 1

def get_next_option_id_from_options_csv() -> int:
    ensure_options_csv()
    with open(SERVICE_OPTIONS_CSV, "r", encoding="utf-8") as f:
        rdr = csv.DictReader(f)
        mx = 0
        for row in rdr:
            try:
                mx = max(mx, int(row["option_id"]))
            except:
                pass
    return mx + 1

# ========================= LINK EXTRACTION =========================
def extract_nested_links_from_csv_files() -> List[str]:
    """Extract all skycoach.gg product links from CSV files in gamesTwo directory"""
    nested_links = []
    
    if not os.path.exists(GAMES_DIRECTORY):
        print(f"❌ Directory '{GAMES_DIRECTORY}' not found")
        return nested_links
    
    gamesTwo_files = os.listdir(GAMES_DIRECTORY)
    print(f"Found {len(gamesTwo_files)} files in {GAMES_DIRECTORY}")
    
    for file_name in gamesTwo_files:
        if not file_name.endswith(".csv"):
            continue
            
        file_path = os.path.join(GAMES_DIRECTORY, file_name)
        print(f"Processing file: {file_name}")
        
        try:
            df = pd.read_csv(file_path)
            
            if 'Link' not in df.columns:
                print(f"  ⚠ No 'Link' column found in {file_name}")
                continue
                
            # Extract skycoach.gg links
            for index, row in df.iterrows():
                link = row['Link']
                if pd.notna(link) and str(link).startswith("https://skycoach.gg/"):
                    print(f"  Found link: {link}")
                    
                    # Get nested product links from the main category page
                    product_links = extract_product_links_from_page(link)
                    nested_links.extend(product_links)
                    
        except Exception as e:
            print(f"  ❌ Error processing {file_name}: {e}")
    
    print(f"✔ Total nested product links found: {len(nested_links)}")
    return nested_links

def extract_product_links_from_page(category_url: str) -> List[str]:
    """Extract individual product links from a category page"""
    try:
        print(f"  Extracting product links from: {category_url}")
        response = requests.get(category_url, timeout=10)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        product_links = []
        
        link_selectors = [
            'a[href*="/products/"]',
            '.product-card a',
            '.offer-card a',
            'a[href*="/boost/products/"]',
        ]
        
        for selector in link_selectors:
            links = soup.select(selector)
            for link in links:
                href = link.get('href')
                if href:
                    if href.startswith('/'):
                        href = 'https://skycoach.gg' + href
                    if '/products/' in href and href.startswith('https://skycoach.gg/'):
                        product_links.append(href)
        
        unique_links = list(dict.fromkeys(product_links))
        print(f"    Found {len(unique_links)} product links")
        return unique_links
        
    except Exception as e:
        print(f"    ❌ Error extracting links from {category_url}: {e}")
        return []

# ========================= TEXT/PRICE UTILS =========================
def clean_text(s: str) -> str:
    return re.sub(r"\s+", " ", s or "").strip()

def parse_currency_to_decimal(txt: str) -> str:
    if not txt:
        return ""
    t = txt.strip().lower()
    if t in ["free", "basic"]:
        return "0.00"
    t = re.sub(r"[^\d,.\-]", "", t)
    if "," in t and "." in t:
        t = t.replace(",", "")
    elif "," in t and "." not in t:
        t = t.replace(",", ".")
    try:
        return f"{float(t):.2f}"
    except:
        return ""

def parse_price_modifier(price_text):
    if not price_text or price_text.lower() in ["free", "basic"]:
        return 0.00
    num = parse_currency_to_decimal(price_text)
    try:
        return float(num) if num else 0.00
    except:
        return 0.00

# ========================= SELENIUM HELPERS =========================
def is_visible(driver, el):
    if not el:
        return False
    try:
        return driver.execute_script(
            "const e=arguments[0];"
            "if(!e) return false;"
            "const s=window.getComputedStyle(e);"
            "const r=e.getBoundingClientRect();"
            "return s && s.display!=='none' && s.visibility!=='hidden' && r.width>0 && r.height>0;",
            el
        )
    except:
        return False

def get_clean_text_el(el):
    try:
        return clean_text(el.text or "")
    except:
        return ""

# ========================= RANGE/SLIDER DETECTION =========================
def detect_slider_kind(group_el) -> str:
    """
    Return 'slider' for single-knob, 'range' for dual-knob (from-to).
    Uses both knob count and number of numeric inputs to be robust.
    """
    try:
        knob_cnt = len(group_el.find_elements(By.CSS_SELECTOR, ".product-option-cluster-range .range__body .range__knob"))
    except:
        knob_cnt = 0
    try:
        inp_cnt = len(group_el.find_elements(By.CSS_SELECTOR, ".product-option-cluster-range .input-container input[type='number']"))
    except:
        inp_cnt = 0

    if knob_cnt >= 2 or inp_cnt >= 2:
        return "range"
    return "slider"

# ========================= SEMANTIC SIGNATURE =========================
def group_kind_and_signature(driver, group_el) -> Tuple[str, str, str]:
    """Return (kind, label, signature) for semantic comparison"""
    label_el_q = group_el.find_elements(By.CSS_SELECTOR, ".product-option__label")
    label = get_clean_text_el(label_el_q[0]) if label_el_q else "Option"
    slug = label.lower().replace(":", "").strip()

    def h(s):
        return hashlib.sha256((s or "").encode("utf-8","ignore")).hexdigest()

    # slider/range?
    range_q = group_el.find_elements(By.CSS_SELECTOR, ".product-option-cluster-range")
    if range_q:
        if is_visible(driver, group_el):
            kind = detect_slider_kind(group_el)  # 'slider' or 'range'
            scales = []
            try:
                for it in range_q[0].find_elements(By.CSS_SELECTOR, ".range__scale-item"):
                    t = clean_text(it.text)
                    if t:
                        scales.append(t)
            except:
                pass
            defaults = []
            try:
                inps = range_q[0].find_elements(By.CSS_SELECTOR, ".input-container input")
                for ip in inps:
                    v = ip.get_attribute("value") or ""
                    if v:
                        defaults.append(v)
            except:
                pass
            sig = f"{kind}|{slug}|scale:{','.join(scales)}|def:{','.join(defaults)}"
            return (kind, label, h(sig))

    # radios?
    radios_q = group_el.find_elements(By.CSS_SELECTOR, ".product-option-cluster-radios")
    if radios_q:
        items = []
        for ro in radios_q[0].find_elements(By.CSS_SELECTOR, ".radio-option"):
            if not is_visible(driver, ro):
                continue
            inps = ro.find_elements(By.CSS_SELECTOR, "input[type='radio']")
            if not inps:
                continue
            val = inps[0].get_attribute("value") or ""
            lab_q = ro.find_elements(By.CSS_SELECTOR, ".radio-check__label")
            lab_txt = ""
            if lab_q:
                for t in (lab_q[0].text or "").split("\n"):
                    t = t.strip()
                    if t and not t.startswith('+') and t.lower() != 'free':
                        lab_txt = t
                        break
            lab_txt = lab_txt or get_clean_text_el(ro)
            price_q = ro.find_elements(By.CSS_SELECTOR, ".radio-option__price")
            price_txt = clean_text(price_q[0].text) if price_q else ""
            items.append(f"{lab_txt}|{val}|{price_txt}")
        sig = f"radio|{slug}|items:{'||'.join(items)}"
        return ("radio", label, h(sig))

    # buttons?
    buttons_q = group_el.find_elements(By.CSS_SELECTOR, ".product-option-cluster-buttons")
    if buttons_q:
        items = []
        for btn in buttons_q[0].find_elements(By.CSS_SELECTOR, "button"):
            if not is_visible(driver, btn):
                continue
            lab_q = btn.find_elements(By.CSS_SELECTOR, ".button-option__label")
            lab_txt = get_clean_text_el(lab_q[0]) if lab_q else get_clean_text_el(btn)
            items.append(lab_txt)
        sig = f"buttons|{slug}|items:{'||'.join(items)}"
        return ("buttons", label, h(sig))

    # checkboxes?
    checks_q = group_el.find_elements(By.CSS_SELECTOR, ".product-option-cluster-checkboxes")
    if checks_q:
        items = []
        for co in checks_q[0].find_elements(By.CSS_SELECTOR, ".checkbox-option"):
            if not is_visible(driver, co):
                continue
            lab_q = co.find_elements(By.CSS_SELECTOR, ".radio-check__label")
            lab_txt = get_clean_text_el(lab_q[0]) if lab_q else get_clean_text_el(co)
            inps = co.find_elements(By.CSS_SELECTOR, "input[type='checkbox']")
            val = inps[0].get_attribute("value") if inps else ""
            price_q = co.find_elements(By.CSS_SELECTOR, ".checkbox-option__price")
            price_txt = clean_text(price_q[0].text) if price_q else ""
            items.append(f"{lab_txt}|{val}|{price_txt}")
        sig = f"checkbox|{slug}|items:{'||'.join(items)}"
        return ("checkbox", label, h(sig))

    # select?
    select_q = group_el.find_elements(By.CSS_SELECTOR, ".product-option-cluster-select select")
    if select_q:
        try:
            sel = Select(select_q[0])
            items = [clean_text(opt.text) for opt in sel.options]
        except:
            items = []
        sig = f"select|{slug}|items:{'||'.join(items)}"
        return ("select", label, h(sig))

    # unknown
    try:
        html = group_el.get_attribute("outerHTML") or ""
    except:
        html = ""
    return ("unknown", label, h(f"unknown|{slug}|{html[:500]}"))

# ========================= SLIDER/RANGE LABEL EXTRACTION =========================
def extract_slider_labels(group_el):
    """Extract labels for slider/range inputs (e.g., 'Random Rolls' or 'Current Power'/'Needed Power')."""
    labels = []
    try:
        # Prefer per-input labels
        input_containers = group_el.find_elements(By.CSS_SELECTOR, ".input-container")
        for container in input_containers:
            label_elements = container.find_elements(By.CSS_SELECTOR, ".label")
            if label_elements:
                label_text = clean_text(label_elements[0].text)
                if label_text:
                    labels.append(label_text)
        # Fallback to main label if none
        if not labels:
            main_labels = group_el.find_elements(By.CSS_SELECTOR, ".product-option__label")
            if main_labels:
                labels.append(clean_text(main_labels[0].text))
        if not labels:
            labels.append("Value")
    except Exception:
        labels = ["Value"]
    return labels

# ========================= WRITERS (PARENT + CHILDREN) =========================
def write_slider_or_range(driver, group_el, kind: str, label: str, parent_option_id, service_id, option_id_start, display_order, rows_out):
    """
    Write a slider OR range block.
    - kind: 'slider' or 'range'
    - Parent option_type = kind
    - Children option_type = f'{kind}_value' (normalizer will strip '_value')
    """
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    option_id = option_id_start

    # Extract labels
    slider_labels = extract_slider_labels(group_el)
    # Parent naming
    if len(slider_labels) == 1:
        parent_option_name = slider_labels[0].lower().replace(' ', '_').replace(':', '')
        parent_option_label = slider_labels[0]
    elif len(slider_labels) == 2:
        parent_option_name = f"{slider_labels[0].lower().replace(' ', '_').replace(':','')}_{slider_labels[1].lower().replace(' ', '_').replace(':','')}"
        parent_option_label = f"{slider_labels[0]} /// {slider_labels[1]}"
    else:
        parent_option_name = "_".join([l.lower().replace(' ', '_').replace(':', '') for l in slider_labels])
        parent_option_label = " /// ".join(slider_labels)

    # Parent row
    parent_id = option_id
    option_id += 1
    rows_out.append({
        'option_id': parent_id, 'service_id': service_id, 'parent_option_id': parent_option_id,
        'option_type': kind, 'option_name': parent_option_name,
        'option_label': parent_option_label, 'option_value': None, 'price_modifier': 0.00,
        'min_value': None, 'max_value': None, 'default_value': None,
        'is_required': 1, 'display_order': display_order, 'is_active': 1,
        'created_at': now, 'updated_at': now
    })
    display_order += 1

    # Extract min/max from scale
    min_val = max_val = None
    try:
        scales = group_el.find_elements(By.CSS_SELECTOR, ".product-option-cluster-range .range__scale-item")
        if scales:
            try:
                min_val = int(clean_text(scales[0].text))
                max_val = int(clean_text(scales[-1].text))
            except:
                pass
    except:
        pass

    # Extract default values from numeric inputs
    default_values = []
    try:
        inputs = group_el.find_elements(By.CSS_SELECTOR, ".product-option-cluster-range .input-container input")
        for inp in inputs:
            val = inp.get_attribute("value") or ""
            if val:
                default_values.append(val)
    except:
        pass

    # Child value rows
    # For 'slider' → usually 1 label; for 'range' → usually 2 labels (from/to)
    for i, slider_label in enumerate(slider_labels):
        value_option_name = f"{parent_option_name}_value_{i+1}" if len(slider_labels) > 1 else f"{parent_option_name}_value"
        default_val = default_values[i] if i < len(default_values) else ""
        rows_out.append({
            'option_id': option_id, 'service_id': service_id, 'parent_option_id': parent_id,
            'option_type': f'{kind}_value', 'option_name': value_option_name,
            'option_label': slider_label, 'option_value': default_val, 'price_modifier': 0.00,
            'min_value': min_val, 'max_value': max_val, 'default_value': default_val,
            'is_required': 1, 'display_order': display_order, 'is_active': 1,
            'created_at': now, 'updated_at': now
        })
        option_id += 1
        display_order += 1

    return option_id, display_order, parent_id

def write_buttons_as_radio(driver, group_el, label, parent_option_id, service_id, option_id_start, display_order, rows_out):
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    option_id = option_id_start

    parent_id = option_id; option_id += 1
    rows_out.append({
        'option_id': parent_id, 'service_id': service_id, 'parent_option_id': parent_option_id,
        'option_type': 'radio', 'option_name': label.lower().replace(" ", "_").replace(":",""),
        'option_label': label, 'option_value': None, 'price_modifier': 0.00,
        'min_value': None, 'max_value': None, 'default_value': None,
        'is_required': 1, 'display_order': display_order, 'is_active': 1,
        'created_at': now, 'updated_at': now
    })
    display_order += 1

    groups = group_el.find_elements(By.CSS_SELECTOR, ".product-option-cluster-buttons .buttons-group")
    for bg in groups:
        for btn in bg.find_elements(By.CSS_SELECTOR, "button"):
            if not is_visible(driver, btn):
                continue
            lab_q = btn.find_elements(By.CSS_SELECTOR, ".button-option__label")
            txt = get_clean_text_el(lab_q[0]) if lab_q else get_clean_text_el(btn)
            val = txt.lower().replace(" ", "_")
            rows_out.append({
                'option_id': option_id, 'service_id': service_id, 'parent_option_id': parent_id,
                'option_type': 'radio', 'option_name': f"{label.lower().replace(' ','_')}_{val}",
                'option_label': txt, 'option_value': val, 'price_modifier': 0.00,
                'min_value': None, 'max_value': None, 'default_value': None,
                'is_required': 0, 'display_order': display_order, 'is_active': 1,
                'created_at': now, 'updated_at': now
            })
            option_id += 1
            display_order += 1
    return option_id, display_order, parent_id

def write_radios(driver, group_el, label, parent_option_id, service_id, option_id_start, display_order, rows_out):
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    option_id = option_id_start

    parent_id = option_id; option_id += 1
    rows_out.append({
        'option_id': parent_id, 'service_id': service_id, 'parent_option_id': parent_option_id,
        'option_type': 'radio', 'option_name': label.lower().replace(" ", "_").replace(":",""),
        'option_label': label, 'option_value': None, 'price_modifier': 0.00,
        'min_value': None, 'max_value': None, 'default_value': None,
        'is_required': 1, 'display_order': display_order, 'is_active': 1,
        'created_at': now, 'updated_at': now
    })
    display_order += 1

    value_rows = []
    for ro in group_el.find_elements(By.CSS_SELECTOR, ".product-option-cluster-radios .radio-option"):
        if not is_visible(driver, ro):
            continue
        inps = ro.find_elements(By.CSS_SELECTOR, "input[type='radio']")
        if not inps:
            continue
        inp = inps[0]
        val = inp.get_attribute("value") or ""

        lab_q = ro.find_elements(By.CSS_SELECTOR, ".radio-check__label")
        txt = ""
        if lab_q:
            for t in (lab_q[0].text or "").split("\n"):
                t = t.strip()
                if t and not t.startswith('+') and t.lower() != 'free':
                    txt = t
                    break
        txt = txt or get_clean_text_el(ro)

        price_q = ro.find_elements(By.CSS_SELECTOR, ".radio-option__price")
        price_mod = parse_price_modifier(price_q[0].text.strip()) if price_q else 0.00
        is_checked = inp.get_attribute("checked") is not None

        row_id = option_id
        rows_out.append({
            'option_id': row_id, 'service_id': service_id, 'parent_option_id': parent_id,
            'option_type': 'radio', 'option_name': f"{label.lower().replace(' ','_')}_{txt.lower().replace(' ','_')}",
            'option_label': txt, 'option_value': val, 'price_modifier': price_mod,
            'min_value': None, 'max_value': None, 'default_value': (val if is_checked else None),
            'is_required': 0, 'display_order': display_order, 'is_active': 1,
            'created_at': now, 'updated_at': now
        })
        value_rows.append((row_id, val))
        option_id += 1
        display_order += 1

    return option_id, display_order, parent_id, value_rows

def write_checkboxes(driver, group_el, label, parent_option_id, service_id, option_id_start, display_order, rows_out):
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    option_id = option_id_start

    parent_id = option_id; option_id += 1
    rows_out.append({
        'option_id': parent_id, 'service_id': service_id, 'parent_option_id': parent_option_id,
        'option_type': 'checkbox', 'option_name': label.lower().replace(" ", "_").replace(":",""),
        'option_label': label, 'option_value': None, 'price_modifier': 0.00,
        'min_value': None, 'max_value': None, 'default_value': None,
        'is_required': 0, 'display_order': display_order, 'is_active': 1,
        'created_at': now, 'updated_at': now
    })
    display_order += 1

    for co in group_el.find_elements(By.CSS_SELECTOR, ".product-option-cluster-checkboxes .checkbox-option"):
        if not is_visible(driver, co):
            continue
        inps = co.find_elements(By.CSS_SELECTOR, "input[type='checkbox']")
        if not inps:
            continue
        chk = inps[0]
        lab_q = co.find_elements(By.CSS_SELECTOR, ".radio-check__label")
        txt = get_clean_text_el(lab_q[0]) if lab_q else get_clean_text_el(co)
        price_q = co.find_elements(By.CSS_SELECTOR, ".checkbox-option__price")
        price_mod = parse_price_modifier(price_q[0].text.strip()) if price_q else 0.00
        val = chk.get_attribute("value") or txt.lower().replace(" ", "_")
        is_checked = chk.get_attribute("checked") is not None

        rows_out.append({
            'option_id': option_id, 'service_id': service_id, 'parent_option_id': parent_id,
            'option_type': 'checkbox', 'option_name': f"{label.lower().replace(' ','_')}_{val}",
            'option_label': txt, 'option_value': val, 'price_modifier': price_mod,
            'min_value': None, 'max_value': None, 'default_value': (val if is_checked else None),
            'is_required': 0, 'display_order': display_order, 'is_active': 1,
            'created_at': now, 'updated_at': now
        })
        option_id += 1
        display_order += 1

    return option_id, display_order, parent_id

def parse_group_to_rows_fixed(driver, group_el, kind: str, label: str, parent_option_id: int, service_id: int,
                        option_id_start: int, display_order: int, rows_out: List[dict]):
    """Fixed parser that supports slider vs range."""
    if kind in ("slider", "range"):
        return write_slider_or_range(driver, group_el, kind, label, parent_option_id, service_id, option_id_start, display_order, rows_out)
    if kind == "radio":
        oid, disp, parent_id, _vals = write_radios(driver, group_el, label, parent_option_id, service_id, option_id_start, display_order, rows_out)
        return oid, disp, parent_id
    if kind == "buttons":
        return write_buttons_as_radio(driver, group_el, label, parent_option_id, service_id, option_id_start, display_order, rows_out)
    if kind == "checkbox":
        return write_checkboxes(driver, group_el, label, parent_option_id, service_id, option_id_start, display_order, rows_out)
    if kind == "select":
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        option_id = option_id_start
        parent_id = option_id; option_id += 1
        rows_out.append({
            'option_id': parent_id, 'service_id': service_id, 'parent_option_id': parent_option_id,
            'option_type': 'dropdown', 'option_name': label.lower().replace(" ", "_").replace(":",""),
            'option_label': label, 'option_value': None, 'price_modifier': 0.00,
            'min_value': None, 'max_value': None, 'default_value': None,
            'is_required': 1, 'display_order': display_order, 'is_active': 1,
            'created_at': now, 'updated_at': now
        })
        display_order += 1
        try:
            sel = Select(group_el.find_element(By.CSS_SELECTOR, ".product-option-cluster-select select"))
            for opt in sel.options:
                txt = clean_text(opt.text)
                val = opt.get_attribute("value") or txt.lower().replace(" ", "_")
                defv = val if opt.get_attribute("selected") is not None else None
                rows_out.append({
                    'option_id': option_id, 'service_id': service_id, 'parent_option_id': parent_id,
                    'option_type': 'dropdown',
                    'option_name': f"{label.lower().replace(' ','_')}_{val}",
                    'option_label': txt, 'option_value': val, 'price_modifier': 0.00,
                    'min_value': None, 'max_value': None, 'default_value': defv,
                    'is_required': 0, 'display_order': display_order, 'is_active': 1,
                    'created_at': now, 'updated_at': now
                })
                option_id += 1
                display_order += 1
        except:
            pass
        return option_id, display_order, parent_id
    return option_id_start, display_order, None

# ========================= OUTPUT NORMALIZATION =========================
def normalize_option_types_in_rows(rows: List[dict]) -> List[dict]:
    out = []
    for r in rows:
        rr = dict(r)
        t = (rr.get("option_type") or "").strip()
        if t.endswith("_value"):
            t = t[: -len("_value")]
        if t in ("button", "buttons"):
            t = "radio"
        rr["option_type"] = t
        out.append(rr)
    return out

# ========================= MAIN EXTRACTION WITH SNAPSHOTS =========================
def extract_options_with_snapshots_fixed(driver, service_id: int):
    """Options extraction that handles dynamic DOM & slider vs range."""
    option_id = get_next_option_id_from_options_csv()
    rows_out = []
    display_order = 1
    wait = WebDriverWait(driver, 25)

    try:
        container = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, ".product-detail-calculator__options")))
    except:
        print("  ⚠ No options container found on this page")
        return rows_out

    # 1) BASELINE: parse all visible groups once
    baseline_sigs: Dict[Tuple[str,str], str] = {}
    baseline_groups: List[Tuple[object,str,str]] = []

    groups = container.find_elements(By.CSS_SELECTOR, ".option-group")
    for g in groups:
        if not is_visible(driver, g):
            continue
        inner = g.find_elements(By.CSS_SELECTOR, ".product-option")
        if not inner:
            continue
        kind, label, sig = group_kind_and_signature(driver, inner[0])
        baseline_sigs[(kind, label)] = sig
        baseline_groups.append((inner[0], kind, label))

    # Write baseline top-level groups
    for group_el, kind, label in baseline_groups:
        option_id, display_order, _parent_id = parse_group_to_rows_fixed(
            driver, group_el, kind, label, parent_option_id=None, service_id=service_id,
            option_id_start=option_id, display_order=display_order, rows_out=rows_out
        )

    # Locate the Difficulty radio group in baseline (for dynamic changes)
    difficulty_group_el = None
    for group_el, kind, label in baseline_groups:
        if kind == "radio" and label.strip().lower() == "difficulty":
            difficulty_group_el = group_el
            break

    if not difficulty_group_el:
        return rows_out

    # Map radio values to parent row IDs
    radio_value_parent_row_ids: Dict[str, int] = {}
    for r in rows_out:
        if r['option_type'] == 'radio' and r['service_id'] == service_id and str(r.get('option_name','')).startswith('difficulty_'):
            radio_value_parent_row_ids[str(r['option_value'])] = r['option_id']

    # Get radio inputs to click
    radio_inputs = difficulty_group_el.find_elements(By.CSS_SELECTOR, ".product-option-cluster-radios .radio-option input[type='radio']")
    radio_values = []
    for inp in radio_inputs:
        v = inp.get_attribute("value") or ""
        if v:
            radio_values.append(v)

    # Helper to wait for DOM changes
    def wait_dom_change(prev_sig: str, timeout=4.0):
        start = time.time()
        while time.time() - start < timeout:
            try:
                html = driver.find_element(By.CSS_SELECTOR, ".product-detail-calculator__options").get_attribute("innerHTML") or ""
                cur_sig = hashlib.sha256(html.encode("utf-8","ignore")).hexdigest()
                if cur_sig != prev_sig:
                    return cur_sig
            except:
                pass
            time.sleep(0.15)
        return prev_sig

    # 2) SNAPSHOTS: click each radio, capture changed groups only
    for val in radio_values:
        parent_radio_row_id = radio_value_parent_row_ids.get(str(val))
        if not parent_radio_row_id:
            continue

        prev_html = container.get_attribute("innerHTML") or ""
        prev_sig = hashlib.sha256(prev_html.encode("utf-8","ignore")).hexdigest()

        # Click radio
        try:
            target_inp = difficulty_group_el.find_element(By.CSS_SELECTOR, f".product-option-cluster-radios input[type='radio'][value='{val}']")
            driver.execute_script("arguments[0].click();", target_inp)
        except:
            continue

        # Wait for DOM update
        time.sleep(0.25)
        wait_dom_change(prev_sig, timeout=4.0)

        # Re-scan visible groups after click
        cur_groups = container.find_elements(By.CSS_SELECTOR, ".option-group")
        for g in cur_groups:
            if not is_visible(driver, g):
                continue
            inner = g.find_elements(By.CSS_SELECTOR, ".product-option")
            if not inner:
                continue
            kind, label, sig = group_kind_and_signature(driver, inner[0])

            # Skip the Difficulty group itself
            if kind == "radio" and label.strip().lower() == "difficulty":
                continue

            # Only keep groups whose semantic signature differs from baseline
            baseline_sig = baseline_sigs.get((kind, label))
            if baseline_sig and baseline_sig == sig:
                continue

            # Remember new signature
            baseline_sigs[(kind, label)] = sig

            # Parse as child of the selected radio value
            option_id, display_order, _child_parent = parse_group_to_rows_fixed(
                driver, inner[0], kind, label, parent_option_id=parent_radio_row_id,
                service_id=service_id, option_id_start=option_id, display_order=display_order, rows_out=rows_out
            )

    return rows_out

def append_options(rows):
    if not rows:
        return 0
    ensure_options_csv()
    with open(SERVICE_OPTIONS_CSV, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        for o in rows:
            w.writerow([
                o['option_id'], o['service_id'],
                ("" if o['parent_option_id'] is None else o['parent_option_id']),
                o['option_type'], o['option_name'], o['option_label'], o['option_value'],
                f"{o['price_modifier']:.2f}" if o['price_modifier'] is not None else "",
                o['min_value'], o['max_value'], o['default_value'],
                o['is_required'], o['display_order'], o['is_active'],
                o['created_at'], o['updated_at']
            ])
    return len(rows)

# ========================= DATABASE HELPERS =========================
def is_nullish(v):
    return v is None or (isinstance(v, float) and math.isnan(v)) or (isinstance(v, str) and v.strip() == "")

def to_nullable_decimal(s):
    if is_nullish(s):
        return None
    try:
        return float(str(s))
    except Exception:
        return None

def to_nullable_int(v):
    if is_nullish(v):
        return None
    try:
        return int(float(v))
    except Exception:
        return None

def to_nullable_str(v):
    if is_nullish(v):
        return None
    return str(v)

def is_parent_row(orow) -> bool:
    p = orow.get("parent_option_id")
    return (p is None) or (str(p).strip() == "") or (str(p).lower() == "nan")

def clean_option_types(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize option_type for insert (child '*_value' -> base; buttons->radio)."""
    df = df.copy()
    df["option_type"] = df["option_type"].astype(str)
    df["option_type"] = df["option_type"].str.replace(r"_value$", "", regex=True)
    df["option_type"] = df["option_type"].replace({"buttons": "radio", "button": "radio"})
    return df

# Database SQL queries
INSERT_SERVICE_SQL = """
INSERT INTO services (game_id, name, description, price_per_unit, sale_price, icon_url, category)
VALUES (%s, %s, %s, %s, %s, %s, %s)
"""

SELECT_SERVICE_SQL = """
SELECT service_id FROM services WHERE game_id = %s AND name = %s LIMIT 1
"""

INSERT_OPTION_SQL = """
INSERT INTO service_options
(service_id, parent_option_id, option_type, option_name, option_label, option_value,
 price_modifier, min_value, max_value, default_value, is_required, display_order,
 is_active, created_at, updated_at)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

def insert_service(cur, row):
    """Insert service and return DB service_id"""
    game_id = FORCE_GAME_ID
    name = to_nullable_str(row.get("name"))
    description = to_nullable_str(row.get("description"))
    price_per_unit = to_nullable_decimal(row.get("price_per_unit"))
    sale_price = to_nullable_decimal(row.get("sale_price"))
    icon_url = to_nullable_str(row.get("icon_url"))
    category = to_nullable_str(row.get("category"))

    if REUSE_EXISTING_SERVICE_BY_NAME and name:
        cur.execute(SELECT_SERVICE_SQL, (game_id, name))
        existing = cur.fetchone()
        if existing:
            return int(existing[0])

    cur.execute(
        INSERT_SERVICE_SQL,
        (game_id, name, description, price_per_unit, sale_price, icon_url, category)
    )
    return cur.lastrowid

def insert_one_option(cur, db_service_id, orow, parent_db_id=None):
    """Insert one option row and return new DB option_id"""
    option_type = to_nullable_str(orow.get("option_type"))
    option_name = to_nullable_str(orow.get("option_name"))
    option_label = to_nullable_str(orow.get("option_label"))
    option_value = to_nullable_str(orow.get("option_value"))

    price_modifier = to_nullable_decimal(orow.get("price_modifier"))
    min_value = to_nullable_int(orow.get("min_value"))
    max_value = to_nullable_int(orow.get("max_value"))
    default_value = to_nullable_str(orow.get("default_value"))

    is_required = to_nullable_int(orow.get("is_required"))
    display_order = to_nullable_int(orow.get("display_order"))
    is_active = to_nullable_int(orow.get("is_active"))

    created_at = to_nullable_str(orow.get("created_at"))
    updated_at = to_nullable_str(orow.get("updated_at"))

    cur.execute(INSERT_OPTION_SQL, (
        db_service_id, parent_db_id, option_type, option_name, option_label, option_value,
        price_modifier, min_value, max_value, default_value, is_required, display_order,
        is_active, created_at, updated_at
    ))
    return cur.lastrowid

# ========================= MAIN SCRAPING FUNCTION =========================
def scrape_and_save_to_csv_fixed(product_url: str, service_id: int, processed_urls: set) -> dict:
    """Scraping function with slider vs range support and duplicate URL guard."""
    if product_url in processed_urls:
        print(f"  ⚠ Skipping already processed URL: {product_url}")
        return None
    processed_urls.add(product_url)
    
    driver = webdriver.Edge(options=SEL_OPTS)
    try:
        print(f"  Scraping: {product_url}")
        driver.get(product_url)
        time.sleep(2.0)

        # Extract basic service info
        html = driver.page_source
        soup = BeautifulSoup(html, "html.parser")

        # Service name
        name = None
        gh = soup.find("div", class_="game-header")
        if gh and gh.find("h1"):
            name = clean_text(gh.find("h1").get_text())
        if not name and soup.find("h1"):
            name = clean_text(soup.find("h1").get_text())

        if not name:
            print(f"    ⚠ No service name found, skipping: {product_url}")
            return None

        # Description
        desc = None
        ds = soup.find("div", class_="product-info-section__html")
        if ds:
            desc = clean_text(ds.get_text(separator="\n"))

        # Icon URL
        icon_url = None
        og = soup.find("meta", property="og:image")
        if og and og.get("content"):
            icon_url = og["content"].strip()

        # Price
        price_per_unit = ""
        p = soup.find("span", class_="payment-summary__price-column-total")
        if p:
            price_per_unit = parse_currency_to_decimal(p.get_text(strip=True))

        # Determine category from URL
        category = "Unknown"
        if "/destiny-boost/" in product_url:
            category = "Destiny 2"
        elif "/wow-boost/" in product_url:
            category = "World of Warcraft"
        elif "/diablo-4-boost/" in product_url:
            category = "Diablo 4"

        # Create service data
        service_data = {
            "service_id": service_id,
            "game_id": FORCE_GAME_ID,
            "name": name or "",
            "description": desc or "",
            "price_per_unit": price_per_unit,
            "sale_price": "",
            "icon_url": icon_url or "",
            "category": category,
            "game_name": category
        }

        # Write service to CSV
        with open(SERVICES_CSV, "a", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow([
                service_data["service_id"], service_data["game_id"], service_data["name"],
                service_data["description"], service_data["price_per_unit"], service_data["sale_price"],
                service_data["icon_url"], service_data["category"], service_data["game_name"]
            ])

        # Extract options using snapshots with slider/range handling
        rows = extract_options_with_snapshots_fixed(driver, service_id=service_id)
        rows = normalize_option_types_in_rows(rows)
        n = append_options(rows)
        
        print(f"    ✔ Scraped service_id={service_id} name='{name}' options={n}")
        return service_data

    except Exception as e:
        print(f"    ❌ Error scraping {product_url}: {e}")
        return None
    finally:
        driver.quit()

# ========================= DATABASE IMPORT FUNCTION =========================
def import_csv_to_database():
    """Import CSV files to MySQL database"""
    # Read CSVs
    df_services = pd.read_csv(SERVICES_CSV, dtype=str, keep_default_na=False)
    df_options_all = pd.read_csv(SERVICE_OPTIONS_CSV, dtype=str, keep_default_na=False)

    if df_services.empty:
        print("No services to import")
        return

    # Validate columns
    needed_svc_cols = {"name", "description", "price_per_unit", "sale_price", "icon_url", "category", "game_id", "game_name"}
    missing = needed_svc_cols - set(df_services.columns)
    if missing:
        raise ValueError(f"services.csv missing columns: {missing}")

    needed_opt_cols = {
        "option_id","service_id","parent_option_id","option_type","option_name",
        "option_label","option_value","price_modifier","min_value","max_value",
        "default_value","is_required","display_order","is_active","created_at","updated_at"
    }
    miss_opt = needed_opt_cols - set(df_options_all.columns)
    if miss_opt:
        raise ValueError(f"service_options.csv missing columns: {miss_opt}")

    # Normalize option types
    df_options_all = clean_option_types(df_options_all)

    # Connect to database
    cnx = mysql.connector.connect(**DB_CONFIG)
    cnx.autocommit = False
    cur = cnx.cursor()

    imported_services = 0
    imported_options = 0

    try:
        has_csv_service_id = "service_id" in df_services.columns

        for _, srow in df_services.iterrows():
            cnx.start_transaction()
            
            # Insert/reuse service
            db_service_id = insert_service(cur, srow)

            # Get options for this service
            if has_csv_service_id:
                csv_sid = str(srow.get("service_id", "")).strip()
                df_opts_raw = df_options_all[df_options_all["service_id"] == csv_sid].copy()
            else:
                if len(df_services) > 1:
                    raise RuntimeError("Multiple services but no service_id column in services.csv")
                df_opts_raw = df_options_all.copy()

            # Split parents and children
            parents = df_opts_raw[df_opts_raw.apply(is_parent_row, axis=1)].copy()
            children = df_opts_raw[~df_opts_raw.apply(is_parent_row, axis=1)].copy()

            # Sort parents
            parents["display_order_int"] = pd.to_numeric(parents["display_order"], errors="coerce").fillna(0).astype(int)
            parents["option_id_int"] = pd.to_numeric(parents["option_id"], errors="coerce").fillna(0).astype(int)
            parents = parents.sort_values(["display_order_int", "option_id_int"], kind="stable")

            # Sort children
            children["parent_option_id_int"] = pd.to_numeric(children["parent_option_id"], errors="coerce").fillna(0).astype(int)
            children["display_order_int"] = pd.to_numeric(children["display_order"], errors="coerce").fillna(0).astype(int)
            children["option_id_int"] = pd.to_numeric(children["option_id"], errors="coerce").fillna(0).astype(int)
            children = children.sort_values(["parent_option_id_int", "display_order_int", "option_id_int"], kind="stable")

            # Insert parents and track ID mapping
            id_map = {}
            for _, prow in parents.iterrows():
                new_id = insert_one_option(cur, db_service_id, prow, parent_db_id=None)
                id_map[str(prow["option_id"])] = new_id
                imported_options += 1

            # Insert children with mapped parent IDs
            for _, crow in children.iterrows():
                csv_parent = str(crow["parent_option_id"]).strip()
                parent_db_id = id_map.get(csv_parent)
                if not parent_db_id:
                    print(f"[WARN] Missing parent for option_id={crow.get('option_id')} (parent={csv_parent})")
                    parent_db_id = None
                new_id = insert_one_option(cur, db_service_id, crow, parent_db_id=parent_db_id)
                id_map[str(crow["option_id"])] = new_id
                imported_options += 1

            cnx.commit()
            imported_services += 1
            print(f"[DB] Imported service '{srow.get('name')}' (DB id {db_service_id}) with {len(df_opts_raw)} options")

    except Exception as e:
        cnx.rollback()
        print(f"[ROLLBACK] Error: {e}")
        raise
    finally:
        cur.close()
        cnx.close()

    print(f"\n✅ Database import complete. Services: {imported_services}, Options: {imported_options}")

# ========================= MAIN UNIFIED FUNCTION =========================
def main():
    """Main function that resets CSVs, scrapes, and imports to DB."""
    print("🚀 Starting unified Skycoach scraper and database importer...")
    
    # Step 1: Reset CSV files
    print("\nStep 1: Resetting CSV files...")
    reset_csv_files()
    
    # Step 2: Extract nested product links from CSV files
    print("\nStep 2: Extracting product links from CSV files...")
    product_links = extract_nested_links_from_csv_files()
    
    if not product_links:
        print("❌ No product links found. Exiting...")
        return
    
    # Remove duplicates while preserving order
    unique_product_links = list(dict.fromkeys(product_links))
    print(f"Found {len(product_links)} total links, {len(unique_product_links)} unique links to scrape")
    
    # Step 3: Scrape each product and save to CSV
    print(f"\nStep 3: Scraping {len(unique_product_links)} products...")
    service_id = 1  # Start from 1 since we reset the CSV
    successful_scrapes = 0
    processed_urls = set()  # Track processed URLs
    
    for i, product_url in enumerate(unique_product_links, 1):
        print(f"[{i}/{len(unique_product_links)}] Processing: {product_url}")
        
        try:
            service_data = scrape_and_save_to_csv_fixed(product_url, service_id, processed_urls)
            if service_data:
                successful_scrapes += 1
                service_id += 1
                print(f"    ✔ Successfully processed service #{successful_scrapes}")
            time.sleep(1)
        except Exception as e:
            print(f"    ❌ Failed to scrape {product_url}: {e}")
            continue
    
    print(f"\n✔ Scraping complete: {successful_scrapes}/{len(unique_product_links)} products scraped successfully")
    
    if successful_scrapes == 0:
        print("❌ No products were successfully scraped. Exiting...")
        return
    
    # Step 4: Import CSV data to database
    print("\nStep 4: Importing scraped data to database...")
    try:
        import_csv_to_database()
        print(f"\n🎉 SUCCESS: Processed {successful_scrapes} products and imported to database!")
        
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("❌ MySQL auth error: check user/password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("❌ Database does not exist")
        else:
            print(f"❌ MySQL error: {err}")
    except Exception as e:
        print(f"❌ Fatal error during database import: {e}")
        raise

if __name__ == "__main__":
    main()
