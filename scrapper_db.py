#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
SkyCoach – Snapshot scraper (multi-file, multi-URL)

- Walks INPUT_DIR for listing CSVs (with a 'Link' column).
- From each listing page, finds nested product URLs.
- For each product URL, scrapes using the "snapshot" logic:
    * Baseline parse of visible groups
    * For each Difficulty radio: click -> re-snapshot
      -> parse ONLY groups whose semantic signature changed
    * Changed groups are added as children of the clicked radio value
- Distinguishes one-way SLIDER vs two-way RANGE dynamically:
    * slider: 1 knob / 1 input
    * range : 2+ knobs / 2 inputs
- Slider/range labels are taken from DOM (e.g., "Random Rolls", "Current Power"/"Needed Power")
- Enforces:
    1) Strip "_value" from option_type (e.g., radio_value -> radio)
    2) Buttons treated as radio (parent + children)
- Outputs:
    services.csv, service_options.csv
"""

import os
import re
import csv
import time
import hashlib
from datetime import datetime
from typing import Dict, Tuple, List

import pandas as pd
from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.webdriver.edge.options import Options as EdgeOptions
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC

# ========================= Config =========================
BASE_URL = "https://skycoach.gg"
INPUT_DIR = "gamesTwo"                  # folder containing listing CSVs (with 'Link')
SERVICES_CSV = "services_destiny.csv"
SERVICE_OPTIONS_CSV = "service_options_destiny.csv"

SEL_OPTS = EdgeOptions()
SEL_OPTS.page_load_strategy = "eager"
# SEL_OPTS.add_argument("--headless=new")  # uncomment for headless
SEL_OPTS.add_argument("--no-sandbox")
SEL_OPTS.add_argument("--disable-dev-shm-usage")

# ========================= CSV helpers =========================
def ensure_services_csv():
    if not os.path.exists(SERVICES_CSV):
        with open(SERVICES_CSV, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow([
                "service_id", "game_id", "name", "description",
                "price_per_unit", "sale_price", "icon_url", "category",
                "game_name"  # helper column; map later if you wish
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

def get_next_service_id() -> int:
    ensure_services_csv()
    mx = 0
    with open(SERVICES_CSV, "r", encoding="utf-8") as f:
        rdr = csv.DictReader(f)
        for row in rdr:
            try:
                mx = max(mx, int(row["service_id"]))
            except:
                pass
    return mx + 1

def get_next_option_id() -> int:
    ensure_options_csv()
    mx = 0
    with open(SERVICE_OPTIONS_CSV, "r", encoding="utf-8") as f:
        rdr = csv.DictReader(f)
        for row in rdr:
            try:
                mx = max(mx, int(row["option_id"]))
            except:
                pass
    return mx + 1

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

# ========================= Text/price utils =========================
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

def normalize_url(href: str) -> str:
    if not href:
        return href
    href = href.strip()
    if href.startswith("/"):
        return BASE_URL + href
    return href

# ========================= Selenium helpers =========================
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

# ========================= Semantic signatures =========================
def group_kind_and_signature(driver, group_el) -> Tuple[str, str, str]:
    """
    Return (kind, label, signature)
    kind ∈ {"slider","range","radio","buttons","checkbox","select","unknown"}
    label is a human label used as a stable key if possible.
    signature is a hash built from semantic content (not raw HTML).
    """
    def h(s):
        return hashlib.sha256((s or "").encode("utf-8","ignore")).hexdigest()

    # Prefer the group head label if present (e.g., "Difficulty")
    label_el_q = group_el.find_elements(By.CSS_SELECTOR, ".product-option__label")
    head_label = ""
    if label_el_q:
        head_label = get_clean_text_el(label_el_q[0])

    # ---------- RANGE/SLIDER (range cluster) ----------
    range_q = group_el.find_elements(By.CSS_SELECTOR, ".product-option-cluster-range")
    if range_q:
        cluster = range_q[0]

        # labels above inputs (e.g., "Random Rolls", "Current Power", "Needed Power")
        input_label_els = cluster.find_elements(By.CSS_SELECTOR, ".input-container .label")
        input_labels = [get_clean_text_el(el) for el in input_label_els if get_clean_text_el(el)]

        # count knobs & inputs
        knob_count = 0
        try:
            knob_count = len(cluster.find_elements(By.CSS_SELECTOR, ".range__body .range__knob"))
        except:
            pass
        input_count = 0
        try:
            input_count = len(cluster.find_elements(By.CSS_SELECTOR, ".input-container input"))
        except:
            pass

        # derive display label for keying: prefer input labels if head is missing
        if head_label:
            label_for_key = head_label
        elif input_labels:
            label_for_key = " - ".join(input_labels)
        else:
            label_for_key = "Range"

        # scale bounds
        scales = []
        try:
            for it in cluster.find_elements(By.CSS_SELECTOR, ".range__scale-item"):
                t = clean_text(it.text)
                if t:
                    scales.append(t)
        except:
            pass
        scale_sig = ""
        if scales:
            scale_sig = f"{scales[0]}..{scales[-1]}"

        # defaults
        defaults = []
        try:
            for inp in cluster.find_elements(By.CSS_SELECTOR, ".input-container input"):
                defaults.append(inp.get_attribute("value") or "")
        except:
            pass

        if knob_count >= 2 or input_count >= 2:
            # RANGE (two-way)
            sig = f"range|{label_for_key}|labels:{'|'.join(input_labels)}|scale:{scale_sig}|defs:{'|'.join(defaults)}"
            return ("range", label_for_key, h(sig))
        else:
            # SLIDER (one-way)
            # set label to either head label or the single input label (e.g., "Random Rolls")
            label_for_key = input_labels[0] if (not head_label and input_labels) else (head_label or "Slider")
            defval = defaults[0] if defaults else ""
            sig = f"slider|{label_for_key}|scale:{scale_sig}|def:{defval}"
            return ("slider", label_for_key, h(sig))

    # ---------- RADIOS ----------
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
        label_for_key = head_label or "Radios"
        sig = f"radio|{label_for_key}|items:{'||'.join(items)}"
        return ("radio", label_for_key, h(sig))

    # ---------- BUTTONS ----------
    buttons_q = group_el.find_elements(By.CSS_SELECTOR, ".product-option-cluster-buttons")
    if buttons_q:
        items = []
        for btn in buttons_q[0].find_elements(By.CSS_SELECTOR, "button"):
            if not is_visible(driver, btn):
                continue
            lab_q = btn.find_elements(By.CSS_SELECTOR, ".button-option__label")
            lab_txt = get_clean_text_el(lab_q[0]) if lab_q else get_clean_text_el(btn)
            items.append(lab_txt)
        label_for_key = head_label or "Buttons"
        sig = f"buttons|{label_for_key}|items:{'|'.join(items)}"
        return ("buttons", label_for_key, h(sig))

    # ---------- CHECKBOXES ----------
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
        label_for_key = head_label or "Checkboxes"
        sig = f"checkbox|{label_for_key}|items:{'||'.join(items)}"
        return ("checkbox", label_for_key, h(sig))

    # ---------- SELECT ----------
    select_q = group_el.find_elements(By.CSS_SELECTOR, ".product-option-cluster-select select")
    if select_q:
        try:
            sel = Select(select_q[0])
            items = [clean_text(opt.text) for opt in sel.options]
        except:
            items = []
        label_for_key = head_label or "Dropdown"
        sig = f"select|{label_for_key}|items:{'||'.join(items)}"
        return ("select", label_for_key, h(sig))

    # ---------- UNKNOWN ----------
    label_for_key = head_label or "Option"
    try:
        html = group_el.get_attribute("outerHTML") or ""
    except:
        html = ""
    return ("unknown", label_for_key, h(f"unknown|{label_for_key}|{html[:500]}"))

# ========================= Writers =========================
def write_slider(driver, group_el, _label, parent_option_id, service_id, option_id_start, display_order, rows_out):
    """
    One-way slider:
      - parent option_type='slider'
      - child option_type='slider'
      - label & name derived from DOM (e.g., "Random Rolls")
    """
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    option_id = option_id_start

    cluster = group_el.find_element(By.CSS_SELECTOR, ".product-option-cluster-range")

    # main label for the slider (prefer input label, else group head, else "Slider")
    input_labels = [get_clean_text_el(el) for el in cluster.find_elements(By.CSS_SELECTOR, ".input-container .label") if get_clean_text_el(el)]
    head_label_els = group_el.find_elements(By.CSS_SELECTOR, ".product-option__label")
    head_label = get_clean_text_el(head_label_els[0]) if head_label_els else ""
    parent_label = input_labels[0] if (input_labels and not head_label) else (head_label or "Slider")

    # slug for name
    parent_name = re.sub(r"[^a-z0-9_]+", "_", parent_label.lower().strip())

    # scale bounds
    min_val = max_val = None
    scales = cluster.find_elements(By.CSS_SELECTOR, ".range__scale-item")
    if scales:
        try:
            min_val = int(clean_text(scales[0].text))
        except:
            pass
        try:
            max_val = int(clean_text(scales[-1].text))
        except:
            pass

    # default value
    default_val = ""
    inps = cluster.find_elements(By.CSS_SELECTOR, ".input-container input")
    if inps:
        default_val = inps[0].get_attribute("value") or ""

    # parent row
    parent_id = option_id; option_id += 1
    rows_out.append({
        'option_id': parent_id, 'service_id': service_id, 'parent_option_id': parent_option_id,
        'option_type': 'slider', 'option_name': parent_name,
        'option_label': parent_label, 'option_value': None, 'price_modifier': 0.00,
        'min_value': None, 'max_value': None, 'default_value': None,
        'is_required': 1, 'display_order': display_order, 'is_active': 1,
        'created_at': now, 'updated_at': now
    })
    display_order += 1

    # child row (holds the actual value)
    rows_out.append({
        'option_id': option_id, 'service_id': service_id, 'parent_option_id': parent_id,
        'option_type': 'slider', 'option_name': f"{parent_name}_value",
        'option_label': parent_label, 'option_value': default_val, 'price_modifier': 0.00,
        'min_value': min_val, 'max_value': max_val, 'default_value': default_val,
        'is_required': 1, 'display_order': display_order, 'is_active': 1,
        'created_at': now, 'updated_at': now
    })
    option_id += 1
    display_order += 1

    return option_id, display_order, parent_id

def write_range(driver, group_el, _label, parent_option_id, service_id, option_id_start, display_order, rows_out):
    """
    Two-way range (from/to):
      - parent option_type='range'
      - two children option_type='range' (from & to)
      - labels taken from the two input labels (e.g., "Current Power", "Needed Power")
    """
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    option_id = option_id_start

    cluster = group_el.find_element(By.CSS_SELECTOR, ".product-option-cluster-range")

    lbls = [get_clean_text_el(el) for el in cluster.find_elements(By.CSS_SELECTOR, ".input-container .label") if get_clean_text_el(el)]
    left_label = lbls[0] if len(lbls) >= 1 else "From"
    right_label = lbls[1] if len(lbls) >= 2 else "To"

    parent_label = f"{left_label} - {right_label}"
    parent_name = re.sub(r"[^a-z0-9_]+", "_", parent_label.lower().strip())

    # defaults
    inputs = cluster.find_elements(By.CSS_SELECTOR, ".input-container input")
    left_val = inputs[0].get_attribute("value") if len(inputs) >= 1 else ""
    right_val = inputs[1].get_attribute("value") if len(inputs) >= 2 else ""

    # scale bounds
    min_val = max_val = None
    scales = cluster.find_elements(By.CSS_SELECTOR, ".range__scale-item")
    if scales:
        try:
            min_val = int(clean_text(scales[0].text))
        except:
            pass
        try:
            max_val = int(clean_text(scales[-1].text))
        except:
            pass

    # parent
    parent_id = option_id; option_id += 1
    rows_out.append({
        'option_id': parent_id, 'service_id': service_id, 'parent_option_id': parent_option_id,
        'option_type': 'range', 'option_name': parent_name,
        'option_label': parent_label, 'option_value': None, 'price_modifier': 0.00,
        'min_value': None, 'max_value': None, 'default_value': None,
        'is_required': 1, 'display_order': display_order, 'is_active': 1,
        'created_at': now, 'updated_at': now
    })
    display_order += 1

    # child: from
    rows_out.append({
        'option_id': option_id, 'service_id': service_id, 'parent_option_id': parent_id,
        'option_type': 'range', 'option_name': f"{parent_name}_from",
        'option_label': left_label, 'option_value': left_val or "", 'price_modifier': 0.00,
        'min_value': min_val, 'max_value': max_val, 'default_value': left_val or "",
        'is_required': 1, 'display_order': display_order, 'is_active': 1,
        'created_at': now, 'updated_at': now
    })
    option_id += 1
    display_order += 1

    # child: to
    rows_out.append({
        'option_id': option_id, 'service_id': service_id, 'parent_option_id': parent_id,
        'option_type': 'range', 'option_name': f"{parent_name}_to",
        'option_label': right_label, 'option_value': right_val or "", 'price_modifier': 0.00,
        'min_value': min_val, 'max_value': max_val, 'default_value': right_val or "",
        'is_required': 1, 'display_order': display_order, 'is_active': 1,
        'created_at': now, 'updated_at': now
    })
    option_id += 1
    display_order += 1

    return option_id, display_order, parent_id

def write_buttons_as_radio(driver, group_el, label, parent_option_id, service_id, option_id_start, display_order, rows_out):
    """Treat Buttons parent/children as Radios."""
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
            val = re.sub(r"[^a-z0-9_]+", "_", txt.lower().strip())
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
            'option_type': 'radio', 'option_name': f"{label.lower().replace(' ','_')}_{re.sub(r'[^a-z0-9_]+','_', txt.lower().strip())}",
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
        val = chk.get_attribute("value") or re.sub(r"[^a-z0-9_]+", "_", txt.lower().strip())
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

def parse_group_to_rows(driver, group_el, kind: str, label: str, parent_option_id: int, service_id: int,
                        option_id_start: int, display_order: int, rows_out: List[dict]):
    if kind == "slider":
        return write_slider(driver, group_el, label, parent_option_id, service_id, option_id_start, display_order, rows_out)
    if kind == "range":
        return write_range(driver, group_el, label, parent_option_id, service_id, option_id_start, display_order, rows_out)
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
                val = opt.get_attribute("value") or re.sub(r"[^a-z0-9_]+","_", txt.lower().strip())
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

# ========================= Normalization =========================
def normalize_option_types_in_rows(rows: List[dict]) -> List[dict]:
    """Strip *_value from option_type; convert button(s) to radio; leave 'range' as-is."""
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

# ========================= Snapshot Extraction =========================
def extract_options_with_snapshots(driver, service_id: int):
    option_id = get_next_option_id()
    rows_out = []
    display_order = 1
    wait = WebDriverWait(driver, 25)

    container = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, ".product-detail-calculator__options")))

    # Baseline scan
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
        option_id, display_order, _parent_id = parse_group_to_rows(
            driver, group_el, kind, label, parent_option_id=None, service_id=service_id,
            option_id_start=option_id, display_order=display_order, rows_out=rows_out
        )

    # Find Difficulty radios
    difficulty_group_el = None
    for group_el, kind, label in baseline_groups:
        if kind == "radio" and label.strip().lower() == "difficulty":
            difficulty_group_el = group_el
            break

    if not difficulty_group_el:
        return rows_out

    # Map: radio input value -> row_id of the specific radio option row we wrote
    radio_value_parent_row_ids: Dict[str, int] = {}
    for r in rows_out:
        if r['option_type'] == 'radio' and r['service_id'] == service_id and r['option_name'].startswith('difficulty_') and r['parent_option_id'] is not None:
            radio_value_parent_row_ids[str(r['option_value'])] = r['option_id']

    # Collect actual radio inputs to click
    radio_inputs = difficulty_group_el.find_elements(By.CSS_SELECTOR, ".product-option-cluster-radios .radio-option input[type='radio']")
    radio_values = []
    for inp in radio_inputs:
        v = inp.get_attribute("value") or ""
        if v:
            radio_values.append(v)

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

    # Click each difficulty radio and parse only changed groups
    for val in radio_values:
        parent_radio_row_id = radio_value_parent_row_ids.get(str(val))
        if not parent_radio_row_id:
            continue

        prev_html = container.get_attribute("innerHTML") or ""
        prev_sig = hashlib.sha256(prev_html.encode("utf-8","ignore")).hexdigest()

        try:
            target_inp = difficulty_group_el.find_element(By.CSS_SELECTOR, f".product-option-cluster-radios input[type='radio'][value='{val}']")
            driver.execute_script("arguments[0].click();", target_inp)
        except:
            continue

        time.sleep(0.25)
        wait_dom_change(prev_sig, timeout=4.0)

        # Re-scan visible groups
        cur_groups = container.find_elements(By.CSS_SELECTOR, ".option-group")
        for g in cur_groups:
            if not is_visible(driver, g):
                continue
            inner = g.find_elements(By.CSS_SELECTOR, ".product-option")
            if not inner:
                continue
            kind, label, sig = group_kind_and_signature(driver, inner[0])

            # skip Difficulty itself
            if kind == "radio" and label.strip().lower() == "difficulty":
                continue

            baseline_sig = baseline_sigs.get((kind, label))
            if baseline_sig and baseline_sig == sig:
                continue  # unchanged

            # remember latest signature to avoid duping same shape again
            baseline_sigs[(kind, label)] = sig

            option_id, display_order, _child_parent = parse_group_to_rows(
                driver, inner[0], kind, label, parent_option_id=parent_radio_row_id,
                service_id=service_id, option_id_start=option_id, display_order=display_order, rows_out=rows_out
            )

    return rows_out

# ========================= Product page scraping =========================
def extract_service_info_and_options_dynamic(product_url: str, category: str, game_name: str):
    """Scrape one product page with dynamic snapshot logic + image extraction."""
    driver = webdriver.Edge(options=SEL_OPTS)
    try:
        url = normalize_url(product_url)
        driver.get(url)
        time.sleep(2.0)

        html = driver.page_source
        soup = BeautifulSoup(html, "html.parser")

        service_id = get_next_service_id()

        # Name
        name = None
        gh = soup.find("div", class_="game-header")
        if gh and gh.find("h1"):
            name = clean_text(gh.find("h1").get_text())
        if not name and soup.find("h1"):
            name = clean_text(soup.find("h1").get_text())

        # Description
        desc = None
        ds = soup.find("div", class_="product-info-section__html")
        if ds:
            desc = clean_text(ds.get_text(separator="\n"))

        # Image (prefer og:image)
        icon_url = None
        og = soup.find("meta", property="og:image")
        if og and og.get("content"):
            icon_url = normalize_url(og["content"])
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

        # Price
        price_per_unit = ""
        sale_price = ""
        p = soup.find("span", class_="payment-summary__price-column-total")
        if p:
            price_per_unit = parse_currency_to_decimal(p.get_text(strip=True))

        # Write service row
        ensure_services_csv()
        with open(SERVICES_CSV, "a", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow([
                service_id, "", name or "", desc or "", price_per_unit, sale_price,
                icon_url or "", category or "", game_name or ""
            ])

        # Extract options with snapshot logic
        rows = extract_options_with_snapshots(driver, service_id=service_id)
        rows = normalize_option_types_in_rows(rows)
        appended = append_options(rows)

        print(f"✔ service_id={service_id} name='{name}' options_written={appended} url={url}")
        return {"service_id": service_id, "name": name, "options_count": appended, "url": url}
    except Exception as e:
        print(f"✖ Error scraping {product_url}: {e}")
        return {"error": str(e), "url": product_url}
    finally:
        driver.quit()

# ========================= Listing helpers =========================
def derive_game_name_from_filename(file_name: str) -> str:
    # "Destiny_2_services.csv" -> "Destiny 2"
    base = file_name.replace("_services.csv", "").replace(".csv", "")
    return base.replace("_", " ").strip()

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
        # de-dup while preserving order
        seen = set()
        deduped = []
        for u in nested:
            if u not in seen:
                seen.add(u)
                deduped.append(u)
        return deduped
    except Exception:
        return []
    finally:
        driver.quit()

# ========================= Batch processing =========================
def process_listing_file(file_name: str):
    path = os.path.join(INPUT_DIR, file_name)
    df = pd.read_csv(path)
    category = derive_game_name_from_filename(file_name)
    game_name = category

    results = []
    # Each row has a 'Link' (listing page → contains many product cards)
    for _, row in df.iterrows():
        link = str(row.get("Link", "")).strip()
        if not link or not link.startswith(BASE_URL):
            continue
        product_urls = get_nested_links_from_listing(link)
        print(f"Found {len(product_urls)} products under listing: {link}")
        for purl in product_urls:
            res = extract_service_info_and_options_dynamic(purl, category, game_name)
            results.append(res)
    return results

def main():
    ensure_services_csv()
    ensure_options_csv()

    if not os.path.isdir(INPUT_DIR):
        print(f"Input folder '{INPUT_DIR}' not found.")
        return

    files = sorted([f for f in os.listdir(INPUT_DIR) if f.endswith(".csv")])
    if not files:
        print(f"No CSV files found in '{INPUT_DIR}'.")
        return

    total_ok = 0
    total_attempts = 0
    for file_name in files:
        print(f"\n=== Processing: {file_name} ===")
        res = process_listing_file(file_name)
        total_attempts += len(res)
        ok = sum(1 for r in res if r and not r.get("error"))
        total_ok += ok
        print(f"Done {file_name}: {ok} services scraped ({len(res)} attempts).")

    print(f"\nAll done. {total_ok} services scraped successfully out of {total_attempts} attempts.")
    print(f"Outputs: {SERVICES_CSV}, {SERVICE_OPTIONS_CSV}")

if __name__ == "__main__":
    main()
