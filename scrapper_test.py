# one_page_skycoach_snapshots.py
# Scrape ONE product page:
# https://skycoach.gg/destiny-boost/products/opaque-hourglass-8613
#
# - Baseline parse: write top-level groups ONCE.
# - For each Difficulty radio: click -> re-snapshot -> parse ONLY groups whose
#   "semantic signature" differs from baseline (i.e., the page actually changed).
# - Changed groups are stored as children of that radio value.
#
# CSV outputs: services.csv, service_options.csv

import os
import re
import csv
import time
import hashlib
from datetime import datetime
from typing import Dict, Tuple, List

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.edge.options import Options as EdgeOptions
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC

# ----------------- Config -----------------
PRODUCT_URL = "https://skycoach.gg/destiny-boost/products/finite-maybe-8616"
SERVICES_CSV = "services.csv"
SERVICE_OPTIONS_CSV = "service_options.csv"

SEL_OPTS = EdgeOptions()
SEL_OPTS.page_load_strategy = "eager"
# SEL_OPTS.add_argument("--headless=new")
SEL_OPTS.add_argument("--no-sandbox")
SEL_OPTS.add_argument("--disable-dev-shm-usage")

# ----------------- CSV helpers -----------------
def ensure_services_csv():
    if not os.path.exists(SERVICES_CSV):
        with open(SERVICES_CSV, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow([
                "service_id", "game_id", "name", "description",
                "price_per_unit", "sale_price", "icon_url", "category",
                "game_name"
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
                f"{o['price_modifier']:.2f}",
                o['min_value'], o['max_value'], o['default_value'],
                o['is_required'], o['display_order'], o['is_active'],
                o['created_at'], o['updated_at']
            ])
    return len(rows)

# ----------------- text/price utils -----------------
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

# ----------------- Selenium helpers -----------------
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

# ----------------- Semantic signature (stable across re-renders) -----------------
def group_kind_and_signature(group_el) -> Tuple[str, str, str]:
    """
    Return (kind, label, signature) where:
      - kind ∈ {"slider","radio","buttons","checkbox","select","unknown"}
      - label is the option group label text
      - signature is a hash built from semantic content (labels/values), NOT raw HTML
    """
    label_el_q = group_el.find_elements(By.CSS_SELECTOR, ".product-option__label")
    label = get_clean_text_el(label_el_q[0]) if label_el_q else "Option"
    slug = label.lower().replace(":", "").strip()

    def h(s):  # short hasher
        return hashlib.sha256((s or "").encode("utf-8","ignore")).hexdigest()

    # slider?
    range_q = group_el.find_elements(By.CSS_SELECTOR, ".product-option-cluster-range")
    if range_q:
        if is_visible(group_el.parent, group_el):  # parent is the .product-option
            # collect scale + default value
            scales = []
            try:
                for it in range_q[0].find_elements(By.CSS_SELECTOR, ".range__scale-item"):
                    t = clean_text(it.text)
                    if t:
                        scales.append(t)
            except:
                pass
            default_val = ""
            try:
                inps = range_q[0].find_elements(By.CSS_SELECTOR, ".input-container input")
                if inps:
                    default_val = inps[0].get_attribute("value") or ""
            except:
                pass
            sig = f"slider|{slug}|scale:{','.join(scales)}|def:{default_val}"
            return ("slider", label, h(sig))

    # radios?
    radios_q = group_el.find_elements(By.CSS_SELECTOR, ".product-option-cluster-radios")
    if radios_q:
        items = []
        for ro in radios_q[0].find_elements(By.CSS_SELECTOR, ".radio-option"):
            if not is_visible(group_el.parent, ro):
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
            if not is_visible(group_el.parent, btn):
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
            if not is_visible(group_el.parent, co):
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

# ----------------- Writers (parent + children) -----------------
def write_slider(group_el, label, parent_option_id, service_id, option_id_start, display_order, rows_out):
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    option_id = option_id_start

    parent_id = option_id; option_id += 1
    rows_out.append({
        'option_id': parent_id, 'service_id': service_id, 'parent_option_id': parent_option_id,
        'option_type': 'slider', 'option_name': label.lower().replace(" ", "_").replace(":",""),
        'option_label': label, 'option_value': None, 'price_modifier': 0.00,
        'min_value': None, 'max_value': None, 'default_value': None,
        'is_required': 1, 'display_order': display_order, 'is_active': 1,
        'created_at': now, 'updated_at': now
    })
    display_order += 1

    min_val = max_val = None
    default_val = ""
    try:
        scales = group_el.find_elements(By.CSS_SELECTOR, ".product-option-cluster-range .range__scale-item")
        if scales:
            min_val = int(clean_text(scales[0].text))
            max_val = int(clean_text(scales[-1].text))
    except:
        pass
    try:
        inps = group_el.find_elements(By.CSS_SELECTOR, ".product-option-cluster-range .input-container input")
        if inps:
            default_val = inps[0].get_attribute("value") or ""
    except:
        pass

    # child row
    rows_out.append({
        'option_id': option_id, 'service_id': service_id, 'parent_option_id': parent_id,
        'option_type': 'slider_value', 'option_name': f"{label.lower().replace(' ','_')}_value",
        'option_label': 'Rolls', 'option_value': default_val, 'price_modifier': 0.00,
        'min_value': min_val, 'max_value': max_val, 'default_value': default_val,
        'is_required': 1, 'display_order': display_order, 'is_active': 1,
        'created_at': now, 'updated_at': now
    })
    option_id += 1
    display_order += 1
    return option_id, display_order, parent_id

def write_buttons(group_el, label, parent_option_id, service_id, option_id_start, display_order, rows_out):
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    option_id = option_id_start

    parent_id = option_id; option_id += 1
    rows_out.append({
        'option_id': parent_id, 'service_id': service_id, 'parent_option_id': parent_option_id,
        'option_type': 'buttons', 'option_name': label.lower().replace(" ", "_").replace(":",""),
        'option_label': label, 'option_value': None, 'price_modifier': 0.00,
        'min_value': None, 'max_value': None, 'default_value': None,
        'is_required': 1, 'display_order': display_order, 'is_active': 1,
        'created_at': now, 'updated_at': now
    })
    display_order += 1

    groups = group_el.find_elements(By.CSS_SELECTOR, ".product-option-cluster-buttons .buttons-group")
    for bg in groups:
        for btn in bg.find_elements(By.CSS_SELECTOR, "button"):
            if not is_visible(group_el.parent, btn):
                continue
            lab_q = btn.find_elements(By.CSS_SELECTOR, ".button-option__label")
            txt = get_clean_text_el(lab_q[0]) if lab_q else get_clean_text_el(btn)
            val = txt.lower().replace(" ", "_")
            rows_out.append({
                'option_id': option_id, 'service_id': service_id, 'parent_option_id': parent_id,
                'option_type': 'button_value', 'option_name': f"{label.lower().replace(' ','_')}_{val}",
                'option_label': txt, 'option_value': val, 'price_modifier': 0.00,
                'min_value': None, 'max_value': None, 'default_value': None,
                'is_required': 0, 'display_order': display_order, 'is_active': 1,
                'created_at': now, 'updated_at': now
            })
            option_id += 1
            display_order += 1

    return option_id, display_order, parent_id

def write_radios(group_el, label, parent_option_id, service_id, option_id_start, display_order, rows_out):
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

    value_rows = []  # (row_id, value)
    for ro in group_el.find_elements(By.CSS_SELECTOR, ".product-option-cluster-radios .radio-option"):
        if not is_visible(group_el.parent, ro):
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
            'option_type': 'radio_value', 'option_name': f"{label.lower().replace(' ','_')}_{txt.lower().replace(' ','_')}",
            'option_label': txt, 'option_value': val, 'price_modifier': price_mod,
            'min_value': None, 'max_value': None, 'default_value': (val if is_checked else None),
            'is_required': 0, 'display_order': display_order, 'is_active': 1,
            'created_at': now, 'updated_at': now
        })
        value_rows.append((row_id, val))
        option_id += 1
        display_order += 1

    return option_id, display_order, parent_id, value_rows

def write_checkboxes(group_el, label, parent_option_id, service_id, option_id_start, display_order, rows_out):
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
        if not is_visible(group_el.parent, co):
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
            'option_type': 'checkbox_value', 'option_name': f"{label.lower().replace(' ','_')}_{val}",
            'option_label': txt, 'option_value': val, 'price_modifier': price_mod,
            'min_value': None, 'max_value': None, 'default_value': (val if is_checked else None),
            'is_required': 0, 'display_order': display_order, 'is_active': 1,
            'created_at': now, 'updated_at': now
        })
        option_id += 1
        display_order += 1

    return option_id, display_order, parent_id

# Parse a group into rows using the appropriate writer
def parse_group_to_rows(group_el, kind: str, label: str, parent_option_id: int, service_id: int,
                        option_id_start: int, display_order: int, rows_out: List[dict]):
    if kind == "slider":
        return write_slider(group_el, label, parent_option_id, service_id, option_id_start, display_order, rows_out)
    if kind == "radio":
        oid, disp, parent_id, _vals = write_radios(group_el, label, parent_option_id, service_id, option_id_start, display_order, rows_out)
        return oid, disp, parent_id
    if kind == "buttons":
        return write_buttons(group_el, label, parent_option_id, service_id, option_id_start, display_order, rows_out)
    if kind == "checkbox":
        return write_checkboxes(group_el, label, parent_option_id, service_id, option_id_start, display_order, rows_out)
    if kind == "select":
        # Treat select as buttons-like (parent + child options)
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
                    'option_type': 'dropdown_value', 'option_name': f"{label.lower().replace(' ','_')}_{val}",
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

    # unknown → skip
    return option_id_start, display_order, None

# ----------------- Main extraction with snapshots -----------------
def extract_options_with_snapshots(driver, service_id: int):
    option_id = get_next_option_id_from_options_csv()
    rows_out = []
    display_order = 1
    wait = WebDriverWait(driver, 25)

    # container
    container = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, ".product-detail-calculator__options")))

    # 1) BASELINE: parse all visible groups once
    baseline_sigs: Dict[Tuple[str,str], str] = {}  # (kind,label) -> signature
    baseline_groups: List[Tuple[object,str,str]] = []  # (el, kind, label)

    groups = container.find_elements(By.CSS_SELECTOR, ".option-group")
    for g in groups:
        if not is_visible(driver, g):
            continue
        inner = g.find_elements(By.CSS_SELECTOR, ".product-option")
        if not inner:
            continue
        kind, label, sig = group_kind_and_signature(inner[0])
        baseline_sigs[(kind, label)] = sig
        baseline_groups.append((inner[0], kind, label))

    # Write baseline top-level groups
    for group_el, kind, label in baseline_groups:
        option_id, display_order, _parent_id = parse_group_to_rows(
            group_el, kind, label, parent_option_id=None, service_id=service_id,
            option_id_start=option_id, display_order=display_order, rows_out=rows_out
        )

    # Locate the Difficulty radio group in baseline (to know which radios to click)
    difficulty_group_el = None
    for group_el, kind, label in baseline_groups:
        if kind == "radio" and label.strip().lower() == "difficulty":
            difficulty_group_el = group_el
            break

    if not difficulty_group_el:
        return rows_out  # no radios to click; we're done

    # Map: radio value text -> row_id we wrote above (to parent dynamic children)
    # Build index from baseline rows we just wrote.
    radio_value_parent_row_ids: Dict[str, int] = {}
    for r in rows_out:
        if r['option_type'] == 'radio_value' and r['service_id'] == service_id and r['option_name'].startswith('difficulty_'):
            # Use the option_value (1/2/3) to key or label; we use option_value for stability
            radio_value_parent_row_ids[str(r['option_value'])] = r['option_id']

    # Get the actual inputs to click (by value)
    radio_inputs = difficulty_group_el.find_elements(By.CSS_SELECTOR, ".product-option-cluster-radios .radio-option input[type='radio']")
    radio_values = []
    for inp in radio_inputs:
        v = inp.get_attribute("value") or ""
        if v:
            radio_values.append(v)

    # Helper to wait until the DOM actually changes after clicking
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
        return prev_sig  # no change detected; avoid hang

    # 2) SNAPSHOTS: click each radio, capture changed groups only
    for val in radio_values:
        # Get row_id of the baseline radio_value we wrote (to parent children)
        parent_radio_row_id = radio_value_parent_row_ids.get(str(val))
        if not parent_radio_row_id:
            continue

        # prev signature of entire options container
        prev_html = container.get_attribute("innerHTML") or ""
        prev_sig = hashlib.sha256(prev_html.encode("utf-8","ignore")).hexdigest()

        # click this radio
        try:
            target_inp = difficulty_group_el.find_element(By.CSS_SELECTOR, f".product-option-cluster-radios input[type='radio'][value='{val}']")
            driver.execute_script("arguments[0].click();", target_inp)
        except:
            continue

        # wait a bit for DOM update
        time.sleep(0.25)
        new_sig = wait_dom_change(prev_sig, timeout=4.0)

        # re-scan visible groups after click
        cur_groups = container.find_elements(By.CSS_SELECTOR, ".option-group")
        for g in cur_groups:
            if not is_visible(driver, g):
                continue
            inner = g.find_elements(By.CSS_SELECTOR, ".product-option")
            if not inner:
                continue
            kind, label, sig = group_kind_and_signature(inner[0])

            # Skip the Difficulty group itself
            if kind == "radio" and label.strip().lower() == "difficulty":
                continue

            # Only keep groups whose semantic signature differs from baseline (changed/new)
            baseline_sig = baseline_sigs.get((kind, label))
            if baseline_sig and baseline_sig == sig:
                continue  # unchanged -> don't duplicate

            # If first time we see a changed version of this (kind,label), remember its sig
            # so we don't add the same changed block again for another radio if it repeats.
            if (kind, label) not in baseline_sigs or baseline_sigs[(kind, label)] != sig:
                baseline_sigs[(kind, label)] = sig

            # parse this group as CHILD of the selected radio value
            option_id, display_order, _child_parent = parse_group_to_rows(
                inner[0], kind, label, parent_option_id=parent_radio_row_id,
                service_id=service_id, option_id_start=option_id, display_order=display_order, rows_out=rows_out
            )

    return rows_out

# ----------------- Scrape the single page -----------------
def scrape_single():
    ensure_services_csv()
    ensure_options_csv()

    driver = webdriver.Edge(options=SEL_OPTS)
    try:
        driver.get(PRODUCT_URL)
        time.sleep(2.0)

        # Basic service info
        html = driver.page_source
        soup = BeautifulSoup(html, "html.parser")

        service_id = get_next_service_id_from_services_csv()

        # name
        name = None
        gh = soup.find("div", class_="game-header")
        if gh and gh.find("h1"):
            name = clean_text(gh.find("h1").get_text())
        if not name and soup.find("h1"):
            name = clean_text(soup.find("h1").get_text())

        # desc
        desc = None
        ds = soup.find("div", class_="product-info-section__html")
        if ds:
            desc = clean_text(ds.get_text(separator="\n"))

        # icon
        icon_url = None
        og = soup.find("meta", property="og:image")
        if og and og.get("content"):
            icon_url = og["content"].strip()

        # default price
        price_per_unit = ""
        p = soup.find("span", class_="payment-summary__price-column-total")
        if p:
            price_per_unit = parse_currency_to_decimal(p.get_text(strip=True))

        # placeholders
        category = "Destiny 2"
        game_name = "Destiny 2"

        with open(SERVICES_CSV, "a", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow([
                service_id, "", name or "", desc or "", price_per_unit, "",
                icon_url or "", category, game_name
            ])

        # Extract using snapshots (baseline + per-radio changes)
        rows = extract_options_with_snapshots(driver, service_id=service_id)
        n = append_options(rows)
        print(f"✔ service_id={service_id} name='{name}' options_written={n}")

    finally:
        driver.quit()

# ----------------- main -----------------
if __name__ == "__main__":
    scrape_single()
