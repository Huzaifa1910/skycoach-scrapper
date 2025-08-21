#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
overgear_scraper_selenium.py

Single-file Selenium-based scraper for Overgear-like product pages that:
- Uses Selenium WebDriver to load dynamic pages (JS-rendered)
- Extracts product details via JSON-LD + DOM fallbacks
- Parses options (slider + radio/checkbox), safely
- Normalizes price modifiers (+50%, +$10)
- Can estimate a price from selected options
- Supports multiple URLs and optional light crawling
"""

import argparse
import json
import re
import sys
import time
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse

# -------------------------- BeautifulSoup --------------------------

try:
    from bs4 import BeautifulSoup
except Exception:
    print("Please install beautifulsoup4: pip install beautifulsoup4", file=sys.stderr)
    raise

PARSER = "lxml"
try:
    import lxml  # noqa: F401
except Exception:
    PARSER = "html.parser"

def get_soup(html: str) -> BeautifulSoup:
    return BeautifulSoup(html, PARSER)

def clean_text(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())

def parse_money(txt: str) -> Tuple[str, Optional[str]]:
    """
    Return (amount:str like '15.00', currency:str or None) from a money-like text.
    """
    if not txt:
        return ("", None)
    t = txt.strip()
    currency = None
    if t.startswith("$"):
        currency = "USD"
    num = re.sub(r"[^\d,.\-]", "", t)
    if "," in num and "." in num:
        num = num.replace(",", "")
    elif "," in num and "." not in num:
        num = num.replace(",", ".")
    try:
        val = f"{float(num):.2f}"
        return (val, currency)
    except Exception:
        return ("", currency)

# -------------------------- Selenium Loader --------------------------

# Default to Chrome; add Firefox if you want (not required now)
try:
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options as ChromeOptions
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.common.exceptions import TimeoutException
except Exception:
    print("Please install selenium: pip install selenium", file=sys.stderr)
    raise

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

def build_chrome(headless: bool = True, driver_path: Optional[str] = None) -> webdriver.Chrome:
    opts = ChromeOptions()
    if headless:
        opts.add_argument("--headless=new")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1400,900")
    opts.add_argument(f"--user-agent={UA}")
    opts.add_argument("--lang=en-US")
    # reduce automation flags visibility
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)
    # allow mixed content (sometimes assets hosted differently)
    opts.add_argument("--allow-running-insecure-content")

    if driver_path:
        driver = webdriver.Chrome(driver_path, options=opts)
    else:
        driver = webdriver.Chrome(options=opts)

    # Stealth-ish tweaks
    try:
        driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
            "source": """
                Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
                window.chrome = { runtime: {} };
                Object.defineProperty(navigator, 'platform', {get: () => 'Win32'});
                Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en']});
                Object.defineProperty(navigator, 'plugins', {get: () => [1,2,3]});
            """
        })
    except Exception:
        pass

    return driver

def fetch_with_selenium(
    driver: webdriver.Chrome,
    url: str,
    wait_seconds: int = 25,
    wait_selectors: Optional[List[str]] = None,
    scroll: bool = True
) -> str:
    driver.get(url)

    # Optionally scroll to trigger lazy content
    if scroll:
        try:
            last_h = driver.execute_script("return document.body.scrollHeight")
            for _ in range(6):
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(0.6)
                new_h = driver.execute_script("return document.body.scrollHeight")
                if new_h == last_h:
                    break
                last_h = new_h
            driver.execute_script("window.scrollTo(0, 0);")
        except Exception:
            pass

    # Wait for any of the expected selectors (if provided)
    # Default targets: product title and price area typical of Overgear
    targets = wait_selectors or [
        "h1",
        ".PurchaseWidgetStickyWrap__StyledFlex-sc-2d5n5g-0 .heading-base",
        "form.product-form__StyledForm-sc-ig4zxp-0",
    ]
    try:
        WebDriverWait(driver, wait_seconds).until(
            lambda d: any(len(d.find_elements(By.CSS_SELECTOR, sel)) > 0 for sel in targets)
        )
    except TimeoutException:
        # fall through with whatever we have
        pass

    return driver.page_source

# ---------------------------- JSON-LD & Sections ----------------------------

def get_jsonld_product(soup: BeautifulSoup) -> Optional[Dict[str, Any]]:
    for script in soup.find_all("script", type="application/ld+json"):
        raw = script.string or ""
        if not raw.strip():
            continue
        try:
            data = json.loads(raw)
        except Exception:
            continue
        candidates = data if isinstance(data, list) else [data]
        for d in candidates:
            at = d.get("@type")
            if isinstance(at, list):
                is_product = "Product" in at
            else:
                is_product = at == "Product"
            if isinstance(d, dict) and is_product:
                return d
    return None

def nearest_section_title(el: BeautifulSoup) -> Optional[str]:
    lab = el.find_previous("span", class_="font-main text-body-s font-medium text-neutral-color6")
    return clean_text(lab.get_text()) if lab else None

# ---------------------------- Option Modifiers -----------------------------

def _parse_modifier(raw: Optional[str]) -> Optional[Dict[str, Any]]:
    if not raw:
        return None
    t = raw.strip().lstrip('+').strip()
    if t.endswith('%'):
        num = t[:-1].strip().replace(',', '.')
        try:
            return {"kind": "percent", "value": float(num)}
        except Exception:
            return None
    num = re.sub(r"[^\d,.\-]", "", t)
    if not num:
        return None
    if "," in num and "." in num:
        num = num.replace(",", "")
    elif "," in num and "." not in num:
        num = num.replace(",", ".")
    try:
        return {"kind": "absolute", "value": float(num)}
    except Exception:
        return None

# --------------------------- Core Page Parsing -----------------------------

def parse_product_page(html: str, base_url: Optional[str] = None) -> Dict[str, Any]:
    soup = get_soup(html)

    # Title
    title = None
    h1 = soup.find("h1")
    if h1:
        title = clean_text(h1.get_text())

    # JSON-LD
    product_ld = get_jsonld_product(soup) or {}
    ld_name = product_ld.get("name")
    ld_desc = product_ld.get("description")
    ld_image = product_ld.get("image")
    ld_offers = product_ld.get("offers") or {}
    ld_price = ld_offers.get("Price") or ld_offers.get("price")
    ld_currency = ld_offers.get("priceCurrency") or None

    # Price (DOM fallback)
    price_val, price_currency = ("", None)
    price_text_node = soup.select_one(".PurchaseWidgetStickyWrap__StyledFlex-sc-2d5n5g-0 .heading-base")
    if price_text_node:
        price_val, price_currency = parse_money(price_text_node.get_text(strip=True))
    if ld_price is not None:
        try:
            price_val = f"{float(ld_price):.2f}"
        except Exception:
            pass
    if ld_currency:
        price_currency = ld_currency or price_currency

    # Image
    image_url = None
    if ld_image:
        image_url = ld_image
    else:
        img = soup.select_one("form.product-form__StyledForm-sc-ig4zxp-0 img[alt]")
        if img and img.has_attr("src"):
            image_url = img["src"]
            if base_url and image_url.startswith("/"):
                image_url = urljoin(base_url, image_url)

    # Description
    description = ld_desc
    if not description:
        desc_block = soup.select_one(".style-content-v3__StyleContentV3-sc-1uqk2fg-0")
        if desc_block:
            description = clean_text(desc_block.get_text(" ", strip=True))

    # Options
    options: List[Dict[str, Any]] = []

    # Hours slider
    hours_default = None
    hours_min = None
    hours_max = None

    hours_input = soup.select_one("input#currency")
    if hours_input:
        hours_default = (hours_input.get("value") or "").strip() or None

    slider_handle = soup.select_one(".rc-slider .rc-slider-handle")
    if slider_handle:
        try:
            vmin = slider_handle.get("aria-valuemin")
            vmax = slider_handle.get("aria-valuemax")
            if vmin is not None:
                hours_min = int(vmin)
            if vmax is not None:
                hours_max = int(vmax)
        except Exception:
            pass

    if hours_default or (hours_min is not None) or (hours_max is not None):
        options.append({
            "group": "Number of hours",
            "type": "slider",
            "name": "number_of_hours",
            "default": hours_default,
            "min": hours_min,
            "max": hours_max,
            "choices": None,
        })

    # Discrete options (radio/checkbox)
    for lab in soup.select("form label"):
        inp = lab.find("input")
        if not inp:
            continue
        itype = (inp.get("type") or "").lower()
        if itype not in ("radio", "checkbox"):
            continue

        txt = clean_text(lab.get_text(" ", strip=True))
        m = re.search(r"(\+\s*\$?\d+(?:[.,]\d+)?%?)", txt)
        modifier_text = m.group(1) if m else None
        label_text = clean_text(txt.replace(modifier_text or "", "")).strip()

        group_title = nearest_section_title(lab) or "Options"

        group = next((g for g in options if g["group"] == group_title and g.get("type") != "slider"), None)
        if not group:
            group = {
                "group": group_title,
                "type": itype,
                "name": re.sub(r"[^a-z0-9_]+", "_", group_title.lower()).strip("_"),
                "default": None,
                "min": None,
                "max": None,
                "choices": []
            }
            options.append(group)
        else:
            if group.get("choices") is None:
                group["choices"] = []
            if itype == "radio":
                group["type"] = "radio"

        is_default = inp.has_attr("checked")
        if is_default and group["type"] == "radio":
            group["default"] = label_text

        group["choices"].append({
            "value": inp.get("value", "") or "",
            "label": label_text,
            "price_modifier": modifier_text
        })

    record = {
        "url": base_url,
        "title": title or ld_name,
        "price": price_val or (f"{float(ld_price):.2f}" if ld_price else ""),
        "currency": price_currency or "USD",
        "image": image_url,
        "description": description,
        "options": options
    }
    return record

# ------------------------ Pricing & Choice Indexing ------------------------

def build_choice_index(service_record: Dict[str, Any]) -> Tuple[Dict[str, Any], Optional[Dict[str, Any]]]:
    idx: Dict[str, Any] = {}
    hours_info = None
    for g in service_record.get("options", []):
        if g.get("type") == "slider":
            hours_info = {
                "group": g["group"],
                "name": g.get("name", "number_of_hours"),
                "default": g.get("default"),
                "min": g.get("min"),
                "max": g.get("max"),
            }
            continue
        choices_map = {}
        for c in (g.get("choices") or []):
            label = c.get("label") or ""
            mod = _parse_modifier(c.get("price_modifier"))
            choices_map[label] = {
                "value": c.get("value", ""),
                "modifier": mod
            }
        idx[g["group"]] = {
            "type": g.get("type") or "radio",
            "choices": choices_map,
            "default": g.get("default")
        }
    return idx, hours_info

def estimate_price(service_record: Dict[str, Any], selections: Dict[str, Any],
                   *, apply_abs_per_hour: bool = False) -> Dict[str, Any]:
    currency = service_record.get("currency") or "USD"
    try:
        base = float(service_record.get("price") or 0.0)
    except Exception:
        base = 0.0

    choice_idx, hours_info = build_choice_index(service_record)

    hours = 1.0
    if hours_info:
        raw_h = selections.get(hours_info["group"], hours_info.get("default", 1))
        try:
            hours = float(raw_h) if raw_h is not None else 1.0
        except Exception:
            hours = 1.0
        if isinstance(hours_info.get("min"), int):
            hours = max(hours, float(hours_info["min"]))
        if isinstance(hours_info.get("max"), int):
            hours = min(hours, float(hours_info["max"]))

    subtotal = base * hours
    percent_mods: List[float] = []
    absolute_mods: List[float] = []

    for group_name, meta in choice_idx.items():
        selected = selections.get(group_name)
        if selected is None and meta["type"] == "radio" and meta.get("default"):
            selected = meta["default"]
        if selected is None:
            continue
        if isinstance(selected, str):
            selected_list = [selected]
        else:
            try:
                selected_list = list(selected)
            except Exception:
                selected_list = [str(selected)]
        if meta["type"] == "radio" and selected_list:
            selected_list = selected_list[:1]
        for label in selected_list:
            choice = meta["choices"].get(label)
            if not choice:
                continue
            mod = choice.get("modifier")
            if not mod:
                continue
            if mod["kind"] == "percent":
                percent_mods.append(mod["value"])
            elif mod["kind"] == "absolute":
                absolute_mods.append(mod["value"])

    explain_lines = [f"Base: {currency} {base:.2f} × hours {hours:g} = {currency} {subtotal:.2f}"]
    pct_factor = 1.0
    for p in percent_mods:
        pct_factor *= (1.0 + p/100.0)
        explain_lines.append(f"Percent modifier +{p:.2f}%")
    subtotal *= pct_factor
    if percent_mods:
        explain_lines.append(f"After % mods: {currency} {subtotal:.2f}")

    abs_total = 0.0
    for a in absolute_mods:
        abs_add = a * (hours if apply_abs_per_hour else 1.0)
        abs_total += abs_add
        explain_lines.append(f"Absolute modifier +{currency} {a:.2f}" + (" per hour" if apply_abs_per_hour else ""))

    total = subtotal + abs_total
    if absolute_mods:
        explain_lines.append(f"After absolute mods (+{currency} {abs_total:.2f}): {currency} {total:.2f}")

    return {
        "currency": currency,
        "base_price": base,
        "hours": hours,
        "percent_mods": percent_mods,
        "absolute_mods": absolute_mods,
        "total": round(total, 2),
        "explain": " | ".join(explain_lines)
    }

# ------------------------------ Link Crawler -------------------------------

def discover_product_links(html: str, base_url: str) -> List[str]:
    soup = get_soup(html)
    host = urlparse(base_url).netloc
    out: List[str] = []
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if href.startswith("#"):
            continue
        abs_url = urljoin(base_url, href)
        if urlparse(abs_url).netloc != host:
            continue
        if "/games/" in abs_url or "/battlefield-6" in abs_url:
            out.append(abs_url)
    # de-dup
    seen = set()
    deduped = []
    for u in out:
        if u not in seen:
            seen.add(u)
            deduped.append(u)
    return deduped

# --------------------------------- CLI -------------------------------------

def main():
    ap = argparse.ArgumentParser(description="Selenium scraper for Overgear-like product pages.")
    ap.add_argument("urls", nargs="+", help="One or more URLs to scrape.")
    ap.add_argument("--crawl", action="store_true", help="Also discover and scrape product-like links from the given pages.")
    ap.add_argument("--out", default="-", help="Output file (.json or .jsonl). '-' for stdout.")
    ap.add_argument("--pretty", action="store_true", help="Pretty-print JSON (when not using JSONL).")
    ap.add_argument("--jsonl", action="store_true", help="Write one JSON object per line.")
    ap.add_argument("--apply-abs-per-hour", action="store_true", help="Treat absolute modifiers as per-hour add-ons in quotes.")
    ap.add_argument("--headless", action="store_true", default=False, help="Run Chrome headless (default off).")
    ap.add_argument("--driver-path", default=None, help="Path to ChromeDriver (if not on PATH).")
    ap.add_argument("--wait-seconds", type=int, default=25, help="Max wait seconds for page ready.")
    args = ap.parse_args()

    driver = build_chrome(headless=args.headless, driver_path=args.driver_path)
    try:
        # Collect URLs (+ crawl)
        targets: List[str] = []
        for u in args.urls:
            targets.append(u)
            if args.crawl:
                try:
                    html0 = fetch_with_selenium(driver, u, wait_seconds=args.wait_seconds)
                    more = discover_product_links(html0, u)
                    targets.extend(more)
                except Exception as e:
                    print(f"[WARN] Could not crawl from {u}: {e}", file=sys.stderr)

        # De-dup
        seen = set()
        url_list: List[str] = []
        for u in targets:
            if u not in seen:
                seen.add(u)
                url_list.append(u)

        results: List[Dict[str, Any]] = []
        for i, url in enumerate(url_list, 1):
            try:
                html = fetch_with_selenium(driver, url, wait_seconds=args.wait_seconds)
                rec = parse_product_page(html, base_url=url)

                # Optional: add a sample quote showing option handling
                try:
                    # Build default selections
                    choice_idx, hours_info = build_choice_index(rec)
                    selections: Dict[str, Any] = {}
                    if hours_info:
                        selections[hours_info["group"]] = hours_info.get("default") or 1
                    for gname, meta in choice_idx.items():
                        if meta["type"] == "radio" and meta.get("default"):
                            selections[gname] = meta["default"]
                    quote = estimate_price(rec, selections, apply_abs_per_hour=args.apply_abs_per_hour)
                    rec["_sample_quote"] = {
                        "selections": selections,
                        **quote
                    }
                except Exception:
                    pass

                results.append(rec)
                print(f"[{i}/{len(url_list)}] OK: {url}", file=sys.stderr)
            except Exception as e:
                print(f"[{i}/{len(url_list)}] ERROR: {url} -> {e}", file=sys.stderr)

        # Output
        if args.out == "-":
            if args.jsonl:
                for r in results:
                    print(json.dumps(r, ensure_ascii=False))
            else:
                print(json.dumps(results, indent=2 if args.pretty else None, ensure_ascii=False))
        else:
            if args.jsonl:
                with open(args.out, "w", encoding="utf-8") as f:
                    for r in results:
                        f.write(json.dumps(r, ensure_ascii=False) + "\n")
            else:
                with open(args.out, "w", encoding="utf-8") as f:
                    json.dump(results, f, indent=2 if args.pretty else None, ensure_ascii=False)
            print(f"Wrote {len(results)} record(s) to {args.out}", file=sys.stderr)
    finally:
        try:
            driver.quit()
        except Exception:
            pass

if __name__ == "__main__":
    main()
