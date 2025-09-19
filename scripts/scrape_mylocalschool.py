#!/usr/bin/env python3
"""
My Local School scraper (v9): Simplified direct extraction
- Uses the tile structure from XPath locations
- Direct value extraction without complex logic

Usage:
  pip install playwright beautifulsoup4 lxml
  playwright install chromium
  xvfb-run -a python scrape_mylocalschool_v9.py --school 6754011 --out school_6754011.json
"""
from __future__ import annotations
import argparse, json, re
from pathlib import Path
from typing import Dict, Any, Optional
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

BASE = "https://mylocalschool.gov.wales/School/{school}?lang=en"

NUM_RE = re.compile(r"[-+]?\d{1,3}(?:,\d{3})*(?:\.\d+)?")
PCT_RE = re.compile(r"([-+]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)\s*%")
CURR_RE = re.compile(r"£\s*([\d,]+(?:\.\d+)?)")
POSTCODE_RE = re.compile(r"([A-Z]{1,2}\d{1,2}[A-Z]?\s*\d[ABD-HJLNP-UW-Z]{2})", re.I)

def _clean(s:str)->str: 
    return re.sub(r"\s+"," ",s.strip())

def _extract_tile_main_value(tile_elem):
    """Extract the main numeric value from a tile element."""
    if not tile_elem:
        return None
    
    text = _clean(tile_elem.get_text(" "))
    
    # Remove common label words to isolate the value
    labels_to_remove = [
        'Number of Pupils', 'Free school meals', 'FSM', '3 year average',
        'Pupil Teacher Ratio', 'PTR', 'Secondary', 'Attendance during the year',
        'School budget per pupil', 'Capped 9 points score', 'interim measures version',
        'Literacy points score', 'Numeracy points score', 'Science points score',
        'Welsh Baccalaureate Skills Challenge Certificate points score'
    ]
    
    # Remove labels from text to help isolate values
    value_text = text
    for label in labels_to_remove:
        value_text = value_text.replace(label, '')
    
    # Look for currency first
    curr_match = CURR_RE.search(value_text)
    if curr_match:
        try:
            return float(curr_match.group(1).replace(",",""))
        except:
            pass
    
    # Look for percentages
    pct_matches = PCT_RE.findall(value_text)
    if pct_matches:
        # Return the first valid percentage
        for pct in pct_matches:
            try:
                val = float(pct.replace(",",""))
                # Skip years
                if 2000 <= val <= 2030:
                    continue
                return val
            except:
                pass
    
    # Look for regular numbers
    num_matches = NUM_RE.findall(value_text)
    if num_matches:
        for num in num_matches:
            try:
                val = float(num.replace(",",""))
                # Skip years
                if 2000 <= val <= 2030:
                    continue
                return val
            except:
                pass
    
    return None

def scrape_one(school_no:str, headless:bool=True, timeout_ms:int=45000)->Dict[str,Any]:
    url = BASE.format(school=school_no)
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        page = browser.new_page()
        page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
        page.wait_for_timeout(2000)
        html = page.content()
        browser.close()
    
    soup = BeautifulSoup(html, "lxml")
    out = {"school_no": str(school_no), "source": url}
    
    # Get school name
    h1 = soup.find(["h1", "h2"], string=True)
    if h1: 
        out["name"] = _clean(h1.get_text(" "))
    
    # Find main section
    main = soup.find("main")
    if not main:
        main = soup
    
    # Get all top-level divs under section
    section = main.find("section")
    if section:
        # Get all direct child divs
        top_divs = section.find_all("div", recursive=False)
        
        # Basic Details Section (3rd div, 1st child, 1st child)
        if len(top_divs) >= 3:
            basic_container = top_divs[2]  # 3rd div (0-indexed)
            basic_row = basic_container.find("div")
            if basic_row:
                basic_section = basic_row.find("div")
                if basic_section:
                    # Get all info items
                    info_items = basic_section.find_all("div", recursive=False)
                    for item in info_items:
                        item_divs = item.find_all("div", recursive=False)
                        if len(item_divs) >= 2:
                            label = _clean(item_divs[0].get_text(" ")).lower()
                            value = _clean(item_divs[1].get_text(" "))
                            
                            if "local authority" in label:
                                out["local_authority"] = value
                            elif "type" in label:
                                out["type"] = value
                            elif "gender mix" in label:
                                out["gender_mix"] = value
                            elif "language" in label:
                                out["language"] = value
                
                # Contact section (3rd child of basic_row)
                contact_divs = basic_row.find_all("div", recursive=False)
                if len(contact_divs) >= 3:
                    contact_section = contact_divs[2]
                    # Find telephone
                    tel_link = contact_section.find('a', href=re.compile(r'^tel:'))
                    if tel_link:
                        tel_text = tel_link.get_text() or tel_link.get('href', '')
                        digits = re.sub(r"\D+", "", tel_text)
                        if digits:
                            out["telephone"] = digits
        
        # Statistics Section (4th div, 1st child, 1st child)
        if len(top_divs) >= 4:
            stats_container = top_divs[3]  # 4th div (0-indexed)
            stats_row = stats_container.find("div")
            if stats_row:
                stats_grid = stats_row.find("div")
                if stats_grid:
                    # Get all tile containers
                    tiles = stats_grid.find_all("div", recursive=False)
                    
                    # Map tiles by position (they appear in order)
                    tile_mapping = [
                        ("number_of_pupils", 0),
                        ("fsm_3yr_pct", 1),
                        ("pupil_teacher_ratio", 2),
                        ("attendance_pct", 3),
                        ("school_budget_per_pupil", 4),
                        ("capped9_points", 5),
                        ("literacy_points", 6),
                        ("numeracy_points", 7),
                        ("science_points", 8),
                        ("welsh_bacc_points", 9)
                    ]
                    
                    for field_name, tile_index in tile_mapping:
                        if tile_index < len(tiles):
                            tile = tiles[tile_index]
                            # Get the first child div which contains the value
                            value_div = tile.find("div")
                            if value_div:
                                value = _extract_tile_main_value(value_div)
                                if value is not None:
                                    # Convert to int for pupil count
                                    if field_name == "number_of_pupils" and isinstance(value, float):
                                        value = int(value)
                                    out[field_name] = value
    
    # Find Estyn report URL
    for a in soup.select("a[href]"):
        href = a.get("href", "").strip()
        if "estyn" in href.lower():
            if href.startswith("http"):
                out["estyn_report_url"] = href
                break
    
    return out

if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Scrape mylocalschool.gov.wales (v9)")
    ap.add_argument("--school", required=True)
    ap.add_argument("--out")
    ap.add_argument("--show", action="store_true")
    a = ap.parse_args()
    
    rec = scrape_one(a.school, headless=not a.show)
    
    if a.out:
        Path(a.out).write_text(json.dumps(rec, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"Wrote {a.out}")
    else:
        print(json.dumps(rec, ensure_ascii=False, indent=2))