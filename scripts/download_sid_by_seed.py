#!/usr/bin/env python3
"""
Download all "Download data" files for a Scottish school by SEED from the
School Information Dashboards (Primary/Secondary/Special).

Improvements vs v1:
- Resilient selectors for the Local Authority / School selectize inputs
- Accept cookie banners if present
- Case-insensitive detection of download controls (buttons/links)
- Debug mode: screenshots + HTML dumps after each step
- Verifies that the selected school name appears on page before downloading

Usage
-----
pip install playwright requests
python -m playwright install chromium

# Primary example
python download_sid_by_seed_v2.py --seed 8212627 --sector primary --out sid_primary --show --debug

# Secondary (headless)
python download_sid_by_seed_v2.py --seed 8212627 --sector secondary --out sid_secondary

Notes
-----
- Please keep concurrency modest if you batch across many schools.
- There is no public API; this automates the official download buttons.
"""

from __future__ import annotations
import argparse
import re
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Tuple, Dict

import requests
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

ARCGIS_BASE = "https://services-eu1.arcgis.com/ELpYE44CpoxrJqcU/ArcGIS/rest/services/Schools_Scotland__2022/FeatureServer/0/query"

DASHBOARD_URLS = {
    "secondary": "https://scotland.shinyapps.io/sg-secondary_school_information_dashboard/",
    "primary":   "https://scotland.shinyapps.io/sg-primary_school_information_dashboard/",
    "special":   "https://scotland.shinyapps.io/sg-special_school_information_dashboard/",
}

LABEL_LA = ["Local Authority", "Local authority", "Select local authority", "Choose local authority"]
LABEL_SCHOOL = ["School", "School name", "Establishment", "Select school", "Choose school"]

@dataclass
class School:
    seed: str
    school_name: str
    la_name: str

def normalize_seed(x) -> str:
    try:
        return str(int(float(str(x).strip()))).zfill(7)
    except Exception:
        return str(x).strip()

def resolve_school(seed: str) -> School:
    seed_int = int(str(seed))
    params = {
        "f": "json",
        "returnGeometry": "false",
        "where": f"SeedCode={seed_int}",
        "outFields": "SchoolName,SeedCode,LAName"
    }
    r = requests.get(ARCGIS_BASE, params=params, timeout=30)
    r.raise_for_status()
    js = r.json()
    feats = js.get("features") or []
    if not feats:
        raise RuntimeError(f"No school found for SEED {seed}")
    a = feats[0]["attributes"]
    return School(
        seed=normalize_seed(a.get("SeedCode")),
        school_name=str(a.get("SchoolName") or ""),
        la_name=str(a.get("LAName") or ""),
    )

def safe_name(s: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", s).strip("_") if s else "unknown"

def debug_dump(page, debug_dir: Path, step: str):
    debug_dir.mkdir(parents=True, exist_ok=True)
    ts = int(time.time())
    png = debug_dir / f"{ts}_{safe_name(step)}.png"
    html = debug_dir / f"{ts}_{safe_name(step)}.html"
    try:
        page.screenshot(path=str(png), full_page=True)
    except Exception:
        pass
    try:
        html.write_text(page.content(), encoding="utf-8")
    except Exception:
        pass

def find_selectize_input(page, label_variants: List[str]):
    # Try label-based XPath first
    for label in label_variants:
        try:
            xpath = (
                "xpath=//label[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), "
                f"'{label.lower()}')]/following::*[contains(@class,'selectize-control')]//input[not(@type) or @type='text'][1]"
            )
            el = page.locator(xpath).first
            if el.count() > 0:
                return el
        except Exception:
            continue
    # Fallback: any selectize input on page
    try:
        el = page.locator("css=.selectize-input input[type='text']").first
        if el.count() > 0:
            return el
    except Exception:
        pass
    return None

def select_value_in_selectize(page, label_variants: List[str], value: str, debug_dir: Optional[Path] = None):
    inp = find_selectize_input(page, label_variants)
    if not inp:
        raise RuntimeError(f"Could not find selectize input for labels: {label_variants}")
    inp.click()
    inp.fill("")  # clear
    inp.type(value)
    page.keyboard.press("Enter")
    # Wait a moment and verify selected text appears in the control
    time.sleep(0.6)
    # Verify selection visibly present
    container = inp.locator("xpath=ancestor::*[contains(@class,'selectize-control')][1]")
    # Selected items are usually '.item' inside the control
    if container.locator(f".item:has-text('{value}')").count() == 0:
        # try again with first dropdown option
        page.keyboard.press("ArrowDown")
        page.keyboard.press("Enter")
        time.sleep(0.6)
    if debug_dir is not None:
        debug_dump(page, debug_dir, f"after_select_{safe_name(value)}")

def click_cookie_banners(page):
    # Try common cookie buttons
    candidates = [
        "button:has-text('Accept all')",
        "button:has-text('Accept All')",
        "button:has-text('Accept')",
        "text=Accept all cookies",
        "text=Accept cookies",
    ]
    for sel in candidates:
        try:
            el = page.locator(sel).first
            if el.count() > 0:
                el.click(timeout=2000)
                time.sleep(0.3)
        except Exception:
            continue

def collect_download_controls(page):
    # Find any 'Download' controls (buttons or links) case-insensitively
    locs = [
        "role=button[name=/download/i]",
        "css=a.shiny-download-link",
        "css=button.shiny-download-link",
        "css=.btn:has-text('Download')",
        "css=.btn:has-text('download')",
        "text=/download data/i",
        "text=/download/i",
    ]
    handles = []
    for sel in locs:
        try:
            L = page.locator(sel)
            n = L.count()
            for i in range(n):
                handles.append(L.nth(i))
        except Exception:
            continue
    # Deduplicate by DOM element handle (best-effort: use unique text+index)
    uniq = []
    seen = set()
    for i, h in enumerate(handles):
        try:
            txt = h.inner_text(timeout=1000)
        except Exception:
            txt = f"el_{i}"
        key = (txt, i)
        if key not in seen:
            seen.add(key)
            uniq.append(h)
    return uniq

def main():
    ap = argparse.ArgumentParser(description="Download all 'Download data' files from Scotland SID for one SEED.")
    ap.add_argument("--seed", required=True, help="SEED (7 digits)")
    ap.add_argument("--sector", choices=["primary","secondary","special"], default="secondary")
    ap.add_argument("--out", default="sid_downloads", help="Output folder")
    ap.add_argument("--show", action="store_true", help="Show the browser (headful)")
    ap.add_argument("--debug", action="store_true", help="Save screenshots/HTML dumps to out/_debug/")
    ap.add_argument("--min-wait", type=float, default=1.0, help="Seconds to wait after selections")
    ap.add_argument("--between", type=float, default=0.8, help="Seconds to wait between downloads")
    args = ap.parse_args()

    school = resolve_school(args.seed)
    outdir = Path(args.out).resolve()
    outdir.mkdir(parents=True, exist_ok=True)
    debug_dir = outdir / "_debug" if args.debug else None

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=not args.show)
        ctx = browser.new_context(accept_downloads=True)
        page = ctx.new_page()

        url = DASHBOARD_URLS[args.sector]
        page.goto(url, wait_until="domcontentloaded", timeout=120_000)
        click_cookie_banners(page)
        page.wait_for_load_state("networkidle", timeout=120_000)
        if debug_dir: debug_dump(page, debug_dir, "loaded")

        # Select LA, then School
        select_value_in_selectize(page, LABEL_LA, school.la_name, debug_dir)
        time.sleep(0.3)
        select_value_in_selectize(page, LABEL_SCHOOL, school.school_name, debug_dir)

        # Let the page react
        time.sleep(args.min_wait)
        if debug_dir: debug_dump(page, debug_dir, "after_selections")

        # Verify the school name appears somewhere on the page (header/card)
        if page.get_by_text(re.compile(re.escape(school.school_name), re.I)).count() == 0:
            print("WARN: Could not confirm that the dashboard updated to the selected school.", file=sys.stderr)

        # Collect download controls
        ctrls = collect_download_controls(page)
        if debug_dir:
            (debug_dir / "controls_count.txt").write_text(f"{len(ctrls)} controls found\n", encoding="utf-8")

        base_folder = outdir / f"{school.seed}-{safe_name(school.school_name)}"
        base_folder.mkdir(parents=True, exist_ok=True)

        downloaded = 0
        for i, ctrl in enumerate(ctrls, start=1):
            try:
                with page.expect_download(timeout=90_000) as dl_info:
                    ctrl.click()
                dl = dl_info.value
                suggested = dl.suggested_filename or f"download_{i}.xlsx"
                target = base_folder / safe_name(suggested)
                if target.exists():
                    # skip existing
                    continue
                dl.save_as(str(target))
                downloaded += 1
                time.sleep(args.between)
            except PWTimeout:
                # No download triggered—skip
                continue
            except Exception:
                # Keep going
                continue

        print(f"Downloaded {downloaded} file(s) for {school.school_name} ({school.la_name}) -> {base_folder}")
        if debug_dir: debug_dump(page, debug_dir, "done")

        ctx.close()
        browser.close()

if __name__ == "__main__":
    main()
