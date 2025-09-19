#!/usr/bin/env python3
"""
Bulk Wales Schools Data Updater
Reads school numbers from wales_schools.csv and updates with scraped data

Usage:
  pip install playwright beautifulsoup4 lxml pandas
  playwright install chromium
  python update_wales_schools.py --input wales_schools.csv --output wales_schools_updated.csv
"""
import argparse
import json
import re
import time
import pandas as pd
from pathlib import Path
from typing import Dict, Any, Optional
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

BASE = "https://mylocalschool.gov.wales/School/{school}?lang=en"

NUM_RE = re.compile(r"[-+]?\d{1,3}(?:,\d{3})*(?:\.\d+)?")
PCT_RE = re.compile(r"([-+]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)\s*%")
CURR_RE = re.compile(r"£\s*([\d,]+(?:\.\d+)?)")

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

def scrape_school(school_no:str, browser, timeout_ms:int=45000)->Dict[str,Any]:
    """Scrape a single school's data"""
    url = BASE.format(school=school_no)
    
    try:
        page = browser.new_page()
        page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
        page.wait_for_timeout(2000)
        html = page.content()
        page.close()
        
        soup = BeautifulSoup(html, "lxml")
        out = {}
        
        # Find main section
        main = soup.find("main")
        if not main:
            main = soup
        
        # Get all top-level divs under section
        section = main.find("section")
        if section:
            # Get all direct child divs
            top_divs = section.find_all("div", recursive=False)
            
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
                            (None, 0),  # Skip number_of_pupils
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
                            if field_name and tile_index < len(tiles):
                                tile = tiles[tile_index]
                                # Get the first child div which contains the value
                                value_div = tile.find("div")
                                if value_div:
                                    value = _extract_tile_main_value(value_div)
                                    if value is not None:
                                        out[field_name] = value
        
        # Find Estyn report URL
        for a in soup.select("a[href]"):
            href = a.get("href", "").strip()
            if "estyn" in href.lower():
                if href.startswith("http"):
                    out["estyn_report_url"] = href
                    break
        
        return out
    
    except Exception as e:
        logger.error(f"Error scraping school {school_no}: {str(e)}")
        return {}

def update_schools_data(input_file: str, output_file: str, limit: Optional[int] = None, 
                       start_from: Optional[int] = None, batch_size: int = 10):
    """
    Update schools data from CSV file with scraped information
    
    Args:
        input_file: Path to input CSV file
        output_file: Path to output CSV file
        limit: Optional limit on number of schools to process
        start_from: Optional starting row index
        batch_size: Number of schools to process before saving
    """
    
    # Read the CSV file
    logger.info(f"Reading {input_file}")
    df = pd.read_csv(input_file, dtype={'School Number': str})
    
    # Fields to update
    fields_to_update = [
        "fsm_3yr_pct", "pupil_teacher_ratio", "attendance_pct", 
        "school_budget_per_pupil", "capped9_points", "literacy_points", 
        "numeracy_points", "science_points", "welsh_bacc_points", 
        "estyn_report_url"
    ]
    
    # Ensure columns exist
    for field in fields_to_update:
        if field not in df.columns:
            df[field] = None
    
    # Determine range to process
    start_idx = start_from if start_from else 0
    end_idx = min(start_idx + limit, len(df)) if limit else len(df)
    
    logger.info(f"Processing schools from index {start_idx} to {end_idx}")
    
    # Initialize browser
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        
        try:
            processed_count = 0
            for idx in range(start_idx, end_idx):
                school_no = str(df.loc[idx, 'School Number'])
                school_name = df.loc[idx, 'School Name']
                
                # Skip if already has data (optional - remove this check to re-scrape)
                if pd.notna(df.loc[idx, 'fsm_3yr_pct']):
                    logger.info(f"Skipping {school_no} - {school_name} (already has data)")
                    continue
                
                logger.info(f"Processing {idx+1}/{end_idx}: {school_no} - {school_name}")
                
                # Scrape the school
                data = scrape_school(school_no, browser)
                
                # Update dataframe with scraped data
                for field in fields_to_update:
                    if field in data:
                        df.loc[idx, field] = data[field]
                        logger.debug(f"  Updated {field}: {data[field]}")
                
                processed_count += 1
                
                # Save periodically
                if processed_count % batch_size == 0:
                    df.to_csv(output_file, index=False)
                    logger.info(f"Saved progress to {output_file} ({processed_count} schools processed)")
                
                # Small delay to be respectful to the server
                time.sleep(1)
        
        finally:
            browser.close()
    
    # Final save
    df.to_csv(output_file, index=False)
    logger.info(f"Processing complete. Saved {processed_count} schools to {output_file}")
    
    # Print summary statistics
    logger.info("\nSummary of updates:")
    for field in fields_to_update:
        non_null_count = df[field].notna().sum()
        logger.info(f"  {field}: {non_null_count} values")

def main():
    parser = argparse.ArgumentParser(description="Update Wales schools data with scraped information")
    parser.add_argument("--input", default="wales_schools.csv", 
                       help="Input CSV file (default: wales_schools.csv)")
    parser.add_argument("--output", default="wales_schools_updated.csv",
                       help="Output CSV file (default: wales_schools_updated.csv)")
    parser.add_argument("--limit", type=int, 
                       help="Limit number of schools to process")
    parser.add_argument("--start-from", type=int,
                       help="Start from row index (0-based)")
    parser.add_argument("--batch-size", type=int, default=10,
                       help="Save progress every N schools (default: 10)")
    parser.add_argument("--verbose", action="store_true",
                       help="Enable verbose logging")
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Check if input file exists
    if not Path(args.input).exists():
        logger.error(f"Input file {args.input} not found")
        return
    
    # Run the update
    update_schools_data(
        input_file=args.input,
        output_file=args.output,
        limit=args.limit,
        start_from=args.start_from,
        batch_size=args.batch_size
    )

if __name__ == "__main__":
    main()
    