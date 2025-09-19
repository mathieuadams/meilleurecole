#!/usr/bin/env python3
"""
Update FSM (Free School Meals) percentage in uk_schools for Wales
from the CSV column:
  fsm_3yr_pct -> uk_schools.percentage_fsm

This script uses a TEMP staging table and a single SQL UPDATE .. FROM join.

Examples
--------
# Dry run to see what would be updated
python update_wales_fsm.py --csv wales_schools_updated.csv --dry-run

# Update all Wales schools
python update_wales_fsm.py --csv wales_schools_updated.csv --country wales

# Update without country filter
python update_wales_fsm.py --csv wales_schools_updated.csv --no-country-filter
"""
import argparse
import csv
import logging
import os
import re
from datetime import datetime
from typing import List, Tuple, Optional

import psycopg2
from psycopg2.extras import execute_values

# ----------------------------
# Config / logging
# ----------------------------
DEFAULT_DB = "postgresql://schoolsdb_user:JdHohdmOIWv1dC4OzSwIgtme44QNxlbw@dpg-d2ju9ee3jp1c73fii3ag-a.frankfurt-postgres.render.com/schoolsdb"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(f"update_wales_fsm_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
        logging.StreamHandler()
    ]
)

# ----------------------------
# Helpers
# ----------------------------

def to_float(val: Optional[str]) -> Optional[float]:
    """Convert various string formats to float, handling nulls and special chars"""
    if val is None:
        return None
    s = str(val).strip().replace("\u00a0", "")  # Remove non-breaking spaces
    if s == "" or s.upper() == "NULL" or s == "-" or s == "*" or s.upper() == "N/A":
        return None
    # Handle various decimal separators and formats
    if s.count(',') == 1 and '.' not in s:
        s = s.replace(',', '.')
    else:
        s = s.replace(',', '')
    s = s.replace('%', '').replace('≈', '').replace('~', '').replace('·', '.')
    # Extract the numeric value
    m = re.search(r"-?\d+(?:\.\d+)?", s)
    return float(m.group()) if m else None

# ----------------------------
# CSV → staging rows
# ----------------------------

def read_fsm_data(csv_path: str, only_urn: Optional[str] = None, debug_urn: Optional[str] = None) -> List[Tuple[int, Optional[float]]]:
    """Read FSM percentage from Wales CSV file"""
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV file not found: {csv_path}")

    with open(csv_path, 'r', encoding='utf-8-sig', newline='') as f:
        reader = csv.DictReader(f, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        headers = reader.fieldnames or []
        
        # Log the headers found
        logging.info(f"Total columns found: {len(headers)}")
        logging.info(f"CSV headers: {headers}")
        
        # Map headers
        urn_key = 'School_Number' if 'School_Number' in headers else 'URN'
        fsm_key = 'fsm_3yr_pct' if 'fsm_3yr_pct' in headers else 'fsm_pct'
        
        # Verify required columns exist
        if urn_key not in headers:
            raise RuntimeError(f"URN column '{urn_key}' not found in CSV headers")
        if fsm_key not in headers:
            # Try alternative names
            for h in headers:
                if 'fsm' in h.lower():
                    fsm_key = h
                    break
            if fsm_key not in headers:
                raise RuntimeError(f"FSM column not found. Looked for: fsm_3yr_pct, fsm_pct, or any column with 'fsm'")
        
        logging.info(f"Column mapping: URN='{urn_key}', FSM='{fsm_key}'")
        logging.info(f"  '{fsm_key}' is at index {headers.index(fsm_key)}")

        rows: List[Tuple[int, Optional[float]]] = []
        target = re.sub(r"[^0-9]", "", only_urn) if only_urn else None
        dbg = re.sub(r"[^0-9]", "", debug_urn) if debug_urn else None
        
        # Counters
        total_rows = 0
        rows_with_fsm = 0
        fsm_count = 0
        
        for row_num, r in enumerate(reader, start=2):
            total_rows += 1
            
            # Extract URN
            urn_raw = r.get(urn_key)
            if not urn_raw:
                continue
            
            urn = re.sub(r"[^0-9]", "", str(urn_raw))
            if not urn:
                continue
            
            # Skip if filtering by URN and doesn't match
            if target and urn != target:
                continue
            
            # Extract FSM percentage
            fsm = to_float(r.get(fsm_key))
            
            # Validate and fix FSM percentage scaling
            if fsm is not None:
                # Check if value might be in wrong scale (e.g., 27161 instead of 27.161)
                if fsm > 100:
                    # These appear to need division by 1000 based on the data pattern
                    if fsm > 1000 and fsm <= 100000:
                        original = fsm
                        fsm = fsm / 1000.0
                        logging.debug(f"Row {row_num} URN {urn}: Converted FSM from {original} to {fsm:.3f}%")
                    # If still over 100 after division by 1000, try division by 100
                    elif fsm > 100 and fsm <= 1000:
                        original = fsm
                        fsm = fsm / 100.0
                        logging.debug(f"Row {row_num} URN {urn}: Converted FSM from {original} to {fsm:.2f}%")
                    else:
                        logging.warning(f"Row {row_num} URN {urn}: Cannot fix FSM value {fsm}, skipping")
                        continue
                
                # Final validation after conversion
                if fsm < 0:
                    logging.warning(f"Row {row_num} URN {urn}: Negative FSM {fsm}%, setting to 0")
                    fsm = 0.0
                elif fsm > 100:
                    logging.warning(f"Row {row_num} URN {urn}: FSM {fsm:.2f}% still > 100% after conversion, capping at 100")
                    fsm = 100.0
                
                fsm_count += 1
            
            # Debug specific URN if requested
            if dbg and urn == dbg:
                logging.info(f"[DEBUG URN {urn}] Row {row_num}")
                logging.info(f"  Raw value: fsm='{r.get(fsm_key)}'")
                logging.info(f"  Parsed: fsm={fsm}")
            
            # Keep only rows with FSM data
            if fsm is None:
                continue
            
            rows_with_fsm += 1
            rows.append((int(urn), fsm))
        
        logging.info(f"CSV processing complete:")
        logging.info(f"  Total rows read: {total_rows}")
        logging.info(f"  Rows with FSM data: {rows_with_fsm}")
        logging.info(f"  Non-null FSM values: {fsm_count}")
        
    return rows

# ----------------------------
# Main update function
# ----------------------------

def main():
    ap = argparse.ArgumentParser(description="Update uk_schools FSM percentage from Wales CSV")
    ap.add_argument('--db', default=DEFAULT_DB, help='Postgres connection URL')
    ap.add_argument('--csv', required=True, help='Path to Wales CSV file')
    ap.add_argument('--only-urn', default=None, help='Update only a single URN')
    ap.add_argument('--country', default='wales', help='Require s.country to match (case-insensitive). Default: wales')
    ap.add_argument('--no-country-filter', action='store_true', help='Do not filter by country')
    ap.add_argument('--dry-run', action='store_true', help='Show what would be updated without committing')
    ap.add_argument('--debug-urn', default=None, help='Print detailed parsing info for this URN')
    args = ap.parse_args()

    # Read data from CSV
    logging.info(f"Reading FSM data from CSV: {args.csv}")
    staging = read_fsm_data(args.csv, only_urn=args.only_urn, debug_urn=args.debug_urn)
    
    if not staging:
        logging.warning("No rows with FSM data found in CSV. Nothing to update.")
        return

    logging.info(f"Prepared {len(staging)} rows for potential update")

    # Connect to database
    logging.info("Connecting to database...")
    conn = psycopg2.connect(args.db)
    conn.autocommit = False
    
    try:
        with conn.cursor() as cur:
            # Create temporary staging table
            logging.info("Creating temporary staging table...")
            cur.execute("DROP TABLE IF EXISTS tmp_wales_fsm")
            cur.execute(
                """
                CREATE TEMP TABLE tmp_wales_fsm (
                  urn BIGINT PRIMARY KEY,
                  fsm_percentage NUMERIC
                ) ON COMMIT DROP
                """
            )
            
            # Insert data into staging table
            logging.info("Loading data into staging table...")
            execute_values(
                cur,
                "INSERT INTO tmp_wales_fsm (urn, fsm_percentage) VALUES %s",
                staging,
                page_size=1000,
            )

            # Diagnostics
            cur.execute("SELECT COUNT(*) FROM tmp_wales_fsm")
            staged_count = cur.fetchone()[0]
            
            cur.execute(
                """
                SELECT COUNT(*)
                FROM tmp_wales_fsm t
                LEFT JOIN uk_schools s ON s.urn = t.urn
                WHERE s.urn IS NULL
                """
            )
            missing_in_db = cur.fetchone()[0]
            
            logging.info(f"Staging diagnostics:")
            logging.info(f"  Rows staged: {staged_count}")
            logging.info(f"  URNs not found in uk_schools: {missing_in_db}")

            # Show sample of missing URNs if any
            if missing_in_db > 0:
                cur.execute(
                    """
                    SELECT t.urn FROM tmp_wales_fsm t
                    LEFT JOIN uk_schools s ON s.urn = t.urn
                    WHERE s.urn IS NULL
                    ORDER BY t.urn
                    LIMIT 10
                    """
                )
                sample_missing = [str(r[0]) for r in cur.fetchall()]
                logging.warning(f"Sample of missing URNs (first 10): {', '.join(sample_missing)}")

            # Build UPDATE query
            where_conditions = ["s.urn = t.urn", "t.fsm_percentage IS NOT NULL"]
            params = []
            
            if not args.no_country_filter:
                where_conditions.append("LOWER(s.country) = LOWER(%s)")
                params.append(args.country)
            
            if args.only_urn:
                where_conditions.append("s.urn = %s")
                params.append(int(re.sub(r"[^0-9]", "", args.only_urn)))
            
            where_clause = " AND ".join(where_conditions)
            
            # For dry run, show what would be updated
            if args.dry_run:
                count_sql = f"""
                    SELECT COUNT(*)
                    FROM uk_schools s
                    JOIN tmp_wales_fsm t ON s.urn = t.urn
                    WHERE {where_clause}
                """
                cur.execute(count_sql, params)
                would_update = cur.fetchone()[0]
                
                # Show sample of what would be updated
                sample_sql = f"""
                    SELECT s.urn, s.name,
                           s.percentage_fsm AS old_fsm, 
                           t.fsm_percentage AS new_fsm
                    FROM uk_schools s
                    JOIN tmp_wales_fsm t ON s.urn = t.urn
                    WHERE {where_clause}
                    LIMIT 10
                """
                cur.execute(sample_sql, params)
                samples = cur.fetchall()
                
                logging.info(f"\n{'='*60}")
                logging.info(f"DRY RUN - Would update {would_update} rows")
                logging.info(f"{'='*60}")
                
                if samples:
                    logging.info("\nSample of changes (first 10):")
                    for s in samples:
                        old_val = f"{s[2]:.1f}" if s[2] is not None else "NULL"
                        new_val = f"{s[3]:.1f}" if s[3] is not None else "NULL"
                        logging.info(f"URN {s[0]} - {s[1][:40]}...")
                        logging.info(f"  FSM%: {old_val} → {new_val}")
                
                conn.rollback()
                logging.info("\nDry run complete. No changes made to database.")
                return

            # Perform the actual update
            update_sql = f"""
                UPDATE uk_schools AS s
                SET percentage_fsm = t.fsm_percentage,
                    updated_at = NOW()
                FROM tmp_wales_fsm t
                WHERE {where_clause}
            """
            
            logging.info("Executing update...")
            cur.execute(update_sql, params)
            updated_count = cur.rowcount
            
            # Commit the transaction
            conn.commit()
            
            logging.info(f"\n{'='*60}")
            logging.info(f"SUCCESS - Updated {updated_count} rows in uk_schools")
            logging.info(f"  Field updated: percentage_fsm")
            logging.info(f"{'='*60}")

    except Exception as e:
        conn.rollback()
        logging.error(f"Error during update: {e}")
        raise
    finally:
        conn.close()
        logging.info("Database connection closed.")

if __name__ == "__main__":
    main()