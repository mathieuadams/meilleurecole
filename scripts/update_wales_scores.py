#!/usr/bin/env python3
"""
Update only English / Math / Science scores in uk_schools for Wales
from the CSV columns:
  literacy_points  -> uk_schools.english_score
  numeracy_points  -> uk_schools.math_score
  science_points   -> uk_schools.science_score

This script uses a TEMP staging table and a single SQL UPDATE .. FROM join.
It prints useful diagnostics (how many rows had values, how many matched in DB,
first few missing URNs, etc.).

Examples
--------
# Dry run to see what would be updated
python update_wales_scores.py --csv wales_schools.csv --dry-run

# Dry run for a single URN
python update_wales_scores.py --csv wales_schools.csv --only-urn 6604025 --dry-run

# Commit all Wales schools
python update_wales_scores.py --csv wales_schools.csv --country wales

# Commit without country filter (updates any matching URN)
python update_wales_scores.py --csv wales_schools.csv --no-country-filter
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
        logging.FileHandler(f"update_wales_scores_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
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
    if s == "" or s.upper() == "NULL" or s == "-" or s == "*":
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

def read_scores(csv_path: str, only_urn: Optional[str] = None, debug_urn: Optional[str] = None) -> List[Tuple[int, Optional[float], Optional[float], Optional[float]]]:
    """Read scores from Wales CSV file"""
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV file not found: {csv_path}")

    with open(csv_path, 'r', encoding='utf-8-sig', newline='') as f:
        # Detect CSV dialect
        sample = f.read(4096)
        f.seek(0)
        try:
            dialect = csv.Sniffer().sniff(sample, delimiters=",\t;|")
        except Exception:
            # Default to comma if detection fails
            class D:
                delimiter = ','
            dialect = D()
        
        reader = csv.DictReader(f, dialect=dialect)
        headers = reader.fieldnames or []
        
        # Log the actual headers found
        logging.info(f"CSV headers found: {headers[:15]}...")
        
        # Map headers (exact case matching for Wales CSV)
        urn_key = 'School_Number'  # Updated to use underscore
        eng_key = 'literacy_points'
        math_key = 'numeracy_points'
        sci_key = 'science_points'
        
        # Verify required columns exist
        if urn_key not in headers:
            raise RuntimeError(f"URN column '{urn_key}' not found in CSV headers")
        if eng_key not in headers:
            logging.warning(f"English score column '{eng_key}' not found")
        if math_key not in headers:
            logging.warning(f"Math score column '{math_key}' not found")
        if sci_key not in headers:
            logging.warning(f"Science score column '{sci_key}' not found")
        
        logging.info(f"Mapped columns: URN='{urn_key}', ENG='{eng_key}', MATH='{math_key}', SCI='{sci_key}'")
        
        # Show the column indices for debugging
        if eng_key in headers:
            logging.info(f"  '{eng_key}' is at index {headers.index(eng_key)}")
        if math_key in headers:
            logging.info(f"  '{math_key}' is at index {headers.index(math_key)}")
        if sci_key in headers:
            logging.info(f"  '{sci_key}' is at index {headers.index(sci_key)}")

        rows: List[Tuple[int, Optional[float], Optional[float], Optional[float]]] = []
        target = re.sub(r"[^0-9]", "", only_urn) if only_urn else None
        dbg = re.sub(r"[^0-9]", "", debug_urn) if debug_urn else None
        
        # Counters for statistics
        total_rows = 0
        rows_with_scores = 0
        c_eng = c_math = c_sci = 0
        
        for row_num, r in enumerate(reader, start=2):  # Start at 2 (header is row 1)
            total_rows += 1
            
            # Extract URN
            urn_raw = r.get(urn_key)
            if not urn_raw:
                logging.debug(f"Row {row_num}: No URN found, skipping")
                continue
            
            urn = re.sub(r"[^0-9]", "", str(urn_raw))
            if not urn:
                logging.debug(f"Row {row_num}: URN '{urn_raw}' has no numeric chars, skipping")
                continue
            
            # Skip if filtering by URN and doesn't match
            if target and urn != target:
                continue
            
            # Extract scores
            eng = to_float(r.get(eng_key)) if eng_key in headers else None
            math = to_float(r.get(math_key)) if math_key in headers else None
            sci = to_float(r.get(sci_key)) if sci_key in headers else None
            
            # Count non-null values
            if eng is not None:
                c_eng += 1
            if math is not None:
                c_math += 1
            if sci is not None:
                c_sci += 1
            
            # Debug specific URN if requested
            if dbg and urn == dbg:
                logging.info(f"[DEBUG URN {urn}] Row {row_num}")
                logging.info(f"  Raw values: eng='{r.get(eng_key)}' math='{r.get(math_key)}' sci='{r.get(sci_key)}'")
                logging.info(f"  Parsed: eng={eng} math={math} sci={sci}")
            
            # Keep only rows with at least one score present
            if eng is None and math is None and sci is None:
                continue
            
            rows_with_scores += 1
            rows.append((int(urn), eng, math, sci))
        
        logging.info(f"CSV processing complete:")
        logging.info(f"  Total rows read: {total_rows}")
        logging.info(f"  Rows with at least one score: {rows_with_scores}")
        logging.info(f"  Non-null counts → english:{c_eng} math:{c_math} science:{c_sci}")
        
    return rows

# ----------------------------
# Main update function
# ----------------------------

def main():
    ap = argparse.ArgumentParser(description="Update uk_schools scores from Wales CSV")
    ap.add_argument('--db', default=DEFAULT_DB, help='Postgres connection URL')
    ap.add_argument('--csv', required=True, help='Path to Wales CSV file (e.g., wales_schools.csv)')
    ap.add_argument('--only-urn', default=None, help='Update only a single URN (e.g., 6604025)')
    ap.add_argument('--country', default='wales', help='Require s.country to match (case-insensitive). Default: wales')
    ap.add_argument('--no-country-filter', action='store_true', help='Do not filter by country (update any matching URN)')
    ap.add_argument('--dry-run', action='store_true', help='Show what would be updated without committing changes')
    ap.add_argument('--debug-urn', default=None, help='Print detailed parsing info for this URN')
    args = ap.parse_args()

    # Read data from CSV
    logging.info(f"Reading scores from CSV: {args.csv}")
    staging = read_scores(args.csv, only_urn=args.only_urn, debug_urn=args.debug_urn)
    
    if not staging:
        logging.warning("No rows with scores found in CSV. Nothing to update.")
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
            cur.execute("DROP TABLE IF EXISTS tmp_wales_scores")
            cur.execute(
                """
                CREATE TEMP TABLE tmp_wales_scores (
                  urn BIGINT PRIMARY KEY,
                  english_score NUMERIC,
                  math_score NUMERIC,
                  science_score NUMERIC
                ) ON COMMIT DROP
                """
            )
            
            # Insert data into staging table
            logging.info("Loading data into staging table...")
            execute_values(
                cur,
                "INSERT INTO tmp_wales_scores (urn, english_score, math_score, science_score) VALUES %s",
                staging,
                page_size=1000,
            )

            # Diagnostics: count staged rows and check for missing URNs
            cur.execute("SELECT COUNT(*) FROM tmp_wales_scores")
            staged_count = cur.fetchone()[0]
            
            cur.execute(
                """
                SELECT COUNT(*)
                FROM tmp_wales_scores t
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
                    SELECT t.urn FROM tmp_wales_scores t
                    LEFT JOIN uk_schools s ON s.urn = t.urn
                    WHERE s.urn IS NULL
                    ORDER BY t.urn
                    LIMIT 10
                    """
                )
                sample_missing = [str(r[0]) for r in cur.fetchall()]
                logging.warning(f"Sample of missing URNs (first 10): {', '.join(sample_missing)}")

            # Build UPDATE query
            where_conditions = ["s.urn = t.urn"]
            where_conditions.append("(t.english_score IS NOT NULL OR t.math_score IS NOT NULL OR t.science_score IS NOT NULL)")
            params = []
            
            if not args.no_country_filter:
                where_conditions.append("LOWER(s.country) = LOWER(%s)")
                params.append(args.country)
            
            if args.only_urn:
                where_conditions.append("s.urn = %s")
                params.append(int(re.sub(r"[^0-9]", "", args.only_urn)))
            
            where_clause = " AND ".join(where_conditions)
            
            # For dry run, count how many rows would be updated
            if args.dry_run:
                count_sql = f"""
                    SELECT COUNT(*)
                    FROM uk_schools s
                    JOIN tmp_wales_scores t ON s.urn = t.urn
                    WHERE {where_clause}
                """
                cur.execute(count_sql, params)
                would_update = cur.fetchone()[0]
                
                # Show sample of what would be updated
                sample_sql = f"""
                    SELECT s.urn, s.name,
                           s.english_score AS old_eng, t.english_score AS new_eng,
                           s.math_score AS old_math, t.math_score AS new_math,
                           s.science_score AS old_sci, t.science_score AS new_sci
                    FROM uk_schools s
                    JOIN tmp_wales_scores t ON s.urn = t.urn
                    WHERE {where_clause}
                    LIMIT 5
                """
                cur.execute(sample_sql, params)
                samples = cur.fetchall()
                
                logging.info(f"\n{'='*60}")
                logging.info(f"DRY RUN - Would update {would_update} rows")
                logging.info(f"{'='*60}")
                
                if samples:
                    logging.info("\nSample of changes (first 5):")
                    for s in samples:
                        logging.info(f"\nURN {s[0]} - {s[1]}:")
                        if s[3] is not None:
                            logging.info(f"  English: {s[2]} → {s[3]}")
                        if s[5] is not None:
                            logging.info(f"  Math:    {s[4]} → {s[5]}")
                        if s[7] is not None:
                            logging.info(f"  Science: {s[6]} → {s[7]}")
                
                conn.rollback()
                logging.info("\nDry run complete. No changes made to database.")
                return

            # Perform the actual update
            update_sql = f"""
                UPDATE uk_schools AS s
                SET english_score = COALESCE(t.english_score, s.english_score),
                    math_score    = COALESCE(t.math_score, s.math_score),
                    science_score = COALESCE(t.science_score, s.science_score),
                    updated_at    = NOW()
                FROM tmp_wales_scores t
                WHERE {where_clause}
            """
            
            logging.info("Executing update...")
            cur.execute(update_sql, params)
            updated_count = cur.rowcount
            
            # Commit the transaction
            conn.commit()
            
            logging.info(f"\n{'='*60}")
            logging.info(f"SUCCESS - Updated {updated_count} rows in uk_schools")
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