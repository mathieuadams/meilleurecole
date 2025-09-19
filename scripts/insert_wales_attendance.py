#!/usr/bin/env python3
"""
Insert Wales schools attendance data into uk_absence_data table.
This script reads attendance data from wales_schools_updated.csv and inserts it
into the uk_absence_data table.

The script maps:
  - School_Number (URN) -> uk_absence_data.urn
  - attendance_pct -> calculates overall_absence_rate (100 - attendance_pct)
  - persistent_absence_rate -> uk_absence_data.persistent_absence_rate (if available)
  - academic_year -> uk_absence_data.academic_year (stored as string)

Examples
--------
# Dry run to see what would be inserted
python insert_wales_attendance.py --csv wales_schools_updated.csv --dry-run

# Insert all Wales schools attendance data
python insert_wales_attendance.py --csv wales_schools_updated.csv --year 2024

# Insert with LA code filter
python insert_wales_attendance.py --csv wales_schools_updated.csv --year 2024 --la-code 660
"""
import argparse
import csv
import logging
import os
import re
from datetime import datetime
from typing import List, Tuple, Optional, Dict

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
        logging.FileHandler(f"insert_wales_attendance_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
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

def to_int(val: Optional[str]) -> Optional[int]:
    """Convert string to integer"""
    if val is None:
        return None
    s = str(val).strip()
    if s == "" or s.upper() == "NULL" or s == "-":
        return None
    # Extract digits only
    m = re.search(r"\d+", s)
    return int(m.group()) if m else None

# ----------------------------
# CSV → staging rows
# ----------------------------

def read_attendance_data(csv_path: str, academic_year: str, only_urn: Optional[str] = None, 
                        la_code: Optional[str] = None) -> List[Dict]:
    """Read attendance data from Wales CSV file"""
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV file not found: {csv_path}")

    rows = []
    
    with open(csv_path, 'r', encoding='utf-8-sig', newline='') as f:
        reader = csv.DictReader(f, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        headers = reader.fieldnames or []
        
        logging.info(f"Total columns found: {len(headers)}")
        logging.info(f"Headers: {headers}")
        
        # Map column names (handle variations)
        urn_key = 'School_Number' if 'School_Number' in headers else 'URN'
        la_key = 'LA_Code' if 'LA_Code' in headers else 'la_code'
        
        # Attendance columns - check what's available
        attendance_key = None
        absence_key = None
        persistent_key = None
        
        for h in headers:
            h_lower = h.lower()
            if 'attendance_pct' in h_lower or 'attendance_rate' in h_lower:
                attendance_key = h
            elif 'overall_absence' in h_lower:
                absence_key = h
            elif 'persistent_absence' in h_lower or 'persistent_absentee' in h_lower:
                persistent_key = h
        
        # If we have attendance % but not absence %, we'll calculate it
        if attendance_key and not absence_key:
            logging.info(f"Will calculate absence rate from attendance column: {attendance_key}")
        
        logging.info(f"Column mapping:")
        logging.info(f"  URN: {urn_key}")
        logging.info(f"  LA Code: {la_key}")
        logging.info(f"  Attendance: {attendance_key}")
        logging.info(f"  Overall Absence: {absence_key}")
        logging.info(f"  Persistent Absence: {persistent_key}")
        
        if not urn_key in headers:
            raise RuntimeError(f"URN column not found. Available columns: {headers}")
        
        # Process rows
        total_rows = 0
        rows_with_data = 0
        
        for row_num, row in enumerate(reader, start=2):
            total_rows += 1
            
            # Extract URN
            urn_raw = row.get(urn_key)
            if not urn_raw:
                continue
            
            urn = re.sub(r"[^0-9]", "", str(urn_raw))
            if not urn:
                continue
            
            # Apply filters if specified
            if only_urn and urn != re.sub(r"[^0-9]", "", only_urn):
                continue
            
            if la_code and la_key in headers:
                row_la = str(row.get(la_key, '')).strip()
                if row_la != str(la_code):
                    continue
            
            # Extract absence data
            overall_absence = None
            persistent_absence = None
            
            # If we have direct absence rate, use it
            if absence_key:
                overall_absence = to_float(row.get(absence_key))
            # Otherwise calculate from attendance if available
            elif attendance_key:
                attendance = to_float(row.get(attendance_key))
                if attendance is not None:
                    # Check if value might be in wrong scale (e.g., 8026 instead of 80.26)
                    if attendance > 100:
                        # Try dividing by 100 if it makes sense
                        if attendance <= 10000:  # Reasonable upper bound for this conversion
                            original = attendance
                            attendance = attendance / 100.0
                            logging.debug(f"Row {row_num} URN {urn}: Converted attendance from {original} to {attendance}%")
                        else:
                            logging.warning(f"Row {row_num} URN {urn}: Invalid attendance {attendance}, skipping")
                            continue
                    
                    # Final validation
                    if attendance < 0 or attendance > 100:
                        logging.warning(f"Row {row_num} URN {urn}: Invalid attendance {attendance}%, skipping")
                        continue
                    
                    overall_absence = 100.0 - attendance
            
            # Get persistent absence if available
            if persistent_key:
                persistent_absence = to_float(row.get(persistent_key))
                # Validate persistent absence
                if persistent_absence is not None and (persistent_absence < 0 or persistent_absence > 100):
                    logging.warning(f"Row {row_num} URN {urn}: Invalid persistent absence {persistent_absence}%, setting to None")
                    persistent_absence = None
            
            # Validate calculated absence rate
            if overall_absence is not None:
                if overall_absence < 0:
                    logging.warning(f"Row {row_num} URN {urn}: Negative absence rate {overall_absence}%, setting to 0")
                    overall_absence = 0.0
                elif overall_absence > 100:
                    logging.warning(f"Row {row_num} URN {urn}: Absence rate {overall_absence}% > 100%, setting to 100")
                    overall_absence = 100.0
            
            # Only include rows with at least overall absence data
            if overall_absence is not None:
                rows_with_data += 1
                rows.append({
                    'urn': int(urn),
                    'la_code': to_int(row.get(la_key)) if la_key in headers else None,
                    'estab_number': None,  # Will be fetched from uk_schools if needed
                    'overall_absence_rate': round(overall_absence, 2) if overall_absence is not None else None,
                    'persistent_absence_rate': round(persistent_absence, 2) if persistent_absence is not None else None,
                    'academic_year': academic_year  # Already a string
                })
        
        logging.info(f"CSV processing complete:")
        logging.info(f"  Total rows read: {total_rows}")
        logging.info(f"  Rows with attendance/absence data: {rows_with_data}")
        
    return rows

# ----------------------------
# Main insert function
# ----------------------------

def main():
    ap = argparse.ArgumentParser(description="Insert Wales schools attendance data into uk_absence_data")
    ap.add_argument('--db', default=DEFAULT_DB, help='Postgres connection URL')
    ap.add_argument('--csv', required=True, help='Path to Wales CSV file with attendance data')
    ap.add_argument('--year', type=str, default='2024', help='Academic year (e.g., 2024 for 2024-25)')
    ap.add_argument('--only-urn', default=None, help='Process only a single URN')
    ap.add_argument('--la-code', default=None, help='Filter by LA code')
    ap.add_argument('--dry-run', action='store_true', help='Show what would be inserted without committing')
    ap.add_argument('--update-existing', action='store_true', help='Update existing records instead of skipping')
    args = ap.parse_args()

    # Ensure year is a string (for database compatibility)
    year_str = str(args.year)
    
    # Read data from CSV
    logging.info(f"Reading attendance data from: {args.csv}")
    logging.info(f"Academic year: {year_str}")
    
    data = read_attendance_data(args.csv, year_str, only_urn=args.only_urn, la_code=args.la_code)
    
    if not data:
        logging.warning("No attendance data found in CSV. Nothing to insert.")
        return

    logging.info(f"Prepared {len(data)} rows for insertion")

    # Connect to database
    logging.info("Connecting to database...")
    conn = psycopg2.connect(args.db)
    conn.autocommit = False
    
    try:
        with conn.cursor() as cur:
            # First, check which URNs already exist in uk_absence_data for this year
            urns = [d['urn'] for d in data]
            cur.execute(
                """
                SELECT urn 
                FROM uk_absence_data 
                WHERE urn = ANY(%s) AND academic_year = %s
                """,
                (urns, year_str)  # Use string for year
            )
            existing_urns = set(row[0] for row in cur.fetchall())
            
            if existing_urns and not args.update_existing:
                logging.warning(f"Found {len(existing_urns)} URNs already in uk_absence_data for year {year_str}")
                logging.info("Use --update-existing flag to update these records")
            
            # Get LA codes and establishment numbers from uk_schools if needed
            logging.info("Fetching additional school data from uk_schools...")
            cur.execute(
                """
                SELECT urn, la_code, establishment_number 
                FROM uk_schools 
                WHERE urn = ANY(%s)
                """,
                (urns,)
            )
            school_info = {row[0]: {'la_code': row[1], 'estab_number': row[2]} 
                          for row in cur.fetchall()}
            
            # Check for missing schools
            missing_schools = set(urns) - set(school_info.keys())
            if missing_schools:
                logging.warning(f"Found {len(missing_schools)} URNs not in uk_schools table")
                sample = list(missing_schools)[:5]
                logging.warning(f"Sample missing URNs: {sample}")
            
            # Prepare data for insertion
            insert_data = []
            update_data = []
            
            for row in data:
                urn = row['urn']
                
                # Skip if school not in uk_schools
                if urn not in school_info:
                    continue
                
                # Use LA code and estab from uk_schools if not in CSV
                if row['la_code'] is None:
                    row['la_code'] = school_info[urn]['la_code']
                row['estab_number'] = school_info[urn]['estab_number']
                
                if urn in existing_urns:
                    if args.update_existing:
                        update_data.append(row)
                else:
                    insert_data.append(row)
            
            if args.dry_run:
                logging.info(f"\n{'='*60}")
                logging.info(f"DRY RUN - Would insert {len(insert_data)} new records")
                if args.update_existing:
                    logging.info(f"DRY RUN - Would update {len(update_data)} existing records")
                logging.info(f"{'='*60}")
                
                # Show sample of what would be inserted
                if insert_data:
                    logging.info("\nSample of new records (first 5):")
                    for row in insert_data[:5]:
                        logging.info(f"  URN {row['urn']}: absence={row['overall_absence_rate']}%, "
                                   f"persistent={row['persistent_absence_rate']}%")
                
                if update_data and args.update_existing:
                    logging.info("\nSample of updates (first 5):")
                    for row in update_data[:5]:
                        logging.info(f"  URN {row['urn']}: absence={row['overall_absence_rate']}%, "
                                   f"persistent={row['persistent_absence_rate']}%")
                
                conn.rollback()
                logging.info("\nDry run complete. No changes made to database.")
                return
            
            # Insert new records
            if insert_data:
                logging.info(f"Inserting {len(insert_data)} new records...")
                insert_query = """
                    INSERT INTO uk_absence_data 
                    (urn, la_code, estab_number, overall_absence_rate, persistent_absence_rate, academic_year, created_at)
                    VALUES (%(urn)s, %(la_code)s, %(estab_number)s, %(overall_absence_rate)s, 
                            %(persistent_absence_rate)s, %(academic_year)s, NOW())
                """
                cur.executemany(insert_query, insert_data)
                logging.info(f"Inserted {len(insert_data)} new records")
            
            # Update existing records if requested
            if update_data and args.update_existing:
                logging.info(f"Updating {len(update_data)} existing records...")
                update_query = """
                    UPDATE uk_absence_data
                    SET overall_absence_rate = %(overall_absence_rate)s,
                        persistent_absence_rate = %(persistent_absence_rate)s
                    WHERE urn = %(urn)s AND academic_year = %(academic_year)s
                """
                cur.executemany(update_query, update_data)
                logging.info(f"Updated {len(update_data)} records")
            
            # Commit transaction
            conn.commit()
            
            total_affected = len(insert_data) + (len(update_data) if args.update_existing else 0)
            logging.info(f"\n{'='*60}")
            logging.info(f"SUCCESS - Affected {total_affected} records in uk_absence_data")
            logging.info(f"  New inserts: {len(insert_data)}")
            if args.update_existing:
                logging.info(f"  Updates: {len(update_data)}")
            logging.info(f"{'='*60}")

    except Exception as e:
        conn.rollback()
        logging.error(f"Error during operation: {e}")
        raise
    finally:
        conn.close()
        logging.info("Database connection closed.")

if __name__ == "__main__":
    main()