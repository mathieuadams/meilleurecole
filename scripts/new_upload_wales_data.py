import csv
import psycopg2
import logging
import argparse
import os
import re
from datetime import datetime
from typing import Dict, Optional, Tuple

# --------------------------------------
# Logging
# --------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'wales_data_upload_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)

# --------------------------------------
# Helpers (robust parsing)
# --------------------------------------

def to_float(value: Optional[str]) -> Optional[float]:
    if value is None:
        return None
    s = str(value).strip().replace('\u00a0', '')  # NBSP
    if s == '' or s.upper() == 'NULL' or s == '-':
        return None
    # Accept 39.7, 39,7, 39.7%, ~39.7, 39·7
    if s.count(',') == 1 and '.' not in s:
        s = s.replace(',', '.')
    else:
        s = s.replace(',', '')
    s = s.replace('%', '').replace('≈', '').replace('~', '').replace('·', '.')
    m = re.search(r'-?\d+(?:\.\d+)?', s)
    try:
        return float(m.group()) if m else None
    except Exception:
        return None

def to_int(value: Optional[str]) -> Optional[int]:
    if value is None:
        return None
    s = str(value).strip().replace('\u00a0', '')
    if s == '' or s.upper() == 'NULL' or s == '-':
        return None
    s = s.replace(',', '')
    m = re.search(r'-?\d+', s)
    return int(m.group()) if m else None

# --------------------------------------
# CSV loader → index by URN
# --------------------------------------

def norm_header(h: str) -> str:
    return re.sub(r'[^a-z0-9]', '', (h or '').lower())

def sniff_csv(path: str):
    with open(path, 'r', encoding='utf-8', newline='') as f:
        sample = f.read(4096)
        try:
            dialect = csv.Sniffer().sniff(sample, delimiters=",\t;|")
        except Exception:
            class D: delimiter = '\t' if '\t' in sample else ','
            dialect = D()
    return dialect

def load_csv_index(csv_path: str) -> Dict[str, dict]:
    dialect = sniff_csv(csv_path)
    with open(csv_path, 'r', encoding='utf-8-sig', newline='') as f:
        reader = csv.DictReader(f, dialect=dialect)
        # build normalized header map
        headers = reader.fieldnames or []
        hmap = {norm_header(h): h for h in headers}
        # expected headers (variants allowed for KS4)
        key_map = {
            'urn': hmap.get('schoolnumber') or hmap.get('urn'),
            'pupils': hmap.get('pupilsseenotes') or hmap.get('pupils'),
            'fsm': hmap.get('fsm3yrpct'),
            'att': hmap.get('attendancepct'),
            'lit': hmap.get('literacypoints') or hmap.get('literacy'),
            'num': hmap.get('numeracypoints') or hmap.get('numeracy'),
            'sci': hmap.get('sciencepoints') or hmap.get('science'),
            'la': hmap.get('localauthority') or hmap.get('lacode')
        }
        missing = [k for k,v in key_map.items() if v is None and k in ('urn',)]
        if missing:
            raise RuntimeError(f"Missing required CSV column(s): {missing} in {headers}")

        idx: Dict[str, dict] = {}
        for r in reader:
            urn_raw = r.get(key_map['urn'])
            urn = re.sub(r'[^0-9]', '', urn_raw) if urn_raw else None
            if not urn:
                continue
            idx[urn] = {
                'pupils': to_int(r.get(key_map['pupils'])) if key_map['pupils'] else None,
                'fsm': to_float(r.get(key_map['fsm'])) if key_map['fsm'] else None,
                'lit': to_float(r.get(key_map['lit'])) if key_map['lit'] else None,
                'num': to_float(r.get(key_map['num'])) if key_map['num'] else None,
                'sci': to_float(r.get(key_map['sci'])) if key_map['sci'] else None,
                'att': to_float(r.get(key_map['att'])) if key_map['att'] else None,
                'la': (r.get(key_map['la']) or '').strip() if key_map['la'] else None,
            }
        logging.info(f"CSV index built: {len(idx)} URNs")
        return idx

# --------------------------------------
# Absence schema detection + UPSERT (works for either URN or school_id schema)
# --------------------------------------

def detect_absence_schema(cur) -> Tuple[str, set]:
    cur.execute("""
        SELECT column_name FROM information_schema.columns
        WHERE table_name = 'uk_absence_data'
    """)
    cols = {r[0] for r in cur.fetchall()}
    if 'urn' in cols:
        return 'URN', cols
    elif 'school_id' in cols:
        return 'SCHOOL_ID', cols
    else:
        return 'UNKNOWN', cols

# --------------------------------------
# Uploader (DB-driven, row-by-row with per-URN logging)
# --------------------------------------

class WalesSchoolDataUploader:
    def __init__(self, db_url: str, academic_year: str = '2023/24'):
        self.db_url = db_url
        self.academic_year = academic_year
        self.conn = None
        self.cursor = None
        # counters
        self.schools_processed = 0
        self.schools_updated = 0
        self.schools_not_found = 0
        self.attendance_records_inserted = 0
        self.schools_with_scores = 0

    def connect_db(self) -> bool:
        try:
            self.conn = psycopg2.connect(self.db_url)
            self.cursor = self.conn.cursor()
            logging.info("Database connection established")
            return True
        except Exception as e:
            logging.error(f"Database connection failed: {e}")
            return False

    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        logging.info("Database connection closed")

    def ensure_absence_table_exists(self):
        """Create table only if missing; prefer URN-based schema to match your FK."""
        try:
            self.cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables WHERE table_name='uk_absence_data'
                )
            """)
            exists = self.cursor.fetchone()[0]
            if not exists:
                logging.info("Creating uk_absence_data (URN-based)…")
                self.cursor.execute("""
                    CREATE TABLE uk_absence_data (
                        id SERIAL PRIMARY KEY,
                        urn BIGINT REFERENCES uk_schools(urn),
                        la_code INTEGER,
                        estab_number INTEGER,
                        overall_absence_rate NUMERIC(5,2),
                        persistent_absence_rate NUMERIC(5,2),
                        academic_year TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP
                    )
                """)
                # Helpful composite index for upserts/joins
                self.cursor.execute("CREATE INDEX ON uk_absence_data (urn, academic_year)")
                self.conn.commit()
                logging.info("Created uk_absence_data table")
            else:
                logging.info("uk_absence_data table exists")
        except Exception as e:
            self.conn.rollback()
            logging.error(f"Error ensuring absence table: {e}")

    def _update_school_row(self, urn_int: int, csvvals: dict) -> int:
        self.cursor.execute(
            """
            UPDATE uk_schools
            SET total_pupils   = COALESCE(%s, total_pupils),
                percentage_fsm = COALESCE(%s, percentage_fsm),
                english_score  = COALESCE(%s, english_score),
                math_score     = COALESCE(%s, math_score),
                science_score  = COALESCE(%s, science_score),
                updated_at     = CURRENT_TIMESTAMP
            WHERE urn = %s
            """,
            (
                csvvals.get('pupils'),
                csvvals.get('fsm'),
                csvvals.get('lit'),
                csvvals.get('num'),
                csvvals.get('sci'),
                urn_int,
            ),
        )
        return self.cursor.rowcount

    def _upsert_absence(self, urn_int: int, school_id: Optional[int], csvvals: dict):
        # Compute overall absence if attendance present
        att = csvvals.get('att')
        if att is None:
            return 0, 0
        overall = round(max(0.0, min(100.0, 100.0 - att)), 2)

        # Detect schema
        mode, cols = detect_absence_schema(self.cursor)

        # First try to UPDATE existing row(s) (by urn or by school_id)
        if mode == 'URN':
            self.cursor.execute(
                """
                UPDATE uk_absence_data
                SET overall_absence_rate = COALESCE(%s, overall_absence_rate),
                    academic_year        = COALESCE(%s, academic_year),
                    updated_at           = CURRENT_TIMESTAMP
                WHERE urn = %s
                """,
                (overall, self.academic_year, urn_int),
            )
            updated = self.cursor.rowcount

            # Then INSERT missing (urn, year)
            # Build flexible columns if la_code/estab_number exist
            insert_cols = ['urn', 'overall_absence_rate', 'persistent_absence_rate', 'academic_year', 'created_at']
            insert_vals = [urn_int, overall, None, self.academic_year]
            if 'la_code' in cols:
                insert_cols.insert(1, 'la_code')
                insert_vals.insert(1, None)
            if 'estab_number' in cols:
                # position after la_code if present
                pos = 2 if 'la_code' in cols else 1
                insert_cols.insert(pos, 'estab_number')
                insert_vals.insert(pos, None)

            placeholders = ', '.join(['%s'] * (len(insert_cols) - 1)) + ", NOW()"
            self.cursor.execute(
                f"""
                INSERT INTO uk_absence_data ({', '.join(insert_cols)})
                SELECT {placeholders}
                WHERE NOT EXISTS (
                    SELECT 1 FROM uk_absence_data a
                    WHERE a.urn = %s AND (a.academic_year IS NOT DISTINCT FROM %s)
                )
                """,
                (*insert_vals, urn_int, self.academic_year),
            )
            inserted = self.cursor.rowcount
            return inserted, updated

        elif mode == 'SCHOOL_ID' and school_id is not None:
            self.cursor.execute(
                """
                UPDATE uk_absence_data
                SET overall_absence_rate = COALESCE(%s, overall_absence_rate),
                    academic_year        = COALESCE(%s, academic_year),
                    updated_at           = CURRENT_TIMESTAMP
                WHERE school_id = %s
                """,
                (overall, self.academic_year, school_id),
            )
            updated = self.cursor.rowcount

            self.cursor.execute(
                """
                INSERT INTO uk_absence_data (school_id, overall_absence_rate, academic_year, created_at)
                SELECT %s, %s, %s, CURRENT_TIMESTAMP
                WHERE NOT EXISTS (
                    SELECT 1 FROM uk_absence_data a
                    WHERE a.school_id = %s AND (a.academic_year IS NOT DISTINCT FROM %s)
                )
                """,
                (school_id, overall, self.academic_year, school_id, self.academic_year),
            )
            inserted = self.cursor.rowcount
            return inserted, updated
        else:
            logging.warning("uk_absence_data schema not recognized (no urn or school_id column). Skipping absence.")
            return 0, 0

    def run(self, csv_index: Dict[str, dict], only_urn: Optional[str] = None, country: Optional[str] = None, all_countries: bool = False, dry_run: bool = False):
        if not self.connect_db():
            return
        try:
            # Only create table if missing; otherwise leave schema as-is
            self.ensure_absence_table_exists()

            with self.conn.cursor() as cur:
                urn_filter = re.sub(r'[^0-9]', '', only_urn) if only_urn else None
                if all_countries:
                    if urn_filter:
                        cur.execute("SELECT id, urn, name, la_code, establishment_number FROM uk_schools WHERE urn = %s ORDER BY urn", (int(urn_filter),))
                    else:
                        cur.execute("SELECT id, urn, name, la_code, establishment_number FROM uk_schools ORDER BY urn")
                else:
                    if urn_filter:
                        cur.execute("SELECT id, urn, name, la_code, establishment_number FROM uk_schools WHERE LOWER(country)=LOWER(%s) AND urn = %s ORDER BY urn", (country or 'Wales', int(urn_filter)))
                    else:
                        cur.execute("SELECT id, urn, name, la_code, establishment_number FROM uk_schools WHERE LOWER(country)=LOWER(%s) ORDER BY urn", (country or 'Wales',))
                schools = cur.fetchall()

            logging.info(f"DB schools to process: {len(schools)}")
            processed = updated = ins_abs = upd_abs = skipped = errors = 0

            for (school_id, urn, name, la_code, estab_no) in schools:
                urn_str = str(urn)
                csvvals = csv_index.get(urn_str)
                if not csvvals:
                    skipped += 1
                    logging.info(f"URN {urn}: not in CSV → skipped")
                    continue

                # Update school row
                try:
                    if dry_run:
                        logging.info(f"[DRY] URN {urn} {name}: pupils={csvvals.get('pupils')} fsm={csvvals.get('fsm')} lit={csvvals.get('lit')} num={csvvals.get('num')} sci={csvvals.get('sci')} att={csvvals.get('att')}")
                        processed += 1
                        continue

                    s_rows = self._update_school_row(int(urn), csvvals)
                    updated += s_rows

                    # Absence upsert
                    ins, upd = self._upsert_absence(int(urn), school_id, csvvals)
                    ins_abs += ins
                    upd_abs += upd

                    self.conn.commit()
                    processed += 1
                    logging.info(f"✓ URN {urn}: school_updated={s_rows}, absence_inserted={ins}, absence_updated={upd}")
                except Exception as e:
                    self.conn.rollback()
                    errors += 1
                    logging.error(f"✗ URN {urn}: {e}")

            self.schools_processed = processed
            self.schools_updated = updated
            self.attendance_records_inserted = ins_abs
            self.schools_not_found = skipped

            logging.info(f"Done. processed={processed}, school_updates={updated}, absence_inserts={ins_abs}, absence_updates={upd_abs}, skipped={skipped}, errors={errors}")
        finally:
            self.close()

# --------------------------------------
# CLI entry
# --------------------------------------

def main():
    parser = argparse.ArgumentParser(description='Wales data uploader (DB-driven, per-URN logging)')
    parser.add_argument('--csv', default='wales_schools.csv', help='Path to Wales CSV (default: wales_schools.csv)')
    parser.add_argument('--db', dest='db_url', default='postgresql://schoolsdb_user:JdHohdmOIWv1dC4OzSwIgtme44QNxlbw@dpg-d2ju9ee3jp1c73fii3ag-a.frankfurt-postgres.render.com/schoolsdb', help='Postgres URL')
    parser.add_argument('--only-urn', default=None, help='Process only this URN (e.g., 6604025)')
    parser.add_argument('--country', default='Wales', help='Filter by country (case-insensitive). Ignored if --all-countries is set')
    parser.add_argument('--all-countries', action='store_true', help='Process all schools in DB (ignore country filter)')
    parser.add_argument('--year', default='2023/24', help='Academic year for absence rows (default: 2023/24)')
    parser.add_argument('--dry-run', action='store_true', help='Print actions without writing to DB')
    args = parser.parse_args()

    # Load CSV to index by URN
    if not os.path.exists(args.csv):
        logging.error(f"CSV not found: {args.csv}")
        return
    csv_index = load_csv_index(args.csv)

    # Run uploader
    uploader = WalesSchoolDataUploader(args.db_url, academic_year=args.year)
    uploader.run(csv_index, only_urn=args.only_urn, country=args.country, all_countries=args.all_countries, dry_run=args.dry_run)

if __name__ == '__main__':
    main()
