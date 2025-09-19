#!/usr/bin/env python3
import argparse, csv, logging, os, re
from datetime import datetime
from typing import List, Tuple, Optional
import psycopg2
from psycopg2.extras import execute_values

DEFAULT_DB = "postgresql://schoolsdb_user:JdHohdmOIWv1dC4OzSwIgtme44QNxlbw@dpg-d2ju9ee3jp1c73fii3ag-a.frankfurt-postgres.render.com/schoolsdb"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler(f"load_ni_attendance_to_absence_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
              logging.StreamHandler()]
)

def pct(x: Optional[str]) -> Optional[float]:
    if x is None: return None
    s = str(x).strip().replace("%","").replace(",","")
    if s == "" or s in {"-","NA","N/A","null","NULL"}: return None
    m = re.search(r"-?\d+(?:\.\d+)?", s)
    return float(m.group()) if m else None

def read_file(path: str, academic_year: str) -> List[Tuple[int,str,Optional[float],Optional[float]]]:
    """Return [(urn, academic_year, attendance_rate, overall_absence_rate), ...]"""
    rows: List[Tuple[int,str,Optional[float],Optional[float]]] = []
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        sample = f.read(4096); f.seek(0)
        try:
            dialect = csv.Sniffer().sniff(sample, delimiters=",\t;|")
        except Exception:
            class D: delimiter = ","
            dialect = D()
        rdr = csv.DictReader(f, dialect=dialect)
        headers = [h.strip() for h in (rdr.fieldnames or [])]
        logging.info(f"[{os.path.basename(path)}] Headers: {headers}")

        ref_key = next((h for h in headers if h.strip().lower()=="reference"), None)
        att_key = next((h for h in headers if h.strip().lower()=="% attendance"), None)
        abs_key = next((h for h in headers if h.strip().lower()=="% absence"), None)

        if not ref_key or not att_key:
            logging.warning(f"[{os.path.basename(path)}] Missing 'Reference' or '% attendance'; skipping file.")
            return rows

        staged = 0
        for r in rdr:
            ref_raw = r.get(ref_key)
            if not ref_raw: continue
            urn_s = re.sub(r"[^0-9]","", str(ref_raw))
            if not urn_s: continue
            urn = int(urn_s)

            att = pct(r.get(att_key))
            if att is None: continue
            abs_pct = pct(r.get(abs_key)) if abs_key else None

            rows.append((urn, academic_year, att, abs_pct))
            staged += 1
        logging.info(f"[{os.path.basename(path)}] staged {staged}")
    return rows

def main():
    ap = argparse.ArgumentParser(description="Load NI attendance into uk_absence_data ONLY (FK-safe)")
    ap.add_argument("--db", default=DEFAULT_DB)
    ap.add_argument("--csvs", nargs="+", required=True,
                    help="Pass the NI attendance CSVs (primary, secondary, special)")
    ap.add_argument("--academic-year", required=True, help="e.g. '2023/2024'")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    staged: List[Tuple[int,str,Optional[float],Optional[float]]] = []
    for p in args.csvs:
        if not os.path.exists(p):
            raise FileNotFoundError(p)
        staged.extend(read_file(p, args.academic_year))

    if not staged:
        logging.warning("No rows staged; exiting.")
        return

    # De-dupe by (urn, year): last wins
    dedup = {}
    for urn, yr, att, ab in staged:
        dedup[(urn, yr)] = (urn, yr, att, ab)
    staged = list(dedup.values())
    logging.info(f"Unique (urn, year) keys: {len(staged)}")

    conn = psycopg2.connect(args.db); conn.autocommit = False
    try:
        with conn.cursor() as cur:
            # Ensure attendance_rate column exists
            cur.execute("""
                DO $$
                BEGIN
                  IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_schema='public' AND table_name='uk_absence_data'
                          AND column_name='attendance_rate'
                  ) THEN
                    ALTER TABLE uk_absence_data ADD COLUMN attendance_rate NUMERIC;
                  END IF;
                END$$;
            """)

            # Stage
            cur.execute("DROP TABLE IF EXISTS tmp_seed_att")
            cur.execute("""
                CREATE TEMP TABLE tmp_seed_att (
                  urn BIGINT,
                  academic_year TEXT,
                  attendance_rate NUMERIC,
                  overall_absence_rate NUMERIC
                ) ON COMMIT DROP
            """)
            execute_values(cur, """
                INSERT INTO tmp_seed_att (urn, academic_year, attendance_rate, overall_absence_rate)
                VALUES %s
            """, staged, page_size=1000)

            # Report URNs that would violate FK
            cur.execute("""
                SELECT COUNT(*) FROM tmp_seed_att t
                LEFT JOIN uk_schools s ON s.urn = t.urn
                WHERE s.urn IS NULL
            """)
            missing = cur.fetchone()[0]
            if missing:
                cur.execute("""
                    SELECT t.urn
                    FROM tmp_seed_att t
                    LEFT JOIN uk_schools s ON s.urn = t.urn
                    WHERE s.urn IS NULL
                    ORDER BY t.urn
                    LIMIT 20
                """)
                sample = ", ".join(str(r[0]) for r in cur.fetchall())
                logging.warning(f"Skipping {missing} rows due to missing uk_schools URNs. Sample: {sample}")

            if args.dry_run:
                cur.execute("""
                    SELECT COUNT(*) FROM tmp_seed_att t
                    JOIN uk_schools s ON s.urn = t.urn
                """)
                logging.info(f"DRY RUN — would insert {cur.fetchone()[0]} rows into uk_absence_data")
                conn.rollback()
                return

            # INSERT only rows whose URN exists in uk_schools (FK-safe)
            cur.execute("""
                INSERT INTO uk_absence_data
                  (urn, la_code, estab_number, overall_absence_rate, persistent_absence_rate,
                   academic_year, created_at, attendance_rate)
                SELECT
                  t.urn, NULL, NULL,
                  t.overall_absence_rate, NULL,
                  t.academic_year, NOW(), t.attendance_rate
                FROM tmp_seed_att t
                JOIN uk_schools s ON s.urn = t.urn
            """)
            logging.info(f"INSERTED {cur.rowcount} rows into uk_absence_data")

            conn.commit()
    except Exception as e:
        conn.rollback()
        logging.error(f"Error: {e}")
        raise
    finally:
        conn.close()
        logging.info("Done.")

if __name__ == "__main__":
    main()
