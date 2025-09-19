#!/usr/bin/env python3
import argparse, csv, logging, os, re
from datetime import datetime
from typing import List, Tuple, Optional, Dict

import psycopg2
from psycopg2.extras import execute_values

DEFAULT_DB = "postgresql://schoolsdb_user:JdHohdmOIWv1dC4OzSwIgtme44QNxlbw@dpg-d2ju9ee3jp1c73fii3ag-a.frankfurt-postgres.render.com/schoolsdb"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(f"update_ni_local_authority_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
        logging.StreamHandler()
    ]
)

def clean_urn(v: Optional[str]) -> Optional[int]:
    if not v: return None
    s = re.sub(r"[^0-9]", "", str(v))
    return int(s) if s else None

def clean_txt(v: Optional[str]) -> Optional[str]:
    if v is None: return None
    s = str(v).strip()
    return s or None

def read_constituencies(csv_path: str) -> List[Tuple[int, str]]:
    """Return (urn, constituency) from a single NI census CSV."""
    rows: List[Tuple[int, str]] = []
    with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
        sample = f.read(4096); f.seek(0)
        try:
            dialect = csv.Sniffer().sniff(sample, delimiters=",\t;|")
        except Exception:
            class D: delimiter = ","
            dialect = D()
        rdr = csv.DictReader(f, dialect=dialect)
        headers = [h.strip() for h in (rdr.fieldnames or [])]
        logging.info(f"[{os.path.basename(csv_path)}] Headers: {headers}")

        # exact columns used in your files
        ref_key = next((h for h in headers if h.strip().lower() == "de ref"), None)
        const_key = next((h for h in headers if h.strip().lower() == "constituency"), None)

        if not ref_key or not const_key:
            logging.warning(f"[{os.path.basename(csv_path)}] Missing 'De ref' or 'constituency' column. Skipping file.")
            return rows

        staged = 0
        for r in rdr:
            urn = clean_urn(r.get(ref_key))
            cons = clean_txt(r.get(const_key))
            if urn is None or cons is None:
                continue
            rows.append((urn, cons))
            staged += 1
        logging.info(f"[{os.path.basename(csv_path)}] staged {staged}")
    return rows

def main():
    ap = argparse.ArgumentParser(description="Update uk_schools.local_authority from NI 'constituency' column")
    ap.add_argument("--db", default=DEFAULT_DB)
    ap.add_argument("--csvs", nargs="+", required=True,
                    help="Pass NI-school-level-primary-school.csv and NI-school-level-post-primary.csv")
    ap.add_argument("--country", default="Northen Irland", help="Country filter in uk_schools (default: Northen Irland)")
    ap.add_argument("--only-null", action="store_true",
                    help="Only update rows where local_authority is NULL")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    # Read all files and de-duplicate by URN (last file wins)
    all_rows: List[Tuple[int, str]] = []
    for p in args.csvs:
        if not os.path.exists(p):
            raise FileNotFoundError(p)
        all_rows.extend(read_constituencies(p))
    if not all_rows:
        logging.warning("No rows staged from CSVs. Exiting.")
        return

    dedup: Dict[int, str] = {}
    for urn, cons in all_rows:
        dedup[urn] = cons
    staged = [(urn, cons) for urn, cons in dedup.items()]
    logging.info(f"Unique URNs staged: {len(staged)}")

    conn = psycopg2.connect(args.db); conn.autocommit = False
    try:
        with conn.cursor() as cur:
            # Optional: ensure the column exists (won't add if already present)
            cur.execute("""
                DO $$
                BEGIN
                  IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_schema='public' AND table_name='uk_schools'
                          AND column_name='local_authority'
                  ) THEN
                    ALTER TABLE uk_schools ADD COLUMN local_authority TEXT;
                  END IF;
                END$$;
            """)

            # Stage data
            cur.execute("DROP TABLE IF EXISTS tmp_ni_const")
            cur.execute("""
                CREATE TEMP TABLE tmp_ni_const (
                  urn BIGINT PRIMARY KEY,
                  constituency TEXT
                ) ON COMMIT DROP
            """)
            execute_values(cur,
                "INSERT INTO tmp_ni_const (urn, constituency) VALUES %s",
                staged, page_size=1000)

            # Diagnostics
            cur.execute("""
                SELECT COUNT(*) FROM tmp_ni_const t
                JOIN uk_schools s ON s.urn = t.urn
                WHERE LOWER(s.country) = LOWER(%s)
            """, (args.country,))
            matches = cur.fetchone()[0]
            cur.execute("""
                SELECT COUNT(*) FROM tmp_ni_const t
                LEFT JOIN uk_schools s ON s.urn = t.urn
                WHERE s.urn IS NULL
            """)
            missing = cur.fetchone()[0]
            if missing:
                cur.execute("""
                    SELECT t.urn FROM tmp_ni_const t
                    LEFT JOIN uk_schools s ON s.urn = t.urn
                    WHERE s.urn IS NULL
                    ORDER BY t.urn LIMIT 20
                """)
                sample = ", ".join(str(r[0]) for r in cur.fetchall())
                logging.warning(f"URNs not found in uk_schools: {missing}. Sample: {sample}")
            logging.info(f"Will target {matches} rows in uk_schools (country={args.country}).")

            # Build update
            where_extra = "AND s.local_authority IS NULL" if args.only_null else ""
            update_sql = f"""
                UPDATE uk_schools s
                SET local_authority = t.constituency,
                    updated_at = NOW()
                FROM tmp_ni_const t
                WHERE s.urn = t.urn
                  AND LOWER(s.country) = LOWER(%s)
                  {where_extra}
            """

            if args.dry_run:
                # Count that would update
                cur.execute(f"""
                    SELECT COUNT(*) FROM uk_schools s
                    JOIN tmp_ni_const t ON s.urn = t.urn
                    WHERE LOWER(s.country) = LOWER(%s)
                    {where_extra}
                """, (args.country,))
                would = cur.fetchone()[0]
                logging.info(f"DRY RUN — would update {would} rows")
                conn.rollback()
                return

            cur.execute(update_sql, (args.country,))
            logging.info(f"UPDATED {cur.rowcount} rows in uk_schools.local_authority")
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
