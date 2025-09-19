#!/usr/bin/env python3
import argparse, csv, logging, os, re
from datetime import datetime
from typing import Optional, List, Tuple
import psycopg2
from psycopg2.extras import execute_values
from psycopg2 import sql

DEFAULT_DB = "postgresql://schoolsdb_user:JdHohdmOIWv1dC4OzSwIgtme44QNxlbw@dpg-d2ju9ee3jp1c73fii3ag-a.frankfurt-postgres.render.com/schoolsdb"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(f"update_ni_fsme_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
        logging.StreamHandler()
    ]
)

def to_float_pct(val: Optional[str]) -> Optional[float]:
    """Parse a percentage like '23.4' or '23.4%' into float 23.4."""
    if val is None:
        return None
    s = str(val).strip()
    if s == "" or s in {"-", "NA", "N/A", "null", "NULL"}:
        return None
    s = s.replace("%", "").replace(",", "")
    try:
        return float(s)
    except Exception:
        m = re.search(r"-?\d+(?:\.\d+)?", s)
        return float(m.group()) if m else None

def to_int_safe(val: Optional[str]) -> Optional[int]:
    if val is None:
        return None
    s = str(val).strip().replace(",", "")
    if s == "" or s in {"-", "NA", "N/A", "null", "NULL"}:
        return None
    m = re.search(r"\d+", s)
    return int(m.group()) if m else None

def read_fsme(csv_path: str) -> List[Tuple[int, Optional[float]]]:
    """
    Returns a list of (urn, fsme_pct) from an NI CSV.

    - URN: 'De ref'
    - FSM %: 'free school Lunch' (preferred) OR 'free school meals'
    - Fallback if blank: % = fsme / (total enrolment | total pupils) * 100
    """
    if not os.path.exists(csv_path):
        raise FileNotFoundError(csv_path)

    rows: List[Tuple[int, Optional[float]]] = []

    with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
        sample = f.read(4096); f.seek(0)
        try:
            dialect = csv.Sniffer().sniff(sample, delimiters=",\t;|")
        except Exception:
            class D: delimiter = ","
            dialect = D()
        rdr = csv.DictReader(f, dialect=dialect)

        headers = [h.strip() for h in (rdr.fieldnames or [])]
        logging.info(f"Headers: {headers}")

        # Resolve headers (exact strings you provided + safe fallbacks)
        def key_eq(h, target): return h.strip().lower() == target
        def key_in(h, targets): return h.strip().lower() in targets

        ref_key = next((h for h in headers if key_eq(h, "de ref")), None)
        fsme_pct_key = next((h for h in headers if key_in(h, {"free school lunch", "free school meals"})), None)
        fsme_cnt_key = next((h for h in headers if key_in(h, {"fsme", "fsm"})), None)
        denom_key = next((h for h in headers if key_in(h, {
            "total enrolment", "total pupils", "total students", "enrolment", "enrollment"
        })), None)

        if not ref_key:
            logging.warning("No 'De ref' column found. Skipping file.")
            return rows

        staged, computed, missing = 0, 0, 0
        for r in rdr:
            ref_raw = r.get(ref_key)
            if not ref_raw:
                continue
            urn = re.sub(r"[^0-9]", "", str(ref_raw))
            if not urn:
                continue

            pct = None
            # 1) Preferred: direct % from 'free school Lunch' (or 'free school meals')
            if fsme_pct_key:
                pct = to_float_pct(r.get(fsme_pct_key))

            # 2) Fallback: compute from count / denominator
            if pct is None and fsme_cnt_key and denom_key:
                num = to_int_safe(r.get(fsme_cnt_key))
                den = to_int_safe(r.get(denom_key))
                if num is not None and den and den > 0:
                    pct = round(100.0 * num / den, 2)
                    computed += 1

            if pct is None:
                missing += 1
                continue

            if pct < 0:
                pct = 0.0
            rows.append((int(urn), float(pct)))
            staged += 1

        logging.info(f"Staged {staged} FSM rows from {os.path.basename(csv_path)} "
                     f"(computed={computed}, no-value={missing})")
    return rows

def ensure_target_column(cur, table: str, column: str) -> None:
    cur.execute("""
        SELECT data_type, udt_name
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = %s AND column_name = %s
    """, (table, column))
    row = cur.fetchone()
    if not row:
        raise RuntimeError(f"Column '{column}' not found in table '{table}'. "
                           f"Use --target-column to specify the correct column.")
    data_type, udt_name = row
    if data_type not in {"numeric", "double precision", "real", "integer", "bigint", "smallint"}:
        logging.warning(f"Target column {table}.{column} has type '{data_type}/{udt_name}'. "
                        f"Ensure it can store percentages (numeric).")

def main():
    ap = argparse.ArgumentParser(description="Update uk_schools FSM % from NI school census CSVs")
    ap.add_argument("--db", default=DEFAULT_DB)
    ap.add_argument("--csvs", nargs="+", required=True, help="One or more NI FSM CSV files")
    ap.add_argument("--country", default="Northen Irland", help="Country filter (default: Northen Irland)")
    ap.add_argument("--target-column", default="percentage_fsm",
                    help="Column in uk_schools to write the FSM percentage to (default: percentage_fsm)")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    # Read + combine CSVs
    staging: List[Tuple[int, Optional[float]]] = []
    for path in args.csvs:
        staging.extend(read_fsme(path))
    if not staging:
        logging.warning("No FSM rows staged. Exiting.")
        return

    # Deduplicate by URN (last one wins)
    dedup = {}
    for urn, pct in staging:
        dedup[urn] = pct
    staging_dedup = [(urn, pct) for urn, pct in dedup.items()]
    logging.info(f"After de-duplication: {len(staging_dedup)} unique URNs")

    conn = psycopg2.connect(args.db); conn.autocommit = False
    try:
        with conn.cursor() as cur:
            # Ensure target column exists
            ensure_target_column(cur, "uk_schools", args.target_column)

            # Stage rows
            cur.execute("DROP TABLE IF EXISTS tmp_ni_fsme")
            cur.execute("""
                CREATE TEMP TABLE tmp_ni_fsme (
                  urn BIGINT PRIMARY KEY,
                  fsme_pct NUMERIC
                ) ON COMMIT DROP
            """)
            execute_values(cur,
                           "INSERT INTO tmp_ni_fsme (urn, fsme_pct) VALUES %s",
                           staging_dedup,
                           page_size=1000)

            # Diagnostics
            cur.execute("SELECT COUNT(*) FROM tmp_ni_fsme")
            logging.info(f"Temp staged rows: {cur.fetchone()[0]}")

            cur.execute("""
                SELECT COUNT(*)
                FROM tmp_ni_fsme t
                JOIN uk_schools s ON s.urn = t.urn
                WHERE LOWER(s.country) = LOWER(%s)
            """, (args.country,))
            logging.info(f"Rows matching uk_schools (country={args.country}): {cur.fetchone()[0]}")

            if args.dry_run:
                logging.info("DRY RUN — no database changes will be made.")
                conn.rollback()
                return

            # UPDATE uk_schools SET <target> = COALESCE(t.fsme_pct, s.<target>)
            update_sql = sql.SQL("""
                UPDATE uk_schools AS s
                SET {target} = COALESCE(t.fsme_pct, s.{target}),
                    updated_at = NOW()
                FROM tmp_ni_fsme t
                WHERE s.urn = t.urn
                  AND LOWER(s.country) = LOWER(%s)
            """).format(target=sql.Identifier(args.target_column))

            cur.execute(update_sql, (args.country,))
            logging.info(f"UPDATED {cur.rowcount} rows in uk_schools")

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
