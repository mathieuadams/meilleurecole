#!/usr/bin/env python3
import argparse, csv, logging, os, re
from datetime import datetime
from typing import Optional, List, Tuple
import psycopg2
from psycopg2.extras import execute_values

DEFAULT_DB = "postgresql://schoolsdb_user:JdHohdmOIWv1dC4OzSwIgtme44QNxlbw@dpg-d2ju9ee3jp1c73fii3ag-a.frankfurt-postgres.render.com/schoolsdb"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler(f"import_ni_schools_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
              logging.StreamHandler()]
)

def to_int(x: Optional[str]) -> Optional[int]:
    if x is None: return None
    s = str(x).strip()
    if s == "" or s.upper() in {"N/A","NA","NULL","-","*"}: return None
    m = re.search(r"\d+", s.replace(",", ""))
    return int(m.group()) if m else None

def to_float(x: Optional[str]) -> Optional[float]:
    if x is None: return None
    s = str(x).strip()
    if s == "" or s.upper() in {"N/A","NA","NULL","-","*"}: return None
    s = s.replace(",", "")
    try:
        return float(s)
    except Exception:
        m = re.search(r"-?\d+(?:\.\d+)?", s)
        return float(m.group()) if m else None

def clean(s: Optional[str]) -> Optional[str]:
    if s is None: return None
    s = str(s).strip()
    return s if s else None

def read_ni_csv(path: str, only_ref: Optional[str]=None) -> List[Tuple]:
    if not os.path.exists(path):
        raise FileNotFoundError(path)

    H = {
        "urn": "Reference",
        "name": "Institution_Name",
        "street": "Address_1",
        "locality": "Address_2",
        "town": "Town_Name",
        "county": "County_Name",
        "postcode": "Postcode",
        "telephone": "Telephone",
        "email": "Email",
        "lat": "Latitude",
        "lon": "Longitude",
        "enrol": "Current_Approved_Enrolment",
        "inst": "Institution_Type",
        "mgmt": "Management_Type",
    }

    rows = []
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        sample = f.read(4096); f.seek(0)
        try:
            dialect = csv.Sniffer().sniff(sample, delimiters=",\t;|")
        except Exception:
            class D: delimiter = ","
            dialect = D()
        rdr = csv.DictReader(f, dialect=dialect)

        total = 0
        for r in rdr:
            total += 1
            ref_raw = r.get(H["urn"])
            if not ref_raw: continue
            ref = re.sub(r"[^0-9]", "", str(ref_raw))
            if not ref: continue
            if only_ref and ref != re.sub(r"[^0-9]", "", only_ref): continue

            rows.append((
                int(ref),
                clean(r.get(H["name"])),
                clean(r.get(H["street"])),
                clean(r.get(H["locality"])),
                clean(r.get("Address_3")),           # optional third line
                clean(r.get(H["town"])),
                clean(r.get(H["county"])),
                clean(r.get(H["postcode"])),
                clean(r.get(H["telephone"])),
                clean(r.get(H["email"])),
                to_float(r.get(H["lat"])),
                to_float(r.get(H["lon"])),
                to_int(r.get(H["enrol"])),
                clean(r.get(H["inst"])),
                clean(r.get(H["mgmt"]))
            ))
        logging.info(f"Parsed NI CSV: total rows={total}, staged={len(rows)}")
    return rows

def main():
    ap = argparse.ArgumentParser(description="First-time import + upsert NI schools into uk_schools")
    ap.add_argument("--db", default=DEFAULT_DB)
    ap.add_argument("--csv", required=True, help="Path to NI-locate-a-school.csv")
    ap.add_argument("--only-ref", default=None)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--establishment-status", default="Open",
                help="Value to use for establishment_status on insert (default: Open)")

    args = ap.parse_args()

    staged = read_ni_csv(args.csv, only_ref=args.only_ref)
    if not staged:
        logging.warning("Nothing staged from CSV. Exiting.")
        return

    conn = psycopg2.connect(args.db); conn.autocommit = False
    try:
        with conn.cursor() as cur:
            # Ensure index/constraint on urn for fast upserts
            cur.execute("""
                DO $$
                BEGIN
                  IF NOT EXISTS (
                    SELECT 1 FROM pg_indexes 
                    WHERE schemaname = 'public' AND indexname = 'idx_uk_schools_urn'
                  ) THEN
                    CREATE INDEX idx_uk_schools_urn ON uk_schools(urn);
                  END IF;
                END$$;
            """)

            cur.execute("DROP TABLE IF EXISTS tmp_ni_seed")
            cur.execute("""
                CREATE TEMP TABLE tmp_ni_seed (
                  urn BIGINT PRIMARY KEY,
                  name TEXT,
                  street TEXT,
                  locality TEXT,
                  address3 TEXT,
                  town TEXT,
                  county TEXT,
                  postcode TEXT,
                  telephone TEXT,
                  email TEXT,
                  latitude NUMERIC,
                  longitude NUMERIC,
                  total_pupils INT,
                  type_of_establishment TEXT,
                  establishment_group TEXT
                ) ON COMMIT DROP
            """)

            execute_values(cur, """
                INSERT INTO tmp_ni_seed
                (urn,name,street,locality,address3,town,county,postcode,telephone,email,
                 latitude,longitude,total_pupils,type_of_establishment,establishment_group)
                VALUES %s
            """, staged, page_size=1000)

            insert_sql = """
                INSERT INTO uk_schools (
                urn, name, street, locality, town, county, postcode, telephone, email,
                latitude, longitude, total_pupils,
                type_of_establishment, establishment_group, establishment_status, country,
                slug, name_lower, created_at, updated_at
                )
                SELECT
                t.urn,
                t.name,
                t.street,
                COALESCE(t.locality, t.address3),
                t.town,
                t.county,
                t.postcode,
                t.telephone,
                t.email,
                t.latitude,
                t.longitude,
                t.total_pupils,
                t.type_of_establishment,
                t.establishment_group,
                'Open' AS establishment_status,   -- hard-coded constant
                'Northen Irland',
                (CASE WHEN COALESCE(t.name,'') <> '' THEN
                    REGEXP_REPLACE(TRIM(BOTH '-' FROM LOWER(REGEXP_REPLACE(t.name, '[^a-z0-9]+','-','g'))), '-+','-','g')
                    || '-' || t.urn::text
                ELSE 'school-' || t.urn::text END) AS slug,
                LOWER(COALESCE(t.name,'')) AS name_lower,
                NOW(), NOW()
                FROM tmp_ni_seed t
                LEFT JOIN uk_schools s ON s.urn = t.urn
                WHERE s.urn IS NULL
            """
            cur.execute(insert_sql)




            # 2) UPDATE existing rows (urn present) with NI values + set country
            update_sql = """
                UPDATE uk_schools AS s
                SET name                  = COALESCE(t.name, s.name),
                    street                = COALESCE(t.street, s.street),
                    locality              = COALESCE(t.locality, COALESCE(t.address3, s.locality)),
                    town                  = COALESCE(t.town, s.town),
                    county                = COALESCE(t.county, s.county),
                    postcode              = COALESCE(t.postcode, s.postcode),
                    telephone             = COALESCE(t.telephone, s.telephone),
                    email                 = COALESCE(t.email, s.email),
                    latitude              = COALESCE(t.latitude, s.latitude),
                    longitude             = COALESCE(t.longitude, s.longitude),
                    total_pupils      = COALESCE(t.total_pupils, s.total_pupils),
                    type_of_establishment = COALESCE(t.type_of_establishment, s.type_of_establishment),
                    establishment_group   = COALESCE(t.establishment_group, s.establishment_group),
                    country               = 'Northen Irland',
                    updated_at            = NOW()
                FROM tmp_ni_seed t
                WHERE s.urn = t.urn
            """

            # Diagnostics
            cur.execute("SELECT COUNT(*) FROM tmp_ni_seed"); staged_cnt = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM uk_schools"); before_cnt = cur.fetchone()[0]

            if args.dry_run:
                cur.execute("""
                    SELECT COUNT(*) FROM tmp_ni_seed t
                    LEFT JOIN uk_schools s ON s.urn = t.urn
                    WHERE s.urn IS NULL
                """); would_insert = cur.fetchone()[0]
                cur.execute("""
                    SELECT COUNT(*) FROM tmp_ni_seed t
                    JOIN uk_schools s ON s.urn = t.urn
                """); would_update = cur.fetchone()[0]
                logging.info(f"DRY RUN — staged={staged_cnt}, would INSERT={would_insert}, would UPDATE={would_update}")
                conn.rollback()
                return

            cur.execute(insert_sql)
            inserted = cur.rowcount

            cur.execute(update_sql)
            updated = cur.rowcount

            cur.execute("SELECT COUNT(*) FROM uk_schools"); after_cnt = cur.fetchone()[0]

            conn.commit()
            logging.info(f"INSERTED: {inserted}, UPDATED: {updated}, total rows before={before_cnt}, after={after_cnt}")

    except Exception as e:
        conn.rollback()
        logging.error(f"Error: {e}")
        raise
    finally:
        conn.close()
        logging.info("Done.")
if __name__ == "__main__":
    main()
