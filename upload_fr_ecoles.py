"""
Upload french_school.csv into PostgreSQL table public.fr_ecoles.

- Fixes phone numbers: strips non-digits, converts 33.. to 0.., prefixes 0 when missing, clamps to 10 digits when possible.
- Parses dates in YYYY-MM-DD or DD/MM/YYYY.
- Parses latitude/longitude (handles comma decimal separator).
- Upserts on primary key identifiant_de_l_etablissement.

Usage:
  python upload_fr_ecoles.py --csv "french_school.csv" \
      --dsn "postgresql://user:pass@host/dbname"

If --dsn is omitted, DATABASE_URL env var is used. A sensible default is
pre-populated for convenience.

Requires: psycopg2-binary (pip install psycopg2-binary)
"""

from __future__ import annotations

import argparse
import csv
import os
import re
from datetime import datetime, date
from typing import Any, Dict, Iterable, List, Sequence, Tuple


DEFAULT_DSN = (
    os.environ.get(
        "DATABASE_URL",
        "postgresql://school_platform_db_fr_user:cDW1EB5Ah6x9KCguituTxOs63EZlXgnV@"
        "dpg-d37b0r6r433s73ejel20-a.oregon-postgres.render.com/school_platform_db_fr",
    )
)


def clean_phone(value: str | None) -> str | None:
    if not value:
        return None
    digits = re.sub(r"\D", "", value)
    if not digits:
        return None
    if digits.startswith("33") and len(digits) >= 11:
        digits = "0" + digits[2:]
    elif not digits.startswith("0"):
        digits = "0" + digits
    # Keep at most 10 digits for standard FR landline/mobile format
    if len(digits) >= 10:
        return digits[:10]
    return digits


def clean_text(value: str | None) -> str | None:
    if value is None:
        return None
    v = value.strip()
    return v if v != "" else None


def parse_int(value: str | None) -> int | None:
    v = clean_text(value)
    if v is None:
        return None
    try:
        return int(v)
    except Exception:
        return None


def parse_float(value: str | None) -> float | None:
    v = clean_text(value)
    if v is None:
        return None
    v = v.replace(",", ".")
    try:
        return float(v)
    except Exception:
        return None


def parse_date(value: str | None) -> date | None:
    v = clean_text(value)
    if v is None:
        return None
    # Try ISO
    for fmt in ("%Y-%m-%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(v, fmt).date()
        except Exception:
            pass
    return None


# Target table columns, in order
COLUMNS: Tuple[str, ...] = (
    "identifiant_de_l_etablissement",
    "nom_etablissement",
    "type_etablissement",
    "statut_public_prive",
    "adresse_1",
    "adresse_2",
    "adresse_3",
    "code_postal",
    "code_commune",
    "nom_commune",
    "code_departement",
    "code_academie",
    "code_region",
    "ecole_maternelle",
    "ecole_elementaire",
    "voie_generale",
    "voie_technologique",
    "voie_professionnelle",
    "telephone",
    "fax",
    "web",
    "mail",
    "restauration",
    "hebergement",
    "ulis",
    "apprentissage",
    "segpa",
    "section_arts",
    "section_cinema",
    "section_theatre",
    "section_sport",
    "section_internationale",
    "section_europeenne",
    "lycee_agricole",
    "lycee_militaire",
    "lycee_des_metiers",
    "post_bac",
    "appartenance_education_prioritaire",
    "greta",
    "siren_siret",
    "nombre_d_eleves",
    "fiche_onisep",
    "libelle_departement",
    "libelle_academie",
    "libelle_region",
    "nom_circonscription",
    "latitude",
    "longitude",
    "date_ouverture",
    "date_maj_ligne",
)


def row_to_values(raw: Dict[str, str]) -> Tuple[Any, ...]:
    # Normalize keys to lowercase to match table columns
    lower = {k.lower(): v for k, v in raw.items()}

    def g(name: str) -> str | None:
        return lower.get(name)

    return (
        clean_text(g("identifiant_de_l_etablissement")),
        clean_text(g("nom_etablissement")),
        clean_text(g("type_etablissement")),
        clean_text(g("statut_public_prive")),
        clean_text(g("adresse_1")),
        clean_text(g("adresse_2")),
        clean_text(g("adresse_3")),
        clean_text(g("code_postal")),
        clean_text(g("code_commune")),
        clean_text(g("nom_commune")),
        clean_text(g("code_departement")),
        clean_text(g("code_academie")),
        clean_text(g("code_region")),
        clean_text(g("ecole_maternelle")),
        clean_text(g("ecole_elementaire")),
        clean_text(g("voie_generale")),
        clean_text(g("voie_technologique")),
        clean_text(g("voie_professionnelle")),
        clean_phone(g("telephone")),
        clean_phone(g("fax")),
        clean_text(g("web")),
        clean_text(g("mail")),
        clean_text(g("restauration")),
        clean_text(g("hebergement")),
        clean_text(g("ulis")),
        clean_text(g("apprentissage")),
        clean_text(g("segpa")),
        clean_text(g("section_arts")),
        clean_text(g("section_cinema")),
        clean_text(g("section_theatre")),
        clean_text(g("section_sport")),
        clean_text(g("section_internationale")),
        clean_text(g("section_europeenne")),
        clean_text(g("lycee_agricole")),
        clean_text(g("lycee_militaire")),
        clean_text(g("lycee_des_metiers")),
        clean_text(g("post_bac")),
        clean_text(g("appartenance_education_prioritaire")),
        clean_text(g("greta")),
        clean_text(g("siren_siret")),
        parse_int(g("nombre_d_eleves")),
        clean_text(g("fiche_onisep")),
        clean_text(g("libelle_departement")),
        clean_text(g("libelle_academie")),
        clean_text(g("libelle_region")),
        clean_text(g("nom_circonscription")),
        parse_float(g("latitude")),
        parse_float(g("longitude")),
        parse_date(g("date_ouverture")),
        parse_date(g("date_maj_ligne")),
    )


def load_csv_rows(path: str) -> Iterable[Tuple[Any, ...]]:
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            yield row_to_values(row)


def build_insert_query(schema: str = "public", table: str = "fr_ecoles") -> str:
    cols = ", ".join(COLUMNS)
    update_cols = ", ".join([f"{c} = EXCLUDED.{c}" for c in COLUMNS if c != "identifiant_de_l_etablissement"])
    return (
        f"INSERT INTO {schema}.{table} ({cols}) VALUES %s "
        f"ON CONFLICT (identifiant_de_l_etablissement) DO UPDATE SET {update_cols};"
    )


def upsert_rows(dsn: str, rows: Iterable[Tuple[Any, ...]], batch_size: int = 2000) -> int:
    import psycopg2
    from psycopg2.extras import execute_values

    total = 0
    conn = psycopg2.connect(dsn)
    conn.autocommit = False
    try:
        with conn.cursor() as cur:
            query = build_insert_query()
            batch: List[Tuple[Any, ...]] = []
            for r in rows:
                # Skip rows without primary key
                if not r or r[0] is None:
                    continue
                batch.append(r)
                if len(batch) >= batch_size:
                    # De-duplicate rows in this batch by primary key to avoid
                    # "ON CONFLICT DO UPDATE command cannot affect row a second time"
                    # when the CSV contains duplicate identifiers.
                    unique = {}
                    for row in batch:
                        unique[row[0]] = row  # keep last occurrence
                    deduped = list(unique.values())
                    if deduped:
                        execute_values(cur, query, deduped, page_size=batch_size)
                        total += len(deduped)
                    batch.clear()
            if batch:
                unique = {}
                for row in batch:
                    unique[row[0]] = row
                deduped = list(unique.values())
                if deduped:
                    execute_values(cur, query, deduped, page_size=batch_size)
                    total += len(deduped)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    return total


def maybe_truncate(dsn: str, truncate: bool) -> None:
    if not truncate:
        return
    import psycopg2

    conn = psycopg2.connect(dsn)
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE public.fr_ecoles RESTART IDENTITY CASCADE;")
    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Upload french_school.csv to PostgreSQL fr_ecoles table")
    parser.add_argument("--csv", dest="csv_path", default="french_school.csv", help="Path to CSV file")
    parser.add_argument("--dsn", dest="dsn", default=DEFAULT_DSN, help="PostgreSQL DSN or env DATABASE_URL")
    parser.add_argument("--truncate", action="store_true", help="Truncate table before import")
    parser.add_argument("--batch", type=int, default=2000, help="Batch size for inserts")
    args = parser.parse_args()

    print(f"CSV: {args.csv_path}")
    print("Connecting to database…")
    maybe_truncate(args.dsn, args.truncate)
    rows = load_csv_rows(args.csv_path)
    total = upsert_rows(args.dsn, rows, batch_size=args.batch)
    print(f"Imported/updated {total} rows into public.fr_ecoles")


if __name__ == "__main__":
    main()
