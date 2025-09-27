#!/usr/bin/env python3
"""
Update French school data from CSVs.

Adds/updates:
- fr_ecoles aggregate columns (students_total, lycee_* totals, boys/girls, bac & DNB)
- fr_tables: a per‑class 2024 table populated from french_school_etudiant_par_class2024.csv

Usage examples:
  python scripts/update_fr_data.py --dry-run
  python scripts/update_fr_data.py --db <DATABASE_URL>
"""

from __future__ import annotations

import argparse
import csv
import os
import re
import unicodedata
from typing import Dict, List, Optional, Tuple

import psycopg2


HARDCODED_DB = (
    "postgresql://school_platform_db_fr_user:cDW1EB5Ah6x9KCguituTxOs63EZlXgnV@"
    "dpg-d37b0r6r433s73ejel20-a.oregon-postgres.render.com/school_platform_db_fr"
)


def log(msg: str) -> None:
    print(msg, flush=True)


def norm(s: Optional[str]) -> str:
    if s is None:
        return ""
    x = unicodedata.normalize("NFKD", str(s))
    x = "".join(ch for ch in x if unicodedata.category(ch) != "Mn")  # strip accents
    x = x.lower()
    x = re.sub(r"[^a-z0-9]+", " ", x)
    return x.strip()


def to_int(v: Optional[str]) -> Optional[int]:
    if v is None:
        return None
    s = str(v).strip()
    if not s:
        return None
    # keep first integer (handles things like "1 234", "1,234", etc.)
    s = s.replace("\xa0", "").replace(",", "")
    m = re.search(r"-?\d+", s)
    return int(m.group()) if m else None


def to_float(v: Optional[str]) -> Optional[float]:
    if v is None:
        return None
    s = str(v).strip().replace("\xa0", "")
    if not s or s.upper() in {"NULL", "N/A", "-"}:
        return None
    if "," in s and "." not in s:  # french decimal
        s = s.replace(" ", "").replace(",", ".")
    else:
        s = s.replace(",", "")
    s = s.replace("%", "")
    m = re.search(r"-?\d+(?:\.\d+)?", s)
    try:
        return float(m.group()) if m else None
    except Exception:
        return None


def detect_delimiter(path: str) -> str:
    try:
        head = open(path, "r", encoding="utf-8", errors="ignore").read(4096)
        semi = head.count(";")
        comma = head.count(",")
        return ";" if semi > comma else ","
    except Exception:
        return ","


def read_csv(path: str) -> Tuple[List[Dict[str, str]], List[str]]:
    """Read CSV with best‑effort encoding handling (UTF‑8 BOM, fallback cp1252)."""
    if not os.path.exists(path):
        log(f"CSV not found: {path}")
        return [], []
    delim = detect_delimiter(path)

    def _load(encoding: str) -> Tuple[List[Dict[str, str]], List[str]]:
        rows: List[Dict[str, str]] = []
        with open(path, "r", encoding=encoding, errors="replace", newline="") as f:
            reader = csv.DictReader(f, delimiter=delim)
            headers = reader.fieldnames or []
            for r in reader:
                rows.append(r)
        return rows, list(headers)

    rows, headers = _load("utf-8-sig")
    if headers and sum(h.count("\ufffd") for h in headers) > 2:  # many replacement chars
        rows, headers = _load("cp1252")

    log(f"Parsed {len(rows)} rows from {os.path.basename(path)} (delimiter '{delim}')")
    return rows, (headers or [])


def _row_key_map(row: Dict[str, str]) -> Dict[str, str]:
    return {norm(k): k for k in row.keys()}


def rget(
    row: Dict[str, str], aliases: List[str] | None = None, tokens_all: List[str] | None = None
) -> Optional[str]:
    """Return row value by header aliases or by token‑based normalized match."""
    aliases = aliases or []
    tokens_all = tokens_all or []
    nmap = _row_key_map(row)

    for a in aliases:
        a_n = norm(a)
        if a_n in nmap:
            return row[nmap[a_n]]

    for a in aliases:
        a_n = norm(a)
        for nk, real in nmap.items():
            if a_n and a_n in nk:
                return row[real]

    if tokens_all:
        want = [norm(t) for t in tokens_all if norm(t)]
        for nk, real in nmap.items():
            if all(w in nk for w in want):
                return row[real]
    return None


# ---------- Database helpers

def ensure_columns(cur) -> None:
    alters = [
        "ALTER TABLE fr_ecoles ADD COLUMN IF NOT EXISTS students_total INTEGER",
        "ALTER TABLE fr_ecoles ADD COLUMN IF NOT EXISTS boys_total INTEGER",
        "ALTER TABLE fr_ecoles ADD COLUMN IF NOT EXISTS girls_total INTEGER",
        "ALTER TABLE fr_ecoles ADD COLUMN IF NOT EXISTS lycee_students_total INTEGER",
        "ALTER TABLE fr_ecoles ADD COLUMN IF NOT EXISTS lycee_effectifs_seconde INTEGER",
        "ALTER TABLE fr_ecoles ADD COLUMN IF NOT EXISTS lycee_effectifs_premiere INTEGER",
        "ALTER TABLE fr_ecoles ADD COLUMN IF NOT EXISTS lycee_effectifs_terminale INTEGER",
        "ALTER TABLE fr_ecoles ADD COLUMN IF NOT EXISTS lycee_bac_candidates INTEGER",
        "ALTER TABLE fr_ecoles ADD COLUMN IF NOT EXISTS lycee_bac_success_rate NUMERIC(6,3)",
        "ALTER TABLE fr_ecoles ADD COLUMN IF NOT EXISTS lycee_mentions_rate NUMERIC(6,3)",
        "ALTER TABLE fr_ecoles ADD COLUMN IF NOT EXISTS college_dnb_candidates INTEGER",
        "ALTER TABLE fr_ecoles ADD COLUMN IF NOT EXISTS college_dnb_success_rate NUMERIC(6,3)",
    ]
    for sql in alters:
        cur.execute(sql)


def ensure_fr_tables(cur) -> None:
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS fr_tables (
            uai TEXT PRIMARY KEY REFERENCES fr_ecoles("identifiant_de_l_etablissement") ON DELETE CASCADE,
            rentree_scolaire INTEGER,
            code_region_academique INTEGER,
            code_region_insee INTEGER,
            region_academique TEXT,
            code_academie INTEGER,
            academie TEXT,
            code_departement INTEGER,
            departement TEXT,
            code_postal TEXT,
            commune TEXT,
            denomination_principale TEXT,
            patronyme TEXT,
            secteur TEXT,
            rep INTEGER,
            rep_plus INTEGER,
            nombre_total_classes INTEGER,
            nombre_total_eleves INTEGER,
            pre_elementaire_hors_ulis INTEGER,
            elementaire_hors_ulis INTEGER,
            eleves_ulis INTEGER,
            eleves_ueea INTEGER,
            cp_hors_ulis INTEGER,
            ce1_hors_ulis INTEGER,
            ce2_hors_ulis INTEGER,
            cm1_hors_ulis INTEGER,
            cm2_hors_ulis INTEGER,
            num_ligne BIGINT
        )
        """
    )


def build_lookup(cur) -> Dict[str, str]:
    cur.execute(
        """
        SELECT "identifiant_de_l_etablissement" AS uai,
               nom_etablissement, nom_commune, libelle_departement
        FROM fr_ecoles
        """
    )
    m: Dict[str, str] = {}
    for uai, name, town, dep in cur.fetchall():
        key = f"{norm(name)}|{norm(town)}|{norm(dep)}"
        m.setdefault(key, uai)
    return m


# ---------- Loaders

def update_lycee_results(cur, rows: List[Dict[str, str]], only_uai: Optional[set] = None) -> int:
    updated = 0
    for r in rows:
        uai = (rget(r, ["UAI", "uai"]) or "").strip()
        if not uai:
            continue
        if only_uai and uai not in only_uai:
            continue

        candidates = to_int(rget(r, tokens_all=["nombre", "bac"]))
        success = to_float(rget(r, tokens_all=["taux", "bruts", "total"]))
        mentions = to_float(rget(r, tokens_all=["taux", "mentions", "bruts"]))

        eff2 = to_int(rget(r, tokens_all=["2nde"]))
        eff1 = to_int(rget(r, tokens_all=["1", "re"]))
        effT = to_int(rget(r, tokens_all=["term"]))

        lycee_total = None
        if any(x is not None for x in (eff2, eff1, effT)):
            lycee_total = sum(x or 0 for x in (eff2, eff1, effT))

        cur.execute(
            """
            UPDATE fr_ecoles SET
              lycee_bac_candidates = COALESCE(%s, lycee_bac_candidates),
              lycee_bac_success_rate = COALESCE(%s, lycee_bac_success_rate),
              lycee_mentions_rate = COALESCE(%s, lycee_mentions_rate),
              lycee_effectifs_seconde = COALESCE(%s, lycee_effectifs_seconde),
              lycee_effectifs_premiere = COALESCE(%s, lycee_effectifs_premiere),
              lycee_effectifs_terminale = COALESCE(%s, lycee_effectifs_terminale),
              lycee_students_total = COALESCE(%s, lycee_students_total)
            WHERE "identifiant_de_l_etablissement" = %s
            """,
            (candidates, success, mentions, eff2, eff1, effT, lycee_total, uai),
        )
        updated += cur.rowcount
    log(f"Lycee results: updated {updated} rows")
    return updated


def update_boys_girls_from_lycee_classes(cur, rows: List[Dict[str, str]], only_uai: Optional[set] = None) -> int:
    updated = 0
    for r in rows:
        uai = (rget(r, ["UAI", "uai"]) or "").strip()
        if not uai:
            continue
        if only_uai and uai not in only_uai:
            continue
        girls = sum(to_int(r.get(h)) or 0 for h in r.keys() if "fille" in norm(h)) or None
        boys = sum(to_int(r.get(h)) or 0 for h in r.keys() if "garcon" in norm(h)) or None
        if girls is None and boys is None:
            continue
        cur.execute(
            """
            UPDATE fr_ecoles SET girls_total = COALESCE(%s, girls_total),
                                  boys_total = COALESCE(%s, boys_total)
            WHERE "identifiant_de_l_etablissement" = %s
            """,
            (girls, boys, uai),
        )
        updated += cur.rowcount
    log(f"Lycee classes (girls/boys): updated {updated} rows")
    return updated


def update_students_2024(
    cur, rows: List[Dict[str, str]], lookup: Dict[str, str], only_uai: Optional[set] = None
) -> Tuple[int, int]:
    matched_uai = matched_name = 0
    for r in rows:
        total = to_int(rget(r, tokens_all=["nombre", "total", "eleves"]))
        if total is None:
            continue

        uai = (
            rget(
                r,
                [
                    "UAI",
                    "Numero de l ecole",
                    "Numero de l'ecole",
                ],
            )
            or ""
        ).strip()

        if uai:
            if only_uai and uai not in only_uai:
                continue
            cur.execute(
                """
                UPDATE fr_ecoles SET 
                    students_total = COALESCE(%s, students_total),
                    "Nombre_d_eleves" = COALESCE(%s, "Nombre_d_eleves")
                WHERE "identifiant_de_l_etablissement" = %s
                """,
                (total, total, uai),
            )
            if cur.rowcount:
                matched_uai += 1
            continue

        # Fallback by name/town/dep
        name = rget(r, tokens_all=["denomination", "principale"]) or ""
        town = rget(r, ["Commune"]) or ""
        dep = rget(r, tokens_all=["departement"]) or ""
        if not (name and town and dep):
            continue
        key = f"{norm(name)}|{norm(town)}|{norm(dep)}"
        uai2 = lookup.get(key)
        if not uai2:
            continue
        if only_uai and uai2 not in only_uai:
            continue
        cur.execute(
            """
            UPDATE fr_ecoles SET 
                students_total = COALESCE(%s, students_total),
                "Nombre_d_eleves" = COALESCE(%s, "Nombre_d_eleves")
            WHERE "identifiant_de_l_etablissement" = %s
            """,
            (total, total, uai2),
        )
        if cur.rowcount:
            matched_name += 1
    log(
        f"2024 totals: matched {matched_uai} by UAI, {matched_name} by name/commune/departement"
    )
    return matched_uai, matched_name


def upsert_fr_tables_all(
    cur, rows: List[Dict[str, str]], only_uai: Optional[set] = None
) -> int:
    """Upsert rows from french_school_etudiant_par_class2024.csv into fr_tables."""
    total = 0
    for r in rows:
        uai = (rget(r, ["UAI", "Numero de l ecole", "Numero de l'ecole"]) or "").strip()
        if not uai:
            continue
        if only_uai and uai not in only_uai:
            continue

        def T(*tokens):
            return to_int(rget(r, tokens_all=list(tokens)))

        values = {
            "rentree_scolaire": T("rentree", "scolaire"),
            "code_region_academique": T("code", "region", "academique"),
            "code_region_insee": T("code", "region", "insee"),
            "region_academique": rget(r, tokens_all=["region", "academique"]) or None,
            "code_academie": T("code", "academie"),
            "academie": rget(r, tokens_all=["academie"]) or None,
            "code_departement": T("code", "departement"),
            "departement": rget(r, tokens_all=["departement"]) or None,
            "code_postal": rget(r, tokens_all=["code", "postal"]) or None,
            "commune": rget(r, tokens_all=["commune"]) or None,
            "denomination_principale": rget(r, tokens_all=["denomination", "principale"]) or None,
            "patronyme": rget(r, tokens_all=["patronyme"]) or None,
            "secteur": rget(r, tokens_all=["secteur"]) or None,
            "rep": T("rep"),
            "rep_plus": T("rep", "+"),
            "nombre_total_classes": T("nombre", "total", "classes"),
            "nombre_total_eleves": T("nombre", "total", "eleves"),
            "pre_elementaire_hors_ulis": T("pre", "elementaire", "hors", "ulis"),
            "elementaire_hors_ulis": T("elementaire", "hors", "ulis"),
            "eleves_ulis": T("eleves", "ulis"),
            "eleves_ueea": T("eleves", "ueea"),
            "cp_hors_ulis": T("cp", "hors", "ulis"),
            "ce1_hors_ulis": T("ce1", "hors", "ulis"),
            "ce2_hors_ulis": T("ce2", "hors", "ulis"),
            "cm1_hors_ulis": T("cm1", "hors", "ulis"),
            "cm2_hors_ulis": T("cm2", "hors", "ulis"),
            "num_ligne": T("num", "ligne"),
        }

        cur.execute(
            """
            INSERT INTO fr_tables (
                uai, rentree_scolaire, code_region_academique, code_region_insee,
                region_academique, code_academie, academie, code_departement, departement,
                code_postal, commune, denomination_principale, patronyme, secteur,
                rep, rep_plus, nombre_total_classes, nombre_total_eleves,
                pre_elementaire_hors_ulis, elementaire_hors_ulis, eleves_ulis, eleves_ueea,
                cp_hors_ulis, ce1_hors_ulis, ce2_hors_ulis, cm1_hors_ulis, cm2_hors_ulis,
                num_ligne
            ) VALUES (
                %(uai)s, %(rentree_scolaire)s, %(code_region_academique)s, %(code_region_insee)s,
                %(region_academique)s, %(code_academie)s, %(academie)s, %(code_departement)s, %(departement)s,
                %(code_postal)s, %(commune)s, %(denomination_principale)s, %(patronyme)s, %(secteur)s,
                %(rep)s, %(rep_plus)s, %(nombre_total_classes)s, %(nombre_total_eleves)s,
                %(pre_elementaire_hors_ulis)s, %(elementaire_hors_ulis)s, %(eleves_ulis)s, %(eleves_ueea)s,
                %(cp_hors_ulis)s, %(ce1_hors_ulis)s, %(ce2_hors_ulis)s, %(cm1_hors_ulis)s, %(cm2_hors_ulis)s,
                %(num_ligne)s
            )
            ON CONFLICT (uai) DO UPDATE SET
                rentree_scolaire = EXCLUDED.rentree_scolaire,
                code_region_academique = EXCLUDED.code_region_academique,
                code_region_insee = EXCLUDED.code_region_insee,
                region_academique = EXCLUDED.region_academique,
                code_academie = EXCLUDED.code_academie,
                academie = EXCLUDED.academie,
                code_departement = EXCLUDED.code_departement,
                departement = EXCLUDED.departement,
                code_postal = EXCLUDED.code_postal,
                commune = EXCLUDED.commune,
                denomination_principale = EXCLUDED.denomination_principale,
                patronyme = EXCLUDED.patronyme,
                secteur = EXCLUDED.secteur,
                rep = EXCLUDED.rep,
                rep_plus = EXCLUDED.rep_plus,
                nombre_total_classes = EXCLUDED.nombre_total_classes,
                nombre_total_eleves = EXCLUDED.nombre_total_eleves,
                pre_elementaire_hors_ulis = EXCLUDED.pre_elementaire_hors_ulis,
                elementaire_hors_ulis = EXCLUDED.elementaire_hors_ulis,
                eleves_ulis = EXCLUDED.eleves_ulis,
                eleves_ueea = EXCLUDED.eleves_ueea,
                cp_hors_ulis = EXCLUDED.cp_hors_ulis,
                ce1_hors_ulis = EXCLUDED.ce1_hors_ulis,
                ce2_hors_ulis = EXCLUDED.ce2_hors_ulis,
                cm1_hors_ulis = EXCLUDED.cm1_hors_ulis,
                cm2_hors_ulis = EXCLUDED.cm2_hors_ulis,
                num_ligne = EXCLUDED.num_ligne
            """,
            {"uai": uai, **values},
        )
        total += 1
    log(f"fr_tables: upserted {total} rows from 2024 per-class CSV")
    return total


def main() -> None:
    ap = argparse.ArgumentParser(description="Update French school data into fr_ecoles and fr_tables")
    ap.add_argument("--db", default=HARDCODED_DB, help="Postgres URL (Render DB by default)")
    ap.add_argument("--csv-lycee-results", default="Lycee general and pro.csv")
    ap.add_argument("--csv-classes-lycee", default="french_school_etudiant_par_class_lycee.csv")
    ap.add_argument("--csv-students-2024", default="french_school_etudiant_par_class2024.csv")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--only-uai", help="Comma-separated list of UAI to update only")
    args = ap.parse_args()

    log(f"Connecting to DB: {args.db.split('@')[1] if '@' in args.db else args.db}")
    connect_kwargs = {}
    if "sslmode=" not in args.db:
        connect_kwargs["sslmode"] = "require"  # Render requires SSL
    conn = psycopg2.connect(args.db, **connect_kwargs)
    conn.autocommit = False
    try:
        with conn.cursor() as cur:
            ensure_columns(cur)
            ensure_fr_tables(cur)

            results_rows, _ = read_csv(args.csv_lycee_results)
            lycee_rows, _ = read_csv(args.csv_classes_lycee)
            enr_rows, _ = read_csv(args.csv_students_2024)

            only_uai: Optional[set] = None
            if args.only_uai:
                only_uai = {u.strip() for u in re.split(r"[,\s]+", args.only_uai) if u.strip()}

            lookup = build_lookup(cur)

            update_lycee_results(cur, results_rows, only_uai)
            update_boys_girls_from_lycee_classes(cur, lycee_rows, only_uai)
            upsert_fr_tables_all(cur, enr_rows, only_uai)
            update_students_2024(cur, enr_rows, lookup, only_uai)

        if args.dry_run:
            log("Dry run: rolling back")
            conn.rollback()
        else:
            conn.commit()
            log("Committed updates to fr_ecoles and fr_tables")
    finally:
        conn.close()


if __name__ == "__main__":
    main()

