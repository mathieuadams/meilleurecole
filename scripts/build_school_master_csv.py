from __future__ import annotations

import csv
import sys
from pathlib import Path
import unicodedata


ROOT = Path(__file__).resolve().parents[1]

BASE_FILE = ROOT / "french_school.csv"
LYCEE_FILE = ROOT / "Lycee general and pro.csv"
COLLEGE_FILE = ROOT / "college resultats.csv"  # present but not used in the requested header
ECOLE_STATS_FILE = ROOT / "french_school_etudiant_par_class2024.csv"
OUT_FILE = ROOT / "french_school_enriched.csv"


def norm(s: str) -> str:
    if s is None:
        return ""
    # Normalize accents and punctuation; keep simple ascii for matching
    s = unicodedata.normalize("NFKD", str(s))
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.replace("\u2019", "'")  # curly apostrophe → straight
    s = s.replace("\u2013", "-")  # en-dash → hyphen
    s = s.replace("\u2014", "-")  # em-dash → hyphen
    s = s.replace(".", " ")
    return " ".join(s.lower().strip().split())


def find_col(fieldnames: list[str], tokens: list[str]) -> str | None:
    """Find a column whose normalized name contains all tokens."""
    norm_map = {norm(h): h for h in fieldnames}
    for nkey, orig in norm_map.items():
        if all(tok in nkey for tok in tokens):
            return orig
    return None


def load_lycee_results(path: Path) -> dict[str, dict[str, str]]:
    results: dict[str, dict[str, str]] = {}
    if not path.exists():
        return results

    # This file appears to be CP-1252/Latin-1 with a BOM-like prefix on first header.
    # We'll parse with csv.reader to reference columns by position, which is stable.
    with path.open("r", encoding="latin-1", errors="strict", newline="") as f:
        reader = csv.reader(f)
        header = next(reader)
        # Build index map
        try:
            idx_uai = header.index("UAI")
        except ValueError:
            # Fallback: case-insensitive search
            idx_uai = next(i for i,h in enumerate(header) if h.strip().lower()=="uai")

        # Anchor around "Taux de mentions bruts"
        try:
            idx_mentions = header.index("Taux de mentions bruts")
        except ValueError:
            # Fallback: search contains 'mentions' token
            idx_mentions = next(i for i,h in enumerate(header) if "mentions".lower() in norm(h))

        idx_nb_bac = idx_mentions - 2
        idx_taux_reussite = idx_mentions - 1
        idx_eff_2nde = idx_mentions + 1
        idx_eff_1ere = idx_mentions + 2
        idx_eff_term = idx_mentions + 3

        for row in reader:
            if not row:
                continue
            key = row[idx_uai].strip()
            if not key:
                continue
            # Ensure row is long enough
            # Some rows may be shorter; pad to avoid IndexError
            while len(row) <= idx_eff_term:
                row.append("")
            results[key] = {
                "Nombre d'élèves présents au Bac": row[idx_nb_bac],
                "Taux réussite Bac": row[idx_taux_reussite],
                "Taux de mentions bruts": row[idx_mentions],
                "Effectifs à la rentrée N 2nde": row[idx_eff_2nde],
                "Effectifs à la rentrée N 1ère": row[idx_eff_1ere],
                "Effectifs à la rentrée N Term.": row[idx_eff_term],
            }
    return results


def load_ecole_stats(path: Path) -> dict[str, dict[str, str]]:
    stats: dict[str, dict[str, str]] = {}
    if not path.exists():
        return stats
    # File appears to be UTF-8 with BOM
    with path.open("r", encoding="utf-8-sig", errors="replace", newline="") as f:
        reader = csv.DictReader(f)
        fn = reader.fieldnames or []
        if not fn:
            return stats

        col_uai = (find_col(fn, ["numero", "ecole"]) or find_col(fn, ["num", "ecole"]) or fn[0])

        # Map of desired output column -> tokens to find in source headers
        wanted = {
            "REP": ["rep"],
            "REP +": ["rep", "+"],
            "Nombre total de classes": ["nombre", "total", "classes"],
            "Nombre total d'élèves": ["nombre", "total", "eleves"],
            "Nombre d'élèves en pré-élémentaire hors ULIS": ["eleves", "pre", "elementaire"],
            "Nombre d'élèves en élémentaire hors ULIS": ["eleves", "elementaire", "hors"],
            "Nombre d'élèves en ULIS": ["eleves", "ulis"],
            "Nombre d'élèves en UEEA": ["eleves", "ueea"],
            "Nombre d'élèves en CP hors ULIS": ["eleves", "cp", "hors"],
            "Nombre d'élèves en CE1 hors ULIS": ["eleves", "ce1", "hors"],
            "Nombre d'élèves en CE2 hors ULIS": ["eleves", "ce2", "hors"],
            "Nombre d'élèves en CM1 hors ULIS": ["eleves", "cm1", "hors"],
            "Nombre d'élèves en CM2 hors ULIS": ["eleves", "cm2", "hors"],
        }

        source_cols: dict[str, str] = {}
        for out_col, tokens in wanted.items():
            found = find_col(fn, tokens)
            if found:
                source_cols[out_col] = found

        for row in reader:
            key = (row.get(col_uai) or "").strip()
            if not key:
                continue
            stats[key] = {oc: row.get(sc, "") for oc, sc in source_cols.items()}
    return stats


def main() -> int:
    if not BASE_FILE.exists():
        print(f"Base file not found: {BASE_FILE}", file=sys.stderr)
        return 2

    lycee = load_lycee_results(LYCEE_FILE)
    ecole = load_ecole_stats(ECOLE_STATS_FILE)

    # Requested output header (exact order)
    output_header = [
        "Identifiant_de_l_etablissement","Nom_etablissement","Type_etablissement","Statut_public_prive","Adresse_1","Adresse_2","Adresse_3","Code_postal","Code_commune","Nom_commune","Code_departement","Code_academie","Code_region","Ecole_maternelle","Ecole_elementaire","Voie_generale","Voie_technologique","Voie_professionnelle","Telephone","Fax","Web","Mail","Restauration","Hebergement","ULIS","Apprentissage","Segpa","Section_arts","Section_cinema","Section_theatre","Section_sport","Section_internationale","Section_europeenne","Lycee_Agricole","Lycee_militaire","Lycee_des_metiers","Post_BAC","Appartenance_Education_Prioritaire","GRETA","SIREN_SIRET","Nombre_d_eleves","Fiche_onisep","Libelle_departement","Libelle_academie","Libelle_region","nom_circonscription","latitude","longitude","date_ouverture","date_maj_ligne",
        "Nombre d'élèves présents au Bac","Taux réussite Bac","Taux de mentions bruts","Effectifs à la rentrée N 2nde","Effectifs à la rentrée N 1ère","Effectifs à la rentrée N Term.",
        "REP","REP +","Nombre total de classes","Nombre total d'élèves","Nombre d'élèves en pré-élémentaire hors ULIS","Nombre d'élèves en élémentaire hors ULIS","Nombre d'élèves en ULIS","Nombre d'élèves en UEEA","Nombre d'élèves en CP hors ULIS","Nombre d'élèves en CE1 hors ULIS","Nombre d'élèves en CE2 hors ULIS","Nombre d'élèves en CM1 hors ULIS","Nombre d'élèves en CM2 hors ULIS",
    ]

    # Open base file and write enriched output
    base_read_encodings = ["utf-8-sig", "utf-8", "latin-1"]
    reader = None
    for enc in base_read_encodings:
        try:
            f = BASE_FILE.open("r", encoding=enc, errors="replace", newline="")
            reader = csv.DictReader(f)
            base_file_handle = f
            break
        except Exception:
            continue
    if reader is None:
        print("Failed to open base CSV with known encodings", file=sys.stderr)
        return 3

    with OUT_FILE.open("w", encoding="utf-8-sig", newline="") as outf:
        writer = csv.DictWriter(outf, fieldnames=output_header, extrasaction="ignore")
        writer.writeheader()

        base_cols = reader.fieldnames or []
        # Process rows
        total = 0
        merged_lycee = 0
        merged_ecole = 0
        for row in reader:
            total += 1
            uai = (row.get("Identifiant_de_l_etablissement") or "").strip()

            out_row = {k: row.get(k, "") for k in output_header if k in base_cols}

            # Initialize extra fields as empty strings
            for k in output_header:
                if k not in out_row:
                    out_row[k] = ""

            if uai and uai in lycee:
                out_row.update(lycee[uai])
                merged_lycee += 1

            if uai and uai in ecole:
                out_row.update(ecole[uai])
                merged_ecole += 1

            writer.writerow(out_row)

    base_file_handle.close()

    print(
        f"Wrote {OUT_FILE.name}: {total} rows | lycee merges: {merged_lycee} | ecole merges: {merged_ecole}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
