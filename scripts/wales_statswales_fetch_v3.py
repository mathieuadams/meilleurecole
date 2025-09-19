#!/usr/bin/env python3
"""
Wales (StatsWales) downloader + filter by school (v3)
- Downloads a dataset ZIP by Open Data code (e.g., SCHS0258.zip)
- Robust CSV decoding
- Lets you **explicitly pass the school-code column** name (e.g., School_Code_INT)
- Optional simple filter, e.g. "Academic_Year eq 2024/25"
- Replaces StatsWales sentinel **-999999999** with NaN for numeric columns (e.g. Data_DEC)

Usage
-----
python wales_statswales_fetch_v3.py \
  --dataset SCHS0258 \
  --school-col School_Code_INT \
  --schools 6613300 6613301 6614002 6614003 6614004 \
  --keep-cols "School_Code_INT,School_ItemName_ENG_STR,Localauthority_ItemName_ENG_STR,Sector_ItemName_ENG_STR,Data_DEC" \
  --out out_schs0258.csv

python wales_statswales_fetch_v3.py \
  --dataset SCHS0XYZ \
  --school-col School_Code_INT \
  --schools-file schools.txt \
  --filter "Academic_Year eq 2024/25" \
  --out out.csv
"""
from __future__ import annotations
import argparse
import io
import sys
import zipfile
from pathlib import Path
from typing import List, Optional

import numpy as np
import pandas as pd
import requests

BASE = "https://statswales.gov.wales/Download/File?fileName={name}"

COMMON_SCHOOL_COLS = [
    "School_Code_INT", "School_Code", "School Code", "School code", "School",
    "SchoolID", "School_Id"
]


def _norm_school_code(x) -> str:
    try:
        return str(int(float(str(x).strip()))).zfill(7)
    except Exception:
        return str(x).strip()


def _read_csv_any_encoding(raw: bytes) -> pd.DataFrame:
    for enc in ("utf-8", "utf-8-sig", "cp1252", "latin-1", "iso-8859-1", "utf-16"):
        try:
            return pd.read_csv(io.BytesIO(raw), encoding=enc, low_memory=False)
        except Exception:
            continue
    # last resort
    return pd.read_csv(io.BytesIO(raw), encoding="latin-1", low_memory=False, on_bad_lines="skip")


def _download_zip_frames(dataset_code: str) -> List[pd.DataFrame]:
    url = BASE.format(name=f"{dataset_code}.zip")
    r = requests.get(url, timeout=120)
    r.raise_for_status()
    z = zipfile.ZipFile(io.BytesIO(r.content))
    frames: List[pd.DataFrame] = []
    for name in z.namelist():
        if not name.lower().endswith(".csv"):
            continue
        raw = z.read(name)
        df = _read_csv_any_encoding(raw)
        df.columns = [str(c).strip().replace("\u00A0"," ") for c in df.columns]
        frames.append(df)
    if not frames:
        raise RuntimeError("No CSV files found in the dataset ZIP.")
    return frames


def _detect_school_col(df: pd.DataFrame, user_col: Optional[str]) -> Optional[str]:
    if user_col and user_col in df.columns:
        return user_col
    for cand in COMMON_SCHOOL_COLS:
        if cand in df.columns:
            return cand
    # loose contains
    for c in df.columns:
        lc = c.lower().replace("_", " ")
        if lc in ("school code", "school id", "school"):
            return c
    return None


def _apply_filter(df: pd.DataFrame, expr: Optional[str]) -> pd.DataFrame:
    if not expr:
        return df
    toks = [t.strip() for t in expr.split(" ")]
    if len(toks) < 3 or toks[1].lower() not in {"eq", "=="}:
        print(f"WARN: Ignoring unsupported filter expression: {expr}", file=sys.stderr)
        return df
    col = toks[0]
    val = " ".join(toks[2:])
    if (val.startswith("'") and val.endswith("'")) or (val.startswith('"') and val.endswith('"')):
        val = val[1:-1]
    if col not in df.columns:
        print(f"WARN: Filter column '{col}' not in dataset. Available columns: {list(df.columns)[:12]}...", file=sys.stderr)
        return df
    return df[df[col].astype(str) == str(val)]


def main():
    ap = argparse.ArgumentParser(description="Download a StatsWales dataset and filter by school numbers (v3: explicit school-col support).")
    ap.add_argument("--dataset", required=True, help="Dataset code (Open Data -> Name), e.g., SCHS0258")
    ap.add_argument("--school-col", help="Explicit school code column (e.g., School_Code_INT)")
    ap.add_argument("--schools", nargs="*", help="List of 7-digit school numbers")
    ap.add_argument("--schools-file", help="Text/CSV with one school number per line")
    ap.add_argument("--filter", help="Optional simple filter, e.g. Academic_Year eq 2024/25")
    ap.add_argument("--keep-cols", help="Comma-separated list of columns to keep in the output")
    ap.add_argument("--out", default="statswales_filtered.csv")
    args = ap.parse_args()

    wanted: List[str] = []
    if args.schools:
        wanted.extend(args.schools)
    if args.schools_file:
        p = Path(args.schools_file)
        if not p.exists():
            sys.exit(f"Schools file not found: {p}")
        for line in p.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            wanted.append(line.split(",")[0].strip())
    if not wanted:
        sys.exit("Provide school numbers via --schools or --schools-file")
    wanted = [_norm_school_code(x) for x in wanted]

    frames = _download_zip_frames(args.dataset)
    df_all = pd.concat(frames, ignore_index=True)

    # replace sentinel -999999999 with NaN for numeric cols
    for c in df_all.select_dtypes(include=[np.number]).columns:
        df_all.loc[df_all[c] == -999999999, c] = np.nan

    df_all = _apply_filter(df_all, args.filter)

    school_col = _detect_school_col(df_all, args.school_col)
    if not school_col:
        raise RuntimeError("Could not find a School code column in this dataset. Use --school-col to specify it (e.g., School_Code_INT).")

    df_all["_School_Code"] = df_all[school_col].astype(str).map(_norm_school_code)
    out = df_all[df_all["_School_Code"].isin(wanted)].copy()

    print(f"Dataset '{args.dataset}' rows: {len(df_all)} | columns: {len(df_all.columns)}")
    print(f"Using school column: '{school_col}'. Returned {len(out)} rows for {len(wanted)} schools.")

    if args.keep_cols:
        keep = [c.strip() for c in args.keep_cols.split(",") if c.strip()]
        if school_col not in keep:
            keep = [school_col] + keep
        existing = [c for c in keep if c in out.columns]
        out = out[existing]

    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(args.out, index=False)
    print(f"Wrote {len(out)} rows to {args.out}. Columns: {list(out.columns)}")

if __name__ == "__main__":
    main()
