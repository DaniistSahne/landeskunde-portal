# -*- coding: utf-8 -*-
"""
Builds a minimal "places backbone" from the Brandenburg municipal directory (Excel).

Input:  data/raw/GemVerz.xlsx
Output: data/processed/places.jsonl
        data/processed/places_index.json

Run:
  python -m src.pipelines.01_build_places
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import pandas as pd
from pyproj import CRS, Transformer


PROJECT_ROOT = Path(__file__).resolve().parents[2]

# Adjust this to your actual file name:
INPUT_PATH = PROJECT_ROOT / "data" / "raw" / "GemVerz.xlsx"

OUT_PLACES = PROJECT_ROOT / "data" / "processed" / "places.jsonl"
OUT_INDEX = PROJECT_ROOT / "data" / "processed" / "places_index.json"


# --- Column handling ---------------------------------------------------------

CANON_MAP = {
    "status -id": "status_id",
    "status-id": "status_id",
    "status_id": "status_id",

    "status": "status_code",

    "amtlicher gemeinde-schlüssel (ags)": "ags",
    "amtlicher gemeindeschlüssel (ags)": "ags",
    "ags": "ags",

    "amtlicher regional-schlüssel (ars)": "ars",
    "amtlicher regional schlüssel (ars)": "ars",
    "ars": "ars",

    "gemeinde-verbandsnr. (gvnr)": "gvnr",
    "gemeindeverbandsnr. (gvnr)": "gvnr",
    "gvnr": "gvnr",

    "gemeinde, ortsteil, gemeindeteil, wohnplatz": "name",
    "gemeinde, ortsteil, gemeindeteil, wohnplatz ": "name",
    "name": "name",

    "sorbischer ortsname": "name_sorb",

    "gemeindeverband": "verband_name",
    "gemeindeverbandsart": "verband_type",

    "landkreis / kreisfreie stadt": "district",

    "einwohnerzahl (31.12.2022)": "population",
    "fläche in ha (31.12.2022)": "area_ha",

    # Excel sometimes flattens the multi-header; we map individual ones if present
    "zone": "utm_zone",
    "ostwert": "utm_easting",
    "nordwert": "utm_northing",

    "postleitzahl (01.01.2026)": "plz",
    "telefon- vorwahl": "vorwahl",

    "region": "region",

    "letzte korrektur": "last_modified",
    "korrektur(en)": "corrections",
}

RE_WHITESPACE = re.compile(r"\s+")


def norm_col(col: str) -> str:
    c = str(col).strip().lower()
    c = c.replace("\ufeff", "")
    c = RE_WHITESPACE.sub(" ", c)
    return c


def to_int_safe(x: Any) -> Optional[int]:
    if pd.isna(x):
        return None
    s = str(x).strip()
    if not s:
        return None
    # population sometimes like "72.461" -> 72461
    s = s.replace(".", "").replace(" ", "")
    s = re.sub(r"[^\d\-]", "", s)
    try:
        return int(s)
    except ValueError:
        return None


def to_float_safe(x: Any) -> Optional[float]:
    if pd.isna(x):
        return None
    s = str(x).strip()
    if not s:
        return None
    # area sometimes like "22.972" -> 22972.0
    s = s.replace(".", "").replace(" ", "")
    s = s.replace(",", ".")
    s = re.sub(r"[^\d\.\-]", "", s)
    try:
        return float(s)
    except ValueError:
        return None


def to_str_safe(x: Any) -> Optional[str]:
    if pd.isna(x):
        return None
    s = str(x).strip()
    return s or None


# --- Coordinate conversion ----------------------------------------------------

@dataclass(frozen=True)
class UTMKey:
    zone: int
    hemisphere: str  # 'N' expected for Brandenburg


_transformers: Dict[UTMKey, Transformer] = {}


def utm_to_wgs84(zone: Any, easting: Any, northing: Any) -> Tuple[Optional[float], Optional[float]]:
    z = to_int_safe(zone)
    e = to_int_safe(easting)
    n = to_int_safe(northing)
    if z is None or e is None or n is None:
        return None, None

    key = UTMKey(zone=z, hemisphere="N")
    if key not in _transformers:
        # EPSG for UTM zone on ETRS89: 258xx (xx=zone)
        src = CRS.from_epsg(25800 + z)
        dst = CRS.from_epsg(4326)
        _transformers[key] = Transformer.from_crs(src, dst, always_xy=True)

    lon, lat = _transformers[key].transform(e, n)
    # basic sanity check: Brandenburg roughly lat 51-54, lon 11-15
    if not (45 <= lat <= 60 and 5 <= lon <= 20):
        return None, None
    return float(lat), float(lon)


def read_excel_loose(path: Path) -> pd.DataFrame:
    """
    Reads GemVerz.xlsx in a way that tolerates:
    - first row header
    - potential extra top rows
    - mixed types

    Strategy:
    1) try header=0
    2) if required columns missing, try header=1..5
    """
    required_norm = {"status -id", "status", "gemeinde, ortsteil, gemeindeteil, wohnplatz"}
    for header_row in range(0, 6):
        df = pd.read_excel(path, header=header_row, dtype=str, engine="openpyxl")
        cols_norm = {norm_col(c) for c in df.columns}
        if required_norm.issubset(cols_norm):
            return df
    # fallback: just read with header=0
    return pd.read_excel(path, header=0, dtype=str, engine="openpyxl")


def main() -> None:
    if not INPUT_PATH.exists():
        raise FileNotFoundError(f"Input not found: {INPUT_PATH}")

    df = read_excel_loose(INPUT_PATH)

    # Normalize headers
    rename = {}
    for c in df.columns:
        nc = norm_col(c)
        if nc in CANON_MAP:
            rename[c] = CANON_MAP[nc]
        else:
            rename[c] = nc
    df = df.rename(columns=rename)

    required = ["status_id", "status_code", "name", "ags", "ars", "district", "utm_zone", "utm_easting", "utm_northing"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(
            "Missing required columns after normalization.\n"
            f"Missing: {missing}\n"
            f"Have: {list(df.columns)}\n\n"
            "Tip: Excel headers may be different (e.g. 'Status -ID' vs 'Status -Id').\n"
            "If this happens, copy/paste the exact header row and extend CANON_MAP."
        )

    OUT_PLACES.parent.mkdir(parents=True, exist_ok=True)

    index: Dict[str, list] = {}

    with OUT_PLACES.open("w", encoding="utf-8") as f:
        for _, row in df.iterrows():
            place_id = to_str_safe(row.get("status_id"))
            name = to_str_safe(row.get("name"))
            if not place_id or not name:
                continue

            lat, lon = utm_to_wgs84(row.get("utm_zone"), row.get("utm_easting"), row.get("utm_northing"))

            obj = {
                "place_id": place_id,
                "name": name,
                "name_sorb": to_str_safe(row.get("name_sorb")),
                "status_code": to_str_safe(row.get("status_code")),
                "ags": to_str_safe(row.get("ags")),
                "ars": to_str_safe(row.get("ars")),
                "gvnr": to_str_safe(row.get("gvnr")),
                "admin": {
                    "district": to_str_safe(row.get("district")),
                    "verband_name": to_str_safe(row.get("verband_name")),
                    "verband_type": to_str_safe(row.get("verband_type")),
                    "region": to_str_safe(row.get("region")),
                    "plz": to_str_safe(row.get("plz")),
                    "vorwahl": to_str_safe(row.get("vorwahl")),
                },
                "stats": {
                    "population": to_int_safe(row.get("population")),
                    "area_ha": to_float_safe(row.get("area_ha")),
                },
                "geo": {
                    "lat": lat,
                    "lon": lon,
                    "utm": {
                        "zone": to_int_safe(row.get("utm_zone")),
                        "easting": to_int_safe(row.get("utm_easting")),
                        "northing": to_int_safe(row.get("utm_northing")),
                        "crs": "ETRS89 / UTM (EPSG:258xx)",
                    },
                },
                "aliases": [],   # later: from GOV / Ortslexikon / Denkmalamt
                "sources": {
                    "gemeindeverzeichnis_bb": {
                        "last_modified": to_str_safe(row.get("last_modified")),
                        "corrections": to_str_safe(row.get("corrections")),
                    }
                }
            }

            f.write(json.dumps(obj, ensure_ascii=False) + "\n")

            key = name.strip().lower()
            index.setdefault(key, []).append(place_id)

    OUT_INDEX.write_text(json.dumps(index, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"OK: wrote {OUT_PLACES}")
    print(f"OK: wrote {OUT_INDEX}")
    print(f"Rows: {len(df)}")


if __name__ == "__main__":
    main()
