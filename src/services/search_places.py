# -*- coding: utf-8 -*-
"""
In-memory place search over places.jsonl using RapidFuzz.

Usage:
  from src.services.search_places import PlaceSearch
  s = PlaceSearch.load_default()
  s.search("ahrensfelde")
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

from rapidfuzz import fuzz, process


PROJECT_ROOT = Path(__file__).resolve().parents[2]
PLACES_PATH = PROJECT_ROOT / "data" / "processed" / "places.jsonl"


@dataclass
class PlaceHit:
    place_id: str
    name: str
    score: float
    status_code: Optional[str] = None
    district: Optional[str] = None
    verband_name: Optional[str] = None
    lat: Optional[float] = None
    lon: Optional[float] = None


class PlaceSearch:
    def __init__(self, places_by_id: Dict[str, Dict[str, Any]]):
        self.places_by_id = places_by_id
        # Precompute candidates for fuzzy search
        self._name_to_id: Dict[str, str] = {}
        self._candidates: List[str] = []
        for pid, p in places_by_id.items():
            name = (p.get("name") or "").strip()
            if not name:
                continue
            key = name.lower()
            # If duplicates exist, keep first; MVP is fine.
            if key not in self._name_to_id:
                self._name_to_id[key] = pid
                self._candidates.append(key)

    @classmethod
    def load_default(cls, path: Path = PLACES_PATH) -> "PlaceSearch":
        if not path.exists():
            raise FileNotFoundError(f"places.jsonl not found. Run pipeline first: {path}")
        places: Dict[str, Dict[str, Any]] = {}
        with path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                obj = json.loads(line)
                pid = obj.get("place_id")
                if pid:
                    places[pid] = obj
        return cls(places)

    def get(self, place_id: str) -> Optional[Dict[str, Any]]:
        return self.places_by_id.get(place_id)

    def search(self, query: str, limit: int = 10, min_score: int = 60) -> List[PlaceHit]:
        q = (query or "").strip().lower()
        if not q:
            return []

        # RapidFuzz: return best matches among candidate strings
        matches = process.extract(
            q,
            self._candidates,
            scorer=fuzz.WRatio,
            limit=limit,
        )

        hits: List[PlaceHit] = []
        for cand, score, _idx in matches:
            if score < min_score:
                continue
            pid = self._name_to_id.get(cand)
            if not pid:
                continue
            p = self.places_by_id.get(pid) or {}
            admin = p.get("admin") or {}
            geo = p.get("geo") or {}
            hits.append(
                PlaceHit(
                    place_id=pid,
                    name=p.get("name") or cand,
                    score=float(score),
                    status_code=p.get("status_code"),
                    district=admin.get("district"),
                    verband_name=admin.get("verband_name"),
                    lat=(geo.get("lat") if isinstance(geo, dict) else None),
                    lon=(geo.get("lon") if isinstance(geo, dict) else None),
                )
            )
        return hits


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python -m src.services.search_places <query> [limit] [min_score]")
        print("Example: python -m src.services.search_places \"Ahrensfelde\"")
        sys.exit(1)
    
    query = sys.argv[1]
    limit = int(sys.argv[2]) if len(sys.argv) > 2 else 10
    min_score = int(sys.argv[3]) if len(sys.argv) > 3 else 60
    
    try:
        search = PlaceSearch.load_default()
        results = search.search(query, limit=limit, min_score=min_score)
        
        if not results:
            print(f"No results found for: {query}")
        else:
            print(f"Found {len(results)} result(s) for: {query}\n")
            for hit in results:
                print(f"  {hit.name:<40} (ID: {hit.place_id}, Score: {hit.score:.1f})")
                if hit.district:
                    print(f"    District: {hit.district}")
                if hit.lat and hit.lon:
                    print(f"    Coordinates: {hit.lat:.4f}, {hit.lon:.4f}")
                if hit.verband_name:
                    print(f"    Verband: {hit.verband_name}")
                print()
    except FileNotFoundError as e:
        print(f"Error: {e}")
        sys.exit(1)
