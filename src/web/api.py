# -*- coding: utf-8 -*-
"""
FastAPI MVP:
- /completion?query=ahr
- /place/{place_id}

Run:
  uvicorn src.web.api:app --reload
"""

from __future__ import annotations

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

from src.services.search_places import PlaceSearch


app = FastAPI(title="Landeskunde-Portal MVP API", version="0.1.0")

# If you later build a small frontend locally, this avoids CORS headaches.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # tighten later
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

searcher = PlaceSearch.load_default()


@app.get("/completion")
def completion(
    query: str = Query(..., min_length=1),
    limit: int = Query(10, ge=1, le=20),
):
    hits = searcher.search(query=query, limit=limit)
    return {
        "results": [
            {
                "value": h.name,
                "place_id": h.place_id,
                "score": h.score,
                "status": h.status_code,
                "district": h.district,
                "verband": h.verband_name,
                "lat": h.lat,
                "lon": h.lon,
            }
            for h in hits
        ],
        "size": len(hits),
        "from": 0,
    }


@app.get("/place/{place_id}")
def place(place_id: str):
    p = searcher.get(place_id)
    if not p:
        raise HTTPException(status_code=404, detail="place_id not found")
    return p
