# -*- coding: utf-8 -*-
"""
Convenience wrapper for place search.

Usage:
  python -m src.search_places "query"
  python -m src.search_places "query" [limit] [min_score]
"""

from src.services.search_places import main

if __name__ == "__main__":
    main()
