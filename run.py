#!/usr/bin/env python3
"""
Schnellstart für das Landeskunde Portal
"""

import sys
import os

# Füge src zum Python-Pfad hinzu
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

def main():
    print("🚀 Landeskunde Portal - Quick Start")
    print("=" * 40)
    
    # 1. GOV Test
    from api.resolvers.gov_resolver import GOVResolver
    
    print("\n1. Teste GOV-Resolver...")
    resolver = GOVResolver()
    results = resolver.search_place("Ahrensfelde", limit=3)
    
    if results:
        print(f"✅ Found {len(results)} places")
        for place in results:
            print(f"   - {place.get('name')} ({place.get('id')})")
    else:
        print("❌ No results from GOV")
    
    # 2. CSV einlesen
    print("\n2. Teste Gemeindeverzeichnis...")
    try:
        import pandas as pd
        df = pd.read_csv('data/gemeindeverzeichnis.csv', sep='\t', encoding='utf-8')
        print(f"✅ CSV geladen: {len(df)} Einträge")
        print(f"   Spalten: {', '.join(df.columns[:5])}...")
    except Exception as e:
        print(f"❌ CSV Fehler: {e}")
        print("   Tipp: Legen Sie data/gemeindeverzeichnis.csv an")
    
    print("\n🎉 Setup abgeschlossen!")
    print("\nNächste Schritte:")
    print("1. Testen Sie Museum-Digital: python test_museum.py")
    print("2. Starten Sie die API: uvicorn src.api.search_api:app --reload")
    print("3. Besuchen Sie: http://localhost:8000/docs")

if __name__ == "__main__":
    main()