import requests
import xml.etree.ElementTree as ET
from typing import List, Dict, Optional
import json

class GOVResolver:
    """Vereinfachter GOV Resolver"""
    
    def __init__(self):
        self.base_url = "https://gov.genealogy.net"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Landeskunde-Portal/1.0'
        })
    
    def search_place(self, place_name: str, limit: int = 10) -> List[Dict]:
        """Suche Ort im GOV (einfache REST Version)"""
        
        # Versuche zuerst REST API
        try:
            return self._search_rest(place_name, limit)
        except Exception as e:
            print(f"REST API fehlgeschlagen, versuche SOAP: {e}")
            return self._search_soap(place_name, limit)
    
    def _search_rest(self, place_name: str, limit: int) -> List[Dict]:
        """Nutze GOV REST API wenn verfügbar"""
        
        # Alternative: Nutze die JSON-Schnittstelle
        url = f"{self.base_url}/api/json/search"
        params = {
            'q': place_name,
            'limit': limit,
            'format': 'json'
        }
        
        response = self.session.get(url, params=params, timeout=10)
        response.raise_for_status()
        
        data = response.json()
        return data.get('results', [])
    
    def _search_soap(self, place_name: str, limit: int) -> List[Dict]:
        """Fallback auf SOAP"""
        
        # Vereinfachter SOAP Request
        soap_body = f'''<?xml version="1.0" encoding="UTF-8"?>
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <searchByName xmlns="http://gov.genealogy.net/ws">
      <placename>{place_name}</placename>
    </searchByName>
  </soap:Body>
</soap:Envelope>'''
        
        response = self.session.post(
            f"{self.base_url}/services/ComplexService",
            data=soap_body,
            headers={'Content-Type': 'text/xml'},
            timeout=10
        )
        
        # Einfaches Parsing (vereinfacht)
        return self._parse_soap_response(response.text)
    
    def _parse_soap_response(self, xml_text: str) -> List[Dict]:
        """Vereinfachtes Parsing von SOAP Response"""
        
        try:
            root = ET.fromstring(xml_text)
            namespaces = {
                'soap': 'http://schemas.xmlsoap.org/soap/envelope/',
                'ns': 'http://gov.genealogy.net/ws'
            }
            
            places = []
            # ... Parsing Logik hier ...
            
            return places
            
        except Exception as e:
            print(f"Fehler beim Parsen: {e}")
            return []
    
    def get_place_details(self, gov_id: str) -> Optional[Dict]:
        """Hole Details zu einer GOV-ID"""
        
        try:
            response = self.session.get(
                f"{self.base_url}/api/json/object/{gov_id}",
                timeout=10
            )
            
            if response.status_code == 200:
                return response.json()
                
        except Exception as e:
            print(f"Fehler beim Abrufen von {gov_id}: {e}")
            
        return None

# Testfunktion
if __name__ == "__main__":
    resolver = GOVResolver()
    
    # Test mit Ihrem Beispiel
    results = resolver.search_place("Ahrensfelde")
    
    print(f"Gefundene Orte für 'Ahrensfelde': {len(results)}")
    for i, place in enumerate(results[:3]):
        print(f"\n{i+1}. {place.get('name', 'Unbekannt')}")
        print(f"   ID: {place.get('id', 'N/A')}")
        print(f"   Typ: {place.get('type', 'N/A')}")
        print(f"   Zeitraum: {place.get('from', '?')} - {place.get('to', '?')}")