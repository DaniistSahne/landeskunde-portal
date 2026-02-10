import requests
import xml.etree.ElementTree as ET
from typing import List, Dict, Optional
import json
from dataclasses import dataclass
from datetime import datetime

@dataclass
class GOVPlace:
    """Datenklasse für GOV Orte"""
    gov_id: str
    name: str
    historical_names: List[Dict]
    type: str
    location: Optional[Dict]
    valid_from: Optional[int]
    valid_to: Optional[int]
    parent: Optional[str]
    external_ids: Dict

class GOVSoapClient:
    """SOAP Client für GOV API"""
    
    def __init__(self):
        self.wsdl_url = "https://gov.genealogy.net/services/ComplexService?wsdl"
        self.service_url = "https://gov.genealogy.net/services/ComplexService"
        
    def search_by_name(self, place_name: str, max_results: int = 10) -> List[GOVPlace]:
        """Suche Orte nach Namen (SOAP)"""
        
        # SOAP Request XML
        soap_request = f'''<?xml version="1.0" encoding="UTF-8"?>
<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
                  xmlns:ws="http://gov.genealogy.net/ws">
   <soapenv:Header/>
   <soapenv:Body>
      <ws:searchByName>
         <placename>{place_name}</placename>
      </ws:searchByName>
   </soapenv:Body>
</soapenv:Envelope>'''
        
        headers = {
            'Content-Type': 'text/xml; charset=utf-8',
            'SOAPAction': 'searchByName'
        }
        
        try:
            response = requests.post(
                self.service_url,
                data=soap_request,
                headers=headers,
                timeout=30
            )
            
            print(f"Status Code: {response.status_code}")
            
            if response.status_code == 200:
                return self._parse_search_response(response.content)
            else:
                print(f"Error: {response.text[:500]}")
                return []
                
        except Exception as e:
            print(f"SOAP Request failed: {e}")
            return []
    
    def _parse_search_response(self, xml_content: bytes) -> List[GOVPlace]:
        """Parse SOAP XML Response"""
        
        try:
            # Namespaces definieren
            namespaces = {
                'soap': 'http://schemas.xmlsoap.org/soap/envelope/',
                'ns': 'http://gov.genealogy.net/ws',
                'data': 'http://gov.genealogy.net/data'
            }
            
            root = ET.fromstring(xml_content)
            
            # Finde alle Ort-Objekte
            places = []
            
            # XPath mit Namespaces
            for obj_elem in root.findall('.//data:object', namespaces):
                place = self._parse_object_element(obj_elem, namespaces)
                if place:
                    places.append(place)
            
            return places
            
        except ET.ParseError as e:
            print(f"XML Parse Error: {e}")
            # Debug: Speichere XML zur Analyse
            with open('debug_gov_response.xml', 'wb') as f:
                f.write(xml_content)
            print("Response saved to debug_gov_response.xml")
            return []
    
    def _parse_object_element(self, elem: ET.Element, namespaces: Dict) -> Optional[GOVPlace]:
        """Parse ein einzelnes GOV Object Element"""
        
        try:
            # Extrahiere ID
            obj_id = elem.get('id')
            if not obj_id:
                return None
            
            # Name finden
            name_elem = elem.find('data:currentName', namespaces)
            name = name_elem.text if name_elem is not None else "Unbekannt"
            
            # Typ
            type_elem = elem.find('data:type', namespaces)
            obj_type = type_elem.get('id') if type_elem is not None else "unknown"
            
            # Historische Namen
            historical_names = []
            for name_var in elem.findall('data:name', namespaces):
                name_text = name_var.text
                valid_from = name_var.get('from')
                valid_to = name_var.get('to')
                historical_names.append({
                    'name': name_text,
                    'from': valid_from,
                    'to': valid_to
                })
            
            # Position/Koordinaten
            location = None
            pos_elem = elem.find('data:position', namespaces)
            if pos_elem is not None:
                lat = pos_elem.get('lat')
                lon = pos_elem.get('lon')
                if lat and lon:
                    location = {'lat': float(lat), 'lon': float(lon)}
            
            # Zeitliche Gültigkeit
            valid_from = elem.get('validFrom')
            valid_to = elem.get('validTo')
            
            # Externe IDs
            external_ids = {}
            for ext_id in elem.findall('data:externalID', namespaces):
                system = ext_id.get('system')
                value = ext_id.text
                if system and value:
                    external_ids[system] = value
            
            return GOVPlace(
                gov_id=obj_id,
                name=name,
                historical_names=historical_names,
                type=obj_type,
                location=location,
                valid_from=int(valid_from) if valid_from and valid_from.isdigit() else None,
                valid_to=int(valid_to) if valid_to and valid_to.isdigit() else None,
                parent=None,  # Müsste aus parent-Element extrahiert werden
                external_ids=external_ids
            )
            
        except Exception as e:
            print(f"Error parsing object: {e}")
            return None

# Alternative: Nutze Python-Zeep für einfachere SOAP Handhabung
class GOVZeepClient:
    """Einfacher GOV Client mit Zeep"""
    
    def __init__(self):
        try:
            from zeep import Client
            from zeep.transports import Transport
            import ssl
            
            # SSL Context für ältere Server
            context = ssl.create_default_context()
            context.check_hostname = False
            context.verify_mode = ssl.CERT_NONE
            
            transport = Transport(ssl_context=context)
            
            self.client = Client(
                'https://gov.genealogy.net/services/ComplexService?wsdl',
                transport=transport
            )
            self.service = self.client.service
            print("✅ Zeep Client initialized")
            
        except ImportError:
            print("❌ Zeep nicht installiert. Installieren mit: pip install zeep")
            self.client = None
    
    def search_simple(self, place_name: str):
        """Einfache Suche mit Zeep"""
        if not self.client:
            return []
        
        try:
            result = self.service.searchByName(place_name)
            print(f"Zeep Result Type: {type(result)}")
            
            # Konvertiere zu einfachen Dicts
            places = []
            if hasattr(result, '__iter__'):
                for item in result:
                    if hasattr(item, 'object'):
                        obj = item.object
                        place = {
                            'id': getattr(obj, 'id', None),
                            'name': getattr(obj, 'currentName', None),
                            'type': getattr(getattr(obj, 'type', None), 'id', None) if hasattr(obj, 'type') else None
                        }
                        places.append(place)
            
            return places
            
        except Exception as e:
            print(f"Zeep Error: {e}")
            return []

def test_gov_search():
    """Testfunktion für GOV Suche"""
    
    print("Testing GOV Search...")
    
    # Methode 1: Direkter SOAP Request
    print("\n=== Methode 1: Direkter SOAP ===")
    client = GOVSoapClient()
    places = client.search_by_name("Ahrensfelde", max_results=5)
    
    print(f"Found {len(places)} places")
    for i, place in enumerate(places[:3]):
        print(f"\n{i+1}. {place.name} ({place.gov_id})")
        print(f"   Type: {place.type}")
        print(f"   Valid: {place.valid_from} - {place.valid_to}")
        print(f"   Location: {place.location}")
        if place.historical_names:
            print(f"   Historical names: {len(place.historical_names)}")
            for hn in place.historical_names[:2]:
                print(f"     - '{hn['name']}' ({hn.get('from')}-{hn.get('to')})")
    
    # Methode 2: Mit Zeep (falls installiert)
    print("\n=== Methode 2: Mit Zeep ===")
    try:
        zeep_client = GOVZeepClient()
        if zeep_client.client:
            zeep_results = zeep_client.search_simple("Ahrensfelde")
            print(f"Zeep found: {len(zeep_results)}")
            for r in zeep_results[:2]:
                print(f"  - {r.get('name')} ({r.get('id')})")
    except Exception as e:
        print(f"Zeep test skipped: {e}")
    
    return places

if __name__ == "__main__":
    test_gov_search()