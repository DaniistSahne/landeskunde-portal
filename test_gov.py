import requests
import json

def test_gov_simple():
    """Einfacher GOV-Test ohne SOAP-Komplexität"""
    
    # GOV hat auch eine einfache REST-Schnittstelle
    # Für Brandenburg Orte suchen
    test_url = "https://gov.genealogy.net/api/json/search?q=Ahrensfelde&state=Brandenburg"
    
    try:
        response = requests.get(test_url, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            print("✅ GOV-API funktioniert!")
            print(f"Gefundene Einträge: {len(data.get('results', []))}")
            
            for result in data.get('results', [])[:3]:  # Erste 3
                print(f"\nOrt: {result.get('name')}")
                print(f"ID: {result.get('id')}")
                print(f"Typ: {result.get('type')}")
                print(f"Zeitraum: {result.get('from')} - {result.get('to')}")
        else:
            print(f"❌ API Fehler: {response.status_code}")
            
    except Exception as e:
        print(f"❌ Fehler: {e}")
        print("\nVersuchen wir SOAP (einfache Version):")
        test_gov_soap_simple()

def test_gov_soap_simple():
    """Einfacher SOAP Test für GOV"""
    
    # Minimaler SOAP Request
    soap_request = '''<?xml version="1.0" encoding="UTF-8"?>
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <searchByName xmlns="http://gov.genealogy.net/ws">
      <placename>Ahrensfelde</placename>
    </searchByName>
  </soap:Body>
</soap:Envelope>'''
    
    headers = {
        'Content-Type': 'text/xml; charset=utf-8',
        'SOAPAction': 'searchByName'
    }
    
    try:
        response = requests.post(
            'https://gov.genealogy.net/services/ComplexService',
            data=soap_request,
            headers=headers,
            timeout=10
        )
        
        print(f"SOAP Response Status: {response.status_code}")
        print("Response (erste 500 Zeichen):")
        print(response.text[:500])
        
    except Exception as e:
        print(f"SOAP Fehler: {e}")

if __name__ == "__main__":
    print("Teste GOV-API...")
    test_gov_simple()