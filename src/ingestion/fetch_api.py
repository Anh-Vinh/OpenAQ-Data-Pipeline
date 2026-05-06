import os
import requests

from dotenv import load_dotenv
from datetime import datetime, timedelta, timezone

load_dotenv(override=True)
OPENAQ_KEY=os.getenv("OPENAQ_KEY")

headers = {
    "X-API-Key": OPENAQ_KEY
}

CITIES = {
    "HCM": "106.3,10.3,107.0,11.1",
    "Hanoi": "105.7,20.9,106.0,21.2"
}

BUCKET = "bronze"

def active_in(datetimeLast, days=7):
    if datetimeLast is not None:
        threshold = datetime.now(timezone.utc) - timedelta(days=days)
        datetimeLast = datetime.fromisoformat(datetimeLast["utc"].replace("Z", "+00:00"))
        return datetimeLast >= threshold
    else:
        return False

def fetch_active_locations():
    url = "https://api.openaq.org/v3/locations"
    locations = []
    
    for _, bbox in CITIES.items():
        params = {
            "bbox": bbox
        }
        
        response = requests.get(url, headers=headers, params=params)
        results = response.json().get('results', [])
        
        locations += [
            location for location in results if active_in(location['datetimeLast'])
        ]
        
    print(f"Fetched {len(locations)} locations from OpenAQ")
    return locations
        
def fetch_measurements(locations, execution_date):
    measurements = []
    
    for location in locations:
        for sensor in location["sensors"]:
            try:
                url = f"https://api.openaq.org/v3/sensors/{sensor['id']}/measurements/hourly"

                params = {
                    "datetime_from": f"{execution_date[:4]}-01-01T00:00:00+07:00",
                    "datetime_to": f"{execution_date}T00:00:00+07:00"
                }
                
                response = requests.get(url, headers=headers, params=params)
                results = response.json().get("results", [])
                
                for measurement in results:
                    measurement["sensor_id"] = sensor["id"]

                measurements.extend(results)

            except Exception as e:
                print("Exception:", e)
                print(f"Cannot fetch data from sensor with id {sensor['id']}")
    
    print(f"Fetched {len(measurements)} measurements from OpenAQ")
    return measurements

if __name__ == "__main__":
    pass
    # locations = fetch_active_locations()
    # measurements = fetch_measurements(locations, execution_date="2026-04-01")
    