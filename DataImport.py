import requests

OpenMeteoURL = "https://api.open-meteo.com/v1/forecast" #https://open-meteo.com/en/docs/historical-forecast-api

params = {
    "latitude": 40.7128,       # NYC latitude
    "longitude": -74.0060,     # NYC longitude
    "hourly": "temperature_2m,relative_humidity_2m,precipitation",
    "daily": "temperature_2m_max,temperature_2m_min,sunrise,sunset",
    "timezone": "America/New_York"
}

response = requests.get(OpenMeteoURL, params=params)

if response.status_code == 200: #OK
    data = response.json()

    """
    data sections:
    latitude
    longitude
    generationtime_ms    
    utc_offset_seconds   
    timezone
    timezone_abbreviation
    elevation
    hourly_units
    hourly
    daily_units
    daily

    hourly sections:
    time, temperature_2m, relative_humidity_2m, precipitation
    """

    print("Hourly data keys:", data["hourly"].keys())
    print("First 5 hourly temps:", data["hourly"]["temperature_2m"][:5])
else:
    print("Error:", response.status_code, response.text)