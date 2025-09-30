import requests
import sqlite3
import pandas as pd
import numpy as np
import time
from datetime import datetime
from openaq import OpenAQ

"""
Extraction Functions
"""

def openMeteo_APICall(latitude, longitude, start_date, end_date):

    url = "https://archive-api.open-meteo.com/v1/archive"

    params = {
        "latitude": latitude,
        "longitude": longitude,
	    "start_date": start_date,
        "end_date": end_date,
        "daily": ["temperature_2m_max", "temperature_2m_min", "precipitation_sum", "rain_sum", "snowfall_sum"],
        "temperature_unit": "fahrenheit",
        "timezone": "America/New_York" #Eastern Time
    }

    response = requests.get(url, params=params)
    data = response.json()
    

    if response.status_code == 200: #OK
        if not "error" in data:
            return data
        else:
            print("Response Error: ", data['error'], data['reason'])
    else:
        print("HTTP Call Error:", response.status_code, response.text)
        return None
    


def NHTSA_APICall(stateCode, countyCode, startYear, endYear):

    url = f"https://crashviewer.nhtsa.dot.gov/CrashAPI/crashes/GetCrashesByLocation?fromCaseYear={startYear}&toCaseYear={endYear}&state={stateCode}&county={countyCode}&format=json"
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
    }

    response = requests.get(url, headers=headers, timeout=30)
    data = response.json()
    
    if response.status_code == 200: #OK
        return data
    else:
        print("Error:", response.status_code, response.text)
        return None
    


def NHTSA_GetCaseSpecifics(db, stateCode):

    conn = sqlite3.connect(db)

    crash_df = pd.read_sql_query("SELECT * FROM crashes", conn)

    caseSpecDB = pd.DataFrame(columns=["state_case","year","month","day"])
    tempdb = pd.read_sql_query(f"SELECT name FROM sqlite_master WHERE type='table' AND name='case_specifics'", conn)
    if len(tempdb["name"]) > 0: #case specifics table already exists in database
        caseSpecDB = pd.read_sql_query("SELECT * FROM case_specifics", conn)

        for index, row in crash_df.iterrows():
            scase = row["state_case"]
            year = row["year"]

            if scase in caseSpecDB["state_case"].unique():
                print(f"Skipping case {scase}, as it is in the dataset already")
            else:
                url = f"https://crashviewer.nhtsa.dot.gov/CrashAPI/crashes/GetCaseDetails?stateCase={scase}&caseYear={year}&state={stateCode}&format=json"

                new_df = NHTSA_CaseSpec_APICall(url, scase)
                if new_df is not None:
                    caseSpecDB = pd.concat([caseSpecDB, new_df])

        return caseSpecDB
    


def NHTSA_CaseSpec_APICall(url, scase):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
    }

    time.sleep(2) #Being extra cautious since this will be sending multiple API calls in rapid succession

    response = requests.get(url, headers=headers, timeout=30)
    data = response.json()

    new_df = None
    if data["Count"] == 1: #Some cases don't have detailed information released yet, in which case the API will return a count of 0
        new_df = pd.DataFrame([[
        scase,
        data["Results"][0][0]['CrashResultSet']['YEAR'],
        data["Results"][0][0]['CrashResultSet']['MONTH'],
        data["Results"][0][0]['CrashResultSet']['DAY']
        ]], columns = ["state_case","year","month","day"])

        print(f"Successfully got date for case {scase}")
    else:
        print(f"Call to case {scase} returned {data["Count"]} rows, unable to get date information.")

    return new_df



def OpenAQ_Sensor_APICall(sensorID, limit, page):
    
    client = OpenAQ(api_key="7755a70a98b5f75b8d6c20c291ea8d3bec9a8b18542a10126358c7d19f49a75a")
    response = client.measurements.list(
        sensors_id=sensorID,
        data="days",
        limit=limit,
        page=page
    )

    return response
    
"""
Transformation Functions
"""

def transform_weather(data):
    weather_df = pd.DataFrame({
                    "latitude": data["latitude"],
                    "longitude": data["longitude"],
                    "date": data["daily"]["time"],
                    "temp_max_F": data["daily"]["temperature_2m_max"],
                    "temp_min_F": data["daily"]["temperature_2m_min"],
                    "precip_sum": data["daily"]["precipitation_sum"],
                    "rain_sum": data["daily"]["rain_sum"],
                    "snowfall_sum": data["daily"]["snowfall_sum"]
                })
    return weather_df



def transform_accidents(data):
    accident_df = pd.DataFrame.from_dict(data['Results'][0])

    accident_df = accident_df.drop(columns=["CITY","COUNTY","STATE","TWAY_ID2","VE_FORMS"])

    accident_df = accident_df.rename(columns={
        "CITYNAME": "city",
        "COUNTYNAME": "county",
        "CaseYear": "year",
        "FATALS": "fatals",
        "LATITUDE": "latitude",
        "LONGITUD": "longitude",
        "STATENAME": "state",
        "ST_CASE": "state_case",
        "TOTALVEHICLES": "vehicles",
        "TWAY_ID": "road_occurred"
    })

    return accident_df

def transform_ozone_measure(data):
    ozone_df = pd.DataFrame(columns=["datetime","ozone_ppm"])
    for item in data.results:
        date = item.period.datetime_from.local[:10]

        new_df = pd.DataFrame([[
            date,
            item.value
        ]], columns=["datetime","ozone_ppm"])

        ozone_df = pd.concat([ozone_df, new_df])
    return ozone_df

"""
Loading Function
"""

def load_to_database(weather_df, crash_df, casespecs_df, ozone_df, db="crashinfo.db"):
    conn = sqlite3.connect(db)

    weather_df.to_sql("weather", conn, if_exists="replace", index=False)
    print("Weather (weather) data loaded into SQLite")
    crash_df.to_sql("crashes", conn, if_exists="replace", index=False)
    print("Crash data (crashes) loaded into SQLite")
    casespecs_df.to_sql("case_specifics", conn, if_exists="replace", index=False)
    print("Case Specific (case_specifics) data loaded into SQLite")
    ozone_df.to_sql("gtech_ozone", conn, if_exists="replace", index=False)
    print("Georgia Tech Ozone (gtech_ozone) data loaded into SQLite")

    conn.close()