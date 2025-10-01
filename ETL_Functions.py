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
    
    #Handling HTTP response codes and errors in response
    if response.status_code == 200:
        if not "error" in data:
            return data
        else:
            print("Response Error: ", data['error'], data['reason'])
            return None
    else:
        print("HTTP Call Error:", response.status_code, response.text)
        return None
    


def NHTSA_APICall(stateCode, countyCode, startYear, endYear):

    url = f"https://crashviewer.nhtsa.dot.gov/CrashAPI/crashes/GetCrashesByLocation?fromCaseYear={startYear}&toCaseYear={endYear}&state={stateCode}&county={countyCode}&format=json"
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
    }

    #Does not accept requests without proper header
    response = requests.get(url, headers=headers, timeout=30)
    data = response.json()
    
    if response.status_code == 200:
        return data
    else:
        print("Error:", response.status_code, response.text)
        return None
    


def NHTSA_GetCaseSpecifics(db, crash_df, stateCode):
    """
    This function acts as an addition to the previous NHTSA call. By default, crash information I obtain above
    only provides the year that the specific incident occurred. To get the specific month and day, the case ID
    alongside other identifiers must be sent back to receive more specific information.
    """

    conn = sqlite3.connect(db)

    #To avoid unnecessary API calls, the following code calls in the case_specifics table if it exists and then skips cases that are already logged.
    caseSpecDB = pd.DataFrame(columns=["state_case","year","month","day"])
    tempdb = pd.read_sql_query(f"SELECT name FROM sqlite_master WHERE type='table' AND name='case_specifics'", conn)
    if len(tempdb["name"]) > 0:
        caseSpecDB = pd.read_sql_query("SELECT * FROM case_specifics", conn)

    for _, row in crash_df.iterrows():
        scase = row["state_case"]
        year = row["year"]

        if scase in caseSpecDB["state_case"].unique():
            print(f"Skipping case {scase}, as it is in the dataset already")
        else:
            url = f"https://crashviewer.nhtsa.dot.gov/CrashAPI/crashes/GetCaseDetails?stateCase={scase}&caseYear={year}&state={stateCode}&format=json"

            #This code was originally less compact and was performing the below code in two separate places, which is why this is a separate function.
            #Even though it isn't necessary anymore, it seems more readable to keep the API call a separate function.
            new_df = NHTSA_CaseSpec_APICall(url, scase, sleep_time=3)
            if new_df is not None:
                caseSpecDB = pd.concat([caseSpecDB, new_df])

    return caseSpecDB
    


def NHTSA_CaseSpec_APICall(url, scase, sleep_time):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
    }

    time.sleep(sleep_time) #Being cautious since this will be sending multiple API calls in rapid succession when new crash specifics emerge

    response = requests.get(url, headers=headers, timeout=30)
    data = response.json()

    new_df = pd.DataFrame([[scase, None, None, None]])
    if data["Count"] == 1: #Some cases don't have detailed information released yet, in which case the API will return a count of 0

        #Since other date values in the datasets I'm working with have leading 0s in month and day values, I am adding them in here.
        #These leading zeroes are why I treat month and day values as TEXT in the schema.
        year = data["Results"][0][0]['CrashResultSet']['YEAR']
        month = f"{int(data["Results"][0][0]['CrashResultSet']['MONTH']):02}"
        day = f"{int(data["Results"][0][0]['CrashResultSet']['DAY']):02}"

        new_df = pd.DataFrame([[scase, year, month, day]], columns = ["state_case","year","month","day"])

        print(f"Successfully got date for {scase}")

    return new_df



def OpenAQ_Sensor_APICall(sensorID, limit, page):
    
    #I was originally avoiding services that required API keys, since keeping the key private is important,
    #but due to needing to choose a third dataset before too much time had passed, I chose one that required it.
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

    #Since I am using OpenAQ's API package, they return their data in custom data types, not typical python data types.
    ozone_df = pd.DataFrame(columns=["datetime","mean_values", "minimum_value", "maximum_value", "name", "units"])
    for item in data.results:
        date = item.period.datetime_from.local[:10]

        new_df = pd.DataFrame([[
            date,
            item.value,
            item.summary.min, 
            item.summary.max, 
            item.parameter.name, 
            item.parameter.units
        ]], columns=["datetime","mean_values", "minimum_value", "maximum_value", "name", "units"])

        ozone_df = pd.concat([ozone_df, new_df])
    return ozone_df

"""
Loading Function
"""

def load_to_database(weather_df, crash_df, casespecs_df, ozone_df, db="crashinfo.db"):
    """
    With more time, I would have liked to have this load function append non-duplicate values to the tables
    created in the schema. For now, this function replaces all values in the tables with new ones, 
    so this pipeline will lose old data when the window of time passes it.
    """
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

"""
Other Functions Used In the Pipeline
"""

def run_sql_script(sqlite_db, sql_file):

    conn = sqlite3.connect(sqlite_db)
    cursor = conn.cursor()

    with open(sql_file, 'r') as file:
        sql = file.read()
    
    try:
        cursor.executescript(sql)
        conn.commit()
    except sqlite3.Error as e:
        print("Error running SQL script: ", e)
        
    conn.close()