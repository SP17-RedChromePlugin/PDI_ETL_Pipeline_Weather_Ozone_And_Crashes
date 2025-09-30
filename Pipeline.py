import ETL_Functions
from datetime import datetime

current_time = datetime.now()

stateCode = 13
countyCode = 121
latitude = 33.8034
longitude = -84.3963
sensorId = 1972 #Code for the Georgia Tech Ozone sensor most closely tied to the chosen Atlanta area

#Specifying weather data from 2-3 years ago, to better align with available accident data
def get_date_string(curTime, yearOffset):
    newTime = "{:04d}-{:02d}-{:02d}".format(curTime.year - yearOffset, curTime.month, curTime.day)
    return newTime

weather_start_time = get_date_string(datetime.now(), 3)
weather_end_time = get_date_string(datetime.now(), 2)

weather_data = ETL_Functions.openMeteo_APICall(33.8034, -84.3963, weather_start_time, weather_end_time)

#Setting the end year to now so that all retrieved information is the most recent possible
crash_data = ETL_Functions.NHTSA_APICall(13, 121, current_time.date().year - 3, current_time.date().year)

#Choosing page 3 will break as its timeframe is static, should implement a solution to get the correct page
ozone_data = ETL_Functions.OpenAQ_Sensor_APICall(sensorId, 1000, 3)

weather_df = ETL_Functions.transform_weather(weather_data)
crash_df = ETL_Functions.transform_accidents(crash_data)
casespecs_df = ETL_Functions.NHTSA_GetCaseSpecifics("crashinfo.db", 13)
ozone_df = ETL_Functions.transform_ozone_measure(ozone_data)

ETL_Functions.load_to_database(weather_df, crash_df, casespecs_df, ozone_df, db="crashinfo.db")