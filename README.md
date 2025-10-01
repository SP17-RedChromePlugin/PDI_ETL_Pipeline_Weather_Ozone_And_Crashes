# ETL Pipeline for Analyzing Weather and Ozone Level Impacts on Crash Frequency

For this ETL pipeline project, I chose to work with the APIs of OpenMeteo, NHTSA, and OpenAQ to extract weather, car crash, and ozone level data for the Atlanta area. My goal was to be able to look at correlations between precipitation and car crash frequency, since rain and snowfall are common road hazards, and potential correlations between ozone levels and car collisions, something that does not have a known correlation.

## Chosen Datasets

OpenMeteo returns weather information and has an interactive tool that helps build requests to their API, so I used that as a guide for formatting the call to them. The returned information is a list of information on precipiation levels, temperature, and rain and snowfall for each day in the specified timeframe.

NHTSA returns collision information and has two APIs that I make calls to, one being an addendum to the other. The first is to get crash information by location, which returns decent information but only provides the year the crash occurred, which isn't very helpful. To get the month and day of the crash, separate API calls must be made for each ID related to the specific crash. I wanted to be very careful when potentially making bulk requests here, so I added a few measures to limit how many calls are made. Only cases that aren't currently stored in the database are called to reduce redundancy, and there is an adjustable timer to specify a delay between calls.

OpenAQ returns ozone level information, specifically ozone readings from the sensor implemented by Georgia Tech in Atlanta. I first called their API to get a list of sensors in the Atlanta area and then chose the one closest to my chosen geographic location for obtaining ozone readings.

## ETL Approach
The data this pipeline takes in is 2-3 years old, since more recent crashes are not yet made public. Due to this and the small number of crashes added to NHTSA's database each day, running this pipeline on a batch cadence makes more sense than streaming. I'm unfamiliar with the best methods for running a pipeline automatically at a set time, so the solution I found for running on a batch cadence is to use Window's Task Scheduler. At the desired interval, it will call "Python.exe" with the Pipeline.py program passed as an argument.

## Views
There are a total of 6 views created by the pipeline:

crash_weather: Links every crash in the database with the respective weather conditions present on that day. Only includes crashes where weather conditions were known.

weather_crashes_by_day: Links every day that weather conditions were recorded with crashes that occurred that day.

crashes_precip_freq: Utilizes the weather_crashes_by_day view to sort rows into buckets depending on the amount of total precipitation that day. It counts the total accidents that occurred in each bucket, as well as the total entries in each bucket, and gives an output of crash frequency in each bucket.

crash_ozone: Links every crash with the respective ozone readings present on that day. Only includes crashes where ozone readings were known.

ozone_crashes_by_day: Links every day that ozone levels were recorded with crashes that occurred that day.

crashes_ozone_freq: Sorts ozone levels into buckets and shows frequency of crashes within each bucket.

## Current Issues / Changes I Would Have Made With More Time
Given the timeframe and my limited knowledge of ETL pipelines, there are multiple areas that would need to be reworked if this were to be implemented. Currently, the load function writes over anything present in the database already, which isn't ideal if this is meant to be collecting information over time. I would want new data to be appended to the database, and duplicate entries to be dropped. I also would have liked to avoid needing an API key for any of my datasets, since sharing it publicly is not a good idea. I hope that what I've put together in this timeframe though is enough to show my passion for the subject and willingness to learn new skills!
