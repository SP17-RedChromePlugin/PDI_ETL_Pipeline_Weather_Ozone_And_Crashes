DROP TABLE IF EXISTS weather;
CREATE TABLE weather (
    date TEXT PRIMARY KEY,
    latitude REAL,
    longitude REAL,
    temp_max_F REAL,
    temp_min_F REAL,
    precip_sum REAL,
    rain_sum REAL,
    snowfall_sum REAL
);

DROP TABLE IF EXISTS crashes;
CREATE TABLE crashes (
    state_case TEXT PRIMARY KEY,
    city TEXT,
    county TEXT,
    year INTEGER,
    fatals INTEGER,
    latitude REAL,
    longitude REAL,
    state TEXT,
    vehicles INTEGER,
    road_occurred TEXT
);

DROP TABLE IF EXISTS case_specifics;
CREATE TABLE case_specifics (
    state_case TEXT PRIMARY KEY,
    year INTEGER,
    month TEXT,
    day TEXT,
    FOREIGN KEY (state_case) REFERENCES crashes(state_case)
)

DROP TABLE IF EXISTS gtech_ozone;
CREATE TABLE gtech_ozone (
    datetime TEXT PRIMARY KEY,
    mean_values REAL,
    minimum_value REAL,
    maximum_value REAL,
    name TEXT,
    units TEXT
)