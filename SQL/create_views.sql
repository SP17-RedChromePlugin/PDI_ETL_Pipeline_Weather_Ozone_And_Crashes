DROP VIEW IF EXISTS crash_weather;
CREATE VIEW crash_weather AS
SELECT
    c.state_case,
    w.date,
    c.city,
    c.county,
    c.state,
    c.fatals,
    c.road_occurred,
    c.vehicles,
    w.temp_max_F,
    w.temp_min_F,
    w.precip_sum,
    w.rain_sum,
    w.snowfall_sum
FROM crashes AS c
JOIN case_specifics AS cs
    ON c.state_case = cs.state_case
JOIN weather AS w
    ON cs.year || '-' || cs.month || '-' || cs.day = w.date;

DROP VIEW IF EXISTS weather_crashes_by_day;
CREATE VIEW weather_crashes_by_day AS
SELECT
    w.date,
    w.temp_max_F,
    w.temp_min_F,
    w.precip_sum,
    w.rain_sum,
    w.snowfall_sum,
    c.state_case,
    c.city,
    c.county,
    c.state,
    c.fatals,
    c.road_occurred,
    c.vehicles
FROM weather AS w
LEFT JOIN case_specifics AS cs
    ON cs.year || '-' || cs.month || '-' || cs.day = w.date
LEFT JOIN crashes AS c
    ON c.state_case = cs.state_case;

DROP VIEW IF EXISTS crashes_precip_freq;
CREATE VIEW crashes_precip_freq AS
SELECT
    CASE
        WHEN wcd.precip_sum == 0 THEN '0mm'
        WHEN wcd.precip_sum > 0 AND wcd.precip_sum < 2.5 THEN '0mm-2.5mm'
        WHEN wcd.precip_sum >= 2.5 AND wcd.precip_sum < 10 THEN '2.5mm-10mm'
        WHEN wcd.precip_sum >= 10 AND wcd.precip_sum < 20 THEN '10mm-20mm'
        WHEN wcd.precip_sum >= 20 THEN '>20mm'
    END AS precipitation_buckets,
    COUNT(wcd.state_case)  as crash_count,
    COUNT(1) as total_days,
    COUNT(wcd.state_case) * 1.0 / COUNT(1) as crash_freq,
    AVG(wcd.fatals) as avg_fatals,
    AVG(wcd.vehicles) AS avg_vehicles_involved
FROM weather_crashes_by_day AS wcd
GROUP BY precipitation_buckets
ORDER BY crash_freq DESC;

DROP VIEW IF EXISTS crash_ozone;
CREATE VIEW crash_ozone AS
SELECT
    c.state_case,
    o.datetime,
    c.city,
    c.county,
    c.state,
    c.fatals,
    c.road_occurred,
    c.vehicles,
    o.mean_values AS mean_ozone_ppm,
    o.minimum_value as min_ozone_ppm,
    o.maximum_value as max_ozone_ppm
FROM crashes AS c
JOIN case_specifics AS cs
    ON c.state_case = cs.state_case
JOIN gtech_ozone AS o
    ON cs.year || '-' || cs.month || '-' || cs.day = o.datetime;

DROP VIEW IF EXISTS ozone_crashes_by_day;
CREATE VIEW ozone_crashes_by_day AS
SELECT
    o.datetime,
    o.mean_values AS mean_ozone_ppm,
    o.minimum_value as min_ozone_ppm,
    o.maximum_value as max_ozone_ppm,
    c.state_case,
    c.city,
    c.county,
    c.state,
    c.fatals,
    c.road_occurred,
    c.vehicles
FROM gtech_ozone AS o
LEFT JOIN case_specifics AS cs
    ON cs.year || '-' || cs.month || '-' || cs.day = o.datetime
LEFT JOIN crashes AS c
    ON c.state_case = cs.state_case;

DROP VIEW IF EXISTS crashes_ozone_freq;
CREATE VIEW crashes_ozone_freq AS
SELECT
    CASE
        WHEN mean_ozone_ppm < 0.02 THEN '<0.02'
        WHEN mean_ozone_ppm >= 0.02 AND mean_ozone_ppm < 0.04 THEN '0.1-0.04'
        WHEN mean_ozone_ppm >= 0.04 THEN '>0.04'
    END AS ozone_buckets,
    COUNT(state_case) AS crash_count,
    COUNT(1) AS total_days,
    COUNT(state_case) * 1.0 / COUNT(1) as crash_freq,
    AVG(fatals) as avg_fatals,
    AVG(vehicles) AS avg_vehicles_involved
FROM ozone_crashes_by_day
GROUP BY ozone_buckets
ORDER BY crash_freq DESC;