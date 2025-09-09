SELECT
  TRI.bikeid,
  TRI.usertype,
  ZIPSTART.zip_code AS zip_code_start,
  ZIPSTARTNAME.borough AS borough_start,
  ZIPSTARTNAME.neighborhood AS neighborhood_start,
  ZIPEND.zip_code AS zip_code_end,
  ZIPENDNAME.borough AS borough_end,
  ZIPENDNAME.neighborhood AS neighborhood_end,
  DATE_ADD(DATE(TRI.starttime), INTERVAL 5 YEAR) AS start_day,
  DATE_ADD(DATE(TRI.stoptime), INTERVAL 5 YEAR) AS stop_day,
  EXTRACT(MONTH FROM DATE_ADD(DATE(TRI.starttime), INTERVAL 5 YEAR)) AS month,
  EXTRACT(YEAR FROM DATE_ADD(DATE(TRI.starttime), INTERVAL 5 YEAR)) AS year,
  WEA.temp AS day_mean_temperature,
  WEA.wdsp AS day_mean_wind_speed,
  WEA.prcp AS day_total_precipitation,
  ROUND(CAST(TRI.tripduration / 60 AS INT64), -1) AS trip_minutes
FROM
  `bigquery-public-data.new_york_citibike.citibike_trips` AS TRI
INNER JOIN
  `bigquery-public-data.geo_us_boundaries.zip_codes` ZIPSTART
ON ST_WITHIN(
  ST_GEOGPOINT(TRI.start_station_longitude, TRI.start_station_latitude),
  ZIPSTART.zip_code_geom)
INNER JOIN
  `bigquery-public-data.geo_us_boundaries.zip_codes` ZIPEND
ON ST_WITHIN(
  ST_GEOGPOINT(TRI.end_station_longitude, TRI.end_station_latitude),
  ZIPEND.zip_code_geom)
INNER JOIN
  `bigquery-public-data.noaa_gsod.gsod20*` AS WEA
ON PARSE_DATE("%Y%m%d", CONCAT(WEA.year, WEA.mo, WEA.da)) = DATE(TRI.starttime)
INNER JOIN
  zip_codes_dataset.zip_codes AS ZIPSTARTNAME
ON ZIPSTART.zip_code = CAST(ZIPSTARTNAME.zip AS STRING)
INNER JOIN
  zip_codes_dataset.zip_codes AS ZIPENDNAME
ON ZIPEND.zip_code = CAST(ZIPENDNAME.zip AS STRING)
WHERE
  WEA.wban = '94728' -- NEW YORK CENTRAL PARK
  AND EXTRACT(YEAR FROM DATE(TRI.starttime)) BETWEEN 2014 AND 2015
  AND EXTRACT(MONTH FROM DATE(TRI.starttime)) IN (6,7,8) -- chỉ lấy tháng 6,7,8
