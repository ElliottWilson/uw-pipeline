WITH smart_meters_snapshot AS (
SELECT 
    id,
    meter_id,
    postcode,
    meter_read_value,
    meter_read_timestamp,
    row_number() over (partition by id order by dbt_updated_at) AS last_updated_rank
FROM {{ ref('smart_meters_snapshot') }} 
WHERE id IS NOT NULL),

weather_snapshot AS (
SELECT 
    id,
    postcode,
    date_time,
    weather,
    row_number() over (partition by id order by dbt_updated_at) AS last_updated_rank
FROM {{ ref('weather_snapshot') }}),

weather_snapshot_dedup AS (
SELECT 
  id,
  postcode,
  date_time,
  lead(date_time) over(partition by postcode order by date_time) AS next_weather_date_time,
  weather
FROM weather_snapshot
WHERE last_updated_rank = 1)

SELECT 
    smart_meters_snapshot.id,
    smart_meters_snapshot.meter_id,
    smart_meters_snapshot.postcode,
    smart_meters_snapshot.meter_read_value,
    smart_meters_snapshot.meter_read_timestamp,
    extract(date FROM meter_read_timestamp) AS meter_read_date,
    extract(month FROM meter_read_timestamp) AS meter_read_month,
    extract(hour FROM meter_read_timestamp) AS meter_read_hour,
    weather_event.weather
FROM smart_meters_snapshot
LEFT JOIN  weather_snapshot_dedup AS weather_event
ON smart_meters_snapshot.postcode = weather_event.postcode
AND (smart_meters_snapshot.meter_read_timestamp >= weather_event.date_time 
AND smart_meters_snapshot.meter_read_timestamp < coalesce(weather_event.next_weather_date_time,CURRENT_DATETIME()))
WHERE last_updated_rank = 1