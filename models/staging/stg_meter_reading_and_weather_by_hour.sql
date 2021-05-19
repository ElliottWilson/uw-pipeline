SELECT 
    meter_read_date,
    meter_read_month,
    meter_read_hour,
    postcode, 
    weather,
    meter_id,
    meter_read_value,
    count(distinct meter_read_hour) over(partition by meter_read_month, postcode, weather) AS nbr_times_weather_in_mth,
    count(meter_id) over(partition by meter_read_hour, postcode, weather) AS meter_reading_in_hour
FROM {{ ref('stg_meter_reading_join_weather_event')}}