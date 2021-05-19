SELECT 
    meter_read_month,
    postcode, 
    weather,
    meter_reading_in_hour,
    nbr_times_weather_in_mth,
    meter_reading_in_hour/nbr_times_weather_in_mth AS nbr_readings_per_hour
FROM {{ ref('stg_meter_reading_and_weather_by_hour') }} 
GROUP BY 1,2,3,4,5