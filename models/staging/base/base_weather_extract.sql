SELECT  
    concat(postcode,date_time) AS id,
    postcode,
    cast(datetime AS datetime) AS date_time,
    weather
FROM `uw-pipeline.uw_raw.weather_extract` 