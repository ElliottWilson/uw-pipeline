SELECT  
    concat(postcode,datetime) AS id,
    postcode,
    cast(datetime AS datetime) AS date_time,
    weather
FROM {{ source('raw', 'weather_extract') }} 