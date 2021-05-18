SELECT  
    concat(meter_id,meter_read_timestamp) AS id,
    meter_id,
    postcode,
    meter_read_value,
    cast(meter_read_timestamp AS datetime) AS meter_read_timestamp
FROM `uw-pipeline.uw_raw.smart_meters_extract` 