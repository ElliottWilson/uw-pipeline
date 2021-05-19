{% snapshot weather_snapshot %}

    {{
        config(
          unique_key='id', 
          strategy='check',
          target_schema='snapshot',
          check_cols='all'
        )
    }}
    
SELECT
    *
FROM {{ ref('base_weather_extract') }}

{% endsnapshot %}