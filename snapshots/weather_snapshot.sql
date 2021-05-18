{% snapshot weather_snapshot %}

    {{
        config(
          unique_key='id', 
          strategy='check',
          check_cols='all'
        )
    }}
    
SELECT
    *
FROM {{ ref('base_weather_extract') }}

{% endsnapshot %}