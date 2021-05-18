{% snapshot smart_meters_snapshot %}

    {{
        config(
          unique_key='id',  
          strategy='check',
          check_cols='all'
        )
    }}
    
SELECT
    *
FROM {{ ref('base_smart_meters_extract') }}

{% endsnapshot %}