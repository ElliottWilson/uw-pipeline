{% snapshot smart_meters_snapshot %}

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
FROM {{ ref('base_smart_meters_extract') }}

{% endsnapshot %}