{{
    config(
        materialized='table'
    )
}}

with source as (
    
    select * from {{ source('bronze', 'bronze_entsoe_a73_generation_forecast') }}

),

renamed as (

    select
        -- Metadata
        country_code,
        document_type,
        ingestion_timestamp,
        
        -- Timestamp
        cast(index as timestamp) as forecast_datetime,
        
        -- All other columns (hundreds of power plant-specific forecasts)
        * except (country_code, document_type, ingestion_timestamp, index)

    from source

)

select * from renamed