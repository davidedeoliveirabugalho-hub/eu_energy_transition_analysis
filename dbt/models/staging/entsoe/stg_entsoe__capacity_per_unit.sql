with source as (
    
    select * from {{ source('bronze', 'bronze_entsoe_a71_installed_capacity_per_unit') }}

),

renamed as (

    select
        -- Identifiers
        country_code,
        unit_id,
        unit_name,
        
        -- Energy classification
        psr_type,
        energy_type,
        is_renewable,
        
        -- Capacity
        installed_capacity_mw,
        
        -- Time period
        period_start,
        period_end,
        
        -- Location (if available)
        latitude,
        longitude,
        
        -- Metadata
        document_type,
        ingestion_timestamp

    from source

)

select * from renamed