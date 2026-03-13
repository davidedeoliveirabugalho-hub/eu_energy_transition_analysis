

with source as (
    
    select * from {{ source('bronze', 'bronze_entsoe_a68_installed_capacity') }}

),

renamed as (

    select
        -- Metadata
        country_code,
        document_type,
        ingestion_timestamp,
        
        -- Timestamp
        cast(index as timestamp) as capacity_date,
        
        -- Installed capacity by type (MW)
        biomass as biomass_capacity_mw,
        energy_storage as energy_storage_capacity_mw,
        fossil_brown_coal_lignite as coal_lignite_capacity_mw,
        fossil_coal_derived_gas as coal_derived_gas_capacity_mw,
        fossil_gas as gas_capacity_mw,
        fossil_hard_coal as hard_coal_capacity_mw,
        fossil_oil as oil_capacity_mw,
        fossil_oil_shale as oil_shale_capacity_mw,
        fossil_peat as peat_capacity_mw,
        geothermal as geothermal_capacity_mw,
        hydro_pumped_storage as hydro_pumped_storage_capacity_mw,
        hydro_run_of_river_and_poundage as hydro_run_of_river_capacity_mw,
        hydro_water_reservoir as hydro_water_reservoir_capacity_mw,
        marine as marine_capacity_mw,
        nuclear as nuclear_capacity_mw,
        other as other_capacity_mw,
        other_renewable as other_renewable_capacity_mw,
        solar as solar_capacity_mw,
        waste as waste_capacity_mw,
        wind_offshore as wind_offshore_capacity_mw,
        wind_onshore as wind_onshore_capacity_mw

    from source

)

select * from renamed
