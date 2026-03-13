

with source as (
    
    select * from {{ source('bronze', 'bronze_entsoe_a75_actual_generation') }}

),

renamed as (

    select
        -- Metadata
        country_code,
        document_type,
        ingestion_timestamp,
        
        -- Timestamp
        cast(index as timestamp) as generation_datetime,
        
        -- Renewable generation (MW)
        biomass_actual_aggregated as biomass_mw,
        hydro_pumped_storage_actual_aggregated as hydro_pumped_storage_mw,
        hydro_run_of_river_and_poundage_actual_aggregated as hydro_run_of_river_mw,
        hydro_water_reservoir_actual_aggregated as hydro_water_reservoir_mw,
        solar_actual_aggregated as solar_mw,
        wind_onshore_actual_aggregated as wind_onshore_mw,
        wind_offshore_actual_aggregated as wind_offshore_mw,
        marine_actual_aggregated as marine_mw,
        geothermal_actual_aggregated as geothermal_mw,
        other_renewable_actual_aggregated as other_renewable_mw,
        
        -- Fossil generation (MW)
        fossil_brown_coal_lignite_actual_aggregated as coal_lignite_mw,
        fossil_coal_derived_gas_actual_aggregated as coal_derived_gas_mw,
        fossil_gas_actual_aggregated as gas_mw,
        fossil_hard_coal_actual_aggregated as hard_coal_mw,
        fossil_oil_actual_aggregated as oil_mw,
        fossil_oil_shale_actual_aggregated as oil_shale_mw,
        fossil_peat_actual_aggregated as peat_mw,
        
        -- Nuclear (MW)
        nuclear_actual_aggregated as nuclear_mw,
        
        -- Other (MW)
        waste_actual_aggregated as waste_mw,
        other_actual_aggregated as other_mw,
        energy_storage_actual_aggregated as energy_storage_mw,
        
        -- Consumption (MW) - for pumped storage, etc.
        biomass_actual_consumption as biomass_consumption_mw,
        hydro_pumped_storage_actual_consumption as hydro_pumped_storage_consumption_mw,
        hydro_run_of_river_and_poundage_actual_consumption as hydro_run_of_river_consumption_mw,
        hydro_water_reservoir_actual_consumption as hydro_water_reservoir_consumption_mw,
        solar_actual_consumption as solar_consumption_mw,
        wind_onshore_actual_consumption as wind_onshore_consumption_mw,
        wind_offshore_actual_consumption as wind_offshore_consumption_mw,
        nuclear_actual_consumption as nuclear_consumption_mw,
        fossil_gas_actual_consumption as gas_consumption_mw,
        fossil_hard_coal_actual_consumption as hard_coal_consumption_mw,
        fossil_oil_actual_consumption as oil_consumption_mw,
        waste_actual_consumption as waste_consumption_mw,
        other_actual_consumption as other_consumption_mw,
        energy_storage_actual_consumption as energy_storage_consumption_mw

    from source

)

select * from renamed