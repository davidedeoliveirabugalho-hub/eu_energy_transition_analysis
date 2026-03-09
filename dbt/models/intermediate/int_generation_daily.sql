{{
    config(
        materialized='table',
        schema='silver'
    )
}}

with generation_data as (
    
    select * from {{ ref('stg_entsoe__generation') }}

),

daily_aggregated as (

    select
        -- Dimensions
        country_code,
        date(generation_datetime) as generation_date,
        
        -- Renewable generation (MWh = MW × hours)
        sum(coalesce(biomass_mw, 0)) as biomass_mwh,
        sum(coalesce(hydro_pumped_storage_mw, 0)) as hydro_pumped_storage_mwh,
        sum(coalesce(hydro_run_of_river_mw, 0)) as hydro_run_of_river_mwh,
        sum(coalesce(hydro_water_reservoir_mw, 0)) as hydro_water_reservoir_mwh,
        sum(coalesce(solar_mw, 0)) as solar_mwh,
        sum(coalesce(wind_onshore_mw, 0)) as wind_onshore_mwh,
        sum(coalesce(wind_offshore_mw, 0)) as wind_offshore_mwh,
        sum(coalesce(marine_mw, 0)) as marine_mwh,
        sum(coalesce(geothermal_mw, 0)) as geothermal_mwh,
        sum(coalesce(other_renewable_mw, 0)) as other_renewable_mwh,
        
        -- Fossil generation (MWh)
        sum(coalesce(coal_lignite_mw, 0)) as coal_lignite_mwh,
        sum(coalesce(coal_derived_gas_mw, 0)) as coal_derived_gas_mwh,
        sum(coalesce(gas_mw, 0)) as gas_mwh,
        sum(coalesce(hard_coal_mw, 0)) as hard_coal_mwh,
        sum(coalesce(oil_mw, 0)) as oil_mwh,
        sum(coalesce(oil_shale_mw, 0)) as oil_shale_mwh,
        sum(coalesce(peat_mw, 0)) as peat_mwh,
        
        -- Nuclear (MWh)
        sum(coalesce(nuclear_mw, 0)) as nuclear_mwh,
        
        -- Other (MWh)
        sum(coalesce(waste_mw, 0)) as waste_mwh,
        sum(coalesce(other_mw, 0)) as other_mwh,
        sum(coalesce(energy_storage_mw, 0)) as energy_storage_mwh,
        
        -- Metadata
        count(*) as hourly_records_count

    from generation_data
    group by country_code, date(generation_datetime)

)

select * from daily_aggregated
