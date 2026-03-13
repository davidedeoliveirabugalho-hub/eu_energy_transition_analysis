
with daily_generation as (
    
    select * from {{ ref('int_generation_daily') }}

),

energy_totals as (

    select
        country_code,
        generation_date,
        
        -- Total renewables (MWh)
        (
            coalesce(biomass_mwh, 0) +
            coalesce(hydro_pumped_storage_mwh, 0) +
            coalesce(hydro_run_of_river_mwh, 0) +
            coalesce(hydro_water_reservoir_mwh, 0) +
            coalesce(solar_mwh, 0) +
            coalesce(wind_onshore_mwh, 0) +
            coalesce(wind_offshore_mwh, 0) +
            coalesce(marine_mwh, 0) +
            coalesce(geothermal_mwh, 0) +
            coalesce(other_renewable_mwh, 0)
        ) as total_renewable_mwh,
        
        -- Total fossil (MWh)
        (
            coalesce(coal_lignite_mwh, 0) +
            coalesce(coal_derived_gas_mwh, 0) +
            coalesce(gas_mwh, 0) +
            coalesce(hard_coal_mwh, 0) +
            coalesce(oil_mwh, 0) +
            coalesce(oil_shale_mwh, 0) +
            coalesce(peat_mwh, 0)
        ) as total_fossil_mwh,
        
        -- Total nuclear (MWh)
        coalesce(nuclear_mwh, 0) as total_nuclear_mwh,
        
        -- Total other (MWh)
        (
            coalesce(waste_mwh, 0) +
            coalesce(other_mwh, 0) +
            coalesce(energy_storage_mwh, 0)
        ) as total_other_mwh

    from daily_generation

),

energy_mix as (

    select
        country_code,
        generation_date,
        
        -- Totals (MWh)
        total_renewable_mwh,
        total_fossil_mwh,
        total_nuclear_mwh,
        total_other_mwh,
        
        -- Total production (MWh)
        (
            total_renewable_mwh +
            total_fossil_mwh +
            total_nuclear_mwh +
            total_other_mwh
        ) as total_production_mwh,
        
        -- Percentages with CASE WHEN protection
        CASE 
            WHEN (total_renewable_mwh + total_fossil_mwh + total_nuclear_mwh + total_other_mwh) = 0 
            THEN NULL
            ELSE round((total_renewable_mwh / (total_renewable_mwh + total_fossil_mwh + total_nuclear_mwh + total_other_mwh)) * 100, 2)
        END as renewable_percentage,
        
        CASE 
            WHEN (total_renewable_mwh + total_fossil_mwh + total_nuclear_mwh + total_other_mwh) = 0 
            THEN NULL
            ELSE round((total_fossil_mwh / (total_renewable_mwh + total_fossil_mwh + total_nuclear_mwh + total_other_mwh)) * 100, 2)
        END as fossil_percentage,
        
        CASE 
            WHEN (total_renewable_mwh + total_fossil_mwh + total_nuclear_mwh + total_other_mwh) = 0 
            THEN NULL
            ELSE round((total_nuclear_mwh / (total_renewable_mwh + total_fossil_mwh + total_nuclear_mwh + total_other_mwh)) * 100, 2)
        END as nuclear_percentage,
        
        CASE 
            WHEN (total_renewable_mwh + total_fossil_mwh + total_nuclear_mwh + total_other_mwh) = 0 
            THEN NULL
            ELSE round((total_other_mwh / (total_renewable_mwh + total_fossil_mwh + total_nuclear_mwh + total_other_mwh)) * 100, 2)
        END as other_percentage

    from energy_totals

)

select * from energy_mix
