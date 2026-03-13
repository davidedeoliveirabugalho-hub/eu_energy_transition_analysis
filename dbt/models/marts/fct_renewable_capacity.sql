

with capacity_data as (
    
    select * from {{ ref('stg_entsoe__capacity') }}

),

capacity_totals as (

    select
        country_code,
        capacity_date,
        
        -- Total renewable capacity (MW)
        (
            coalesce(biomass_capacity_mw, 0) +
            coalesce(hydro_pumped_storage_capacity_mw, 0) +
            coalesce(hydro_run_of_river_capacity_mw, 0) +
            coalesce(hydro_water_reservoir_capacity_mw, 0) +
            coalesce(solar_capacity_mw, 0) +
            coalesce(wind_onshore_capacity_mw, 0) +
            coalesce(wind_offshore_capacity_mw, 0) +
            coalesce(marine_capacity_mw, 0) +
            coalesce(geothermal_capacity_mw, 0) +
            coalesce(other_renewable_capacity_mw, 0)
        ) as total_renewable_capacity_mw,
        
        -- Total fossil capacity (MW)
        (
            coalesce(coal_lignite_capacity_mw, 0) +
            coalesce(coal_derived_gas_capacity_mw, 0) +
            coalesce(gas_capacity_mw, 0) +
            coalesce(hard_coal_capacity_mw, 0) +
            coalesce(oil_capacity_mw, 0) +
            coalesce(oil_shale_capacity_mw, 0) +
            coalesce(peat_capacity_mw, 0)
        ) as total_fossil_capacity_mw,
        
        -- Total nuclear capacity (MW)
        coalesce(nuclear_capacity_mw, 0) as total_nuclear_capacity_mw,
        
        -- Total other capacity (MW)
        (
            coalesce(waste_capacity_mw, 0) +
            coalesce(other_capacity_mw, 0) +
            coalesce(energy_storage_capacity_mw, 0)
        ) as total_other_capacity_mw

    from capacity_data

),

capacity_mix as (

    select
        country_code,
        capacity_date,
        
        -- Totals (MW)
        total_renewable_capacity_mw,
        total_fossil_capacity_mw,
        total_nuclear_capacity_mw,
        total_other_capacity_mw,
        
        -- Total installed capacity (MW)
        (
            total_renewable_capacity_mw +
            total_fossil_capacity_mw +
            total_nuclear_capacity_mw +
            total_other_capacity_mw
        ) as total_installed_capacity_mw,
        
        -- Percentages with CASE WHEN protection
        CASE 
            WHEN (total_renewable_capacity_mw + total_fossil_capacity_mw + total_nuclear_capacity_mw + total_other_capacity_mw) = 0 
            THEN NULL
            ELSE round((total_renewable_capacity_mw / (total_renewable_capacity_mw + total_fossil_capacity_mw + total_nuclear_capacity_mw + total_other_capacity_mw)) * 100, 2)
        END as renewable_capacity_percentage,
        
        CASE 
            WHEN (total_renewable_capacity_mw + total_fossil_capacity_mw + total_nuclear_capacity_mw + total_other_capacity_mw) = 0 
            THEN NULL
            ELSE round((total_fossil_capacity_mw / (total_renewable_capacity_mw + total_fossil_capacity_mw + total_nuclear_capacity_mw + total_other_capacity_mw)) * 100, 2)
        END as fossil_capacity_percentage,
        
        CASE 
            WHEN (total_renewable_capacity_mw + total_fossil_capacity_mw + total_nuclear_capacity_mw + total_other_capacity_mw) = 0 
            THEN NULL
            ELSE round((total_nuclear_capacity_mw / (total_renewable_capacity_mw + total_fossil_capacity_mw + total_nuclear_capacity_mw + total_other_capacity_mw)) * 100, 2)
        END as nuclear_capacity_percentage,
        
        CASE 
            WHEN (total_renewable_capacity_mw + total_fossil_capacity_mw + total_nuclear_capacity_mw + total_other_capacity_mw) = 0 
            THEN NULL
            ELSE round((total_other_capacity_mw / (total_renewable_capacity_mw + total_fossil_capacity_mw + total_nuclear_capacity_mw + total_other_capacity_mw)) * 100, 2)
        END as other_capacity_percentage

    from capacity_totals

)

select * from capacity_mix
