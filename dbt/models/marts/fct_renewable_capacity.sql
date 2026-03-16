with a68_data as (
    -- Portugal depuis A68 (seul pays avec données agrégées)
    select
        country_code,
        DATE(capacity_date) as capacity_date,
        
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

    from {{ ref('stg_entsoe__capacity') }}
    where country_code NOT IN ('FR', 'UK', 'IT', 'NL', 'BE', 'AT', 'ES', 'DE')
    qualify ROW_NUMBER() OVER (PARTITION BY country_code ORDER BY capacity_date DESC) = 1

),

a71_data as (
    -- 7 autres pays depuis A71 (agrégé par catégorie)
    select
        country_code,
        MAX(DATE(TIMESTAMP(latest_period_start))) as capacity_date,
        
        -- Total renewable capacity (MW) - using is_renewable flag
        SUM(CASE WHEN is_renewable THEN total_capacity_mw ELSE 0 END) as total_renewable_capacity_mw,
        
        -- Total fossil capacity (MW)
        SUM(CASE WHEN energy_type LIKE 'Fossil%' THEN total_capacity_mw ELSE 0 END) as total_fossil_capacity_mw,
        
        -- Total nuclear capacity (MW)
        SUM(CASE WHEN energy_type = 'Nuclear' THEN total_capacity_mw ELSE 0 END) as total_nuclear_capacity_mw,
        
        -- Total other capacity (MW)
        SUM(CASE 
            WHEN NOT is_renewable 
                AND energy_type != 'Nuclear' 
                AND energy_type NOT LIKE 'Fossil%' 
            THEN total_capacity_mw 
            ELSE 0 
        END) as total_other_capacity_mw

    from {{ ref('fct_capacity_per_unit') }}
    
    -- Filters:
    -- 1. Exclude PT: A68 has more complete data for Portugal (21k MW vs 10k MW in A71)
    --    TODO: If A71 data for PT improves, compare totals and use the better source
    -- 2. Only include countries present in dim_countries (built from A75 generation data)
    --    This ensures consistency across all dashboard tabs
    where country_code != 'PT'  -- PT is more complete in A68
        AND country_code IN (SELECT country_code FROM {{ ref('dim_countries') }})
    group by country_code

),

combined as (
    -- Union A68 (Portugal) + A71 (7 autres pays)
    select * from a68_data
    union all
    select * from a71_data
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

    from combined

)

select * from capacity_mix