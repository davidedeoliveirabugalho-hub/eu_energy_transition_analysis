with capacity_data as (
    
    select * from {{ ref('stg_entsoe__capacity_per_unit') }}

),

aggregated as (

    select
        country_code,
        energy_type,
        is_renewable,
        
        -- Aggregate capacity by country and energy type
        sum(installed_capacity_mw) as total_capacity_mw,
        count(distinct unit_id) as number_of_units,
        
        -- Keep latest period info
        max(period_start) as latest_period_start,
        max(period_end) as latest_period_end

    from capacity_data
    group by country_code, energy_type, is_renewable

)

select 
    country_code,
    energy_type,
    is_renewable,
    total_capacity_mw,
    number_of_units,
    latest_period_start,
    latest_period_end
from aggregated
order by country_code, total_capacity_mw desc