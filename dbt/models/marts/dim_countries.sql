
with countries_list as (
    
    select distinct
        country_code
    from {{ ref('int_generation_daily') }}

),

countries_enriched as (

    select
        country_code,
        
        -- Country names (à enrichir plus tard si besoin)
        case country_code
            when 'AT' then 'Austria'
            when 'BE' then 'Belgium'
            when 'DE' then 'Germany'
            when 'ES' then 'Spain'
            when 'FR' then 'France'
            when 'IT' then 'Italy'
            when 'NL' then 'Netherlands'
            when 'PT' then 'Portugal'
            else country_code
        end as country_name

    from countries_list

)

select * from countries_enriched
order by country_code
