{{
    config(
        materialized='table'
    )
}}

with bike_points as (
    select * from {{ ref('stg_bike_points') }}
),

availability_metrics as (
    select
        id as bike_point_id,
        common_name,
        terminal_name,
        lat,
        lon,
        -- Cast to proper types
        cast(nb_bikes as integer) as bikes_available,
        cast(nb_standard_bikes as integer) as standard_bikes_available,
        cast(nb_ebikes as integer) as ebikes_available,
        cast(nb_empty_docks as integer) as empty_docks,
        cast(nb_docks as integer) as total_docks,

        -- Calculate utilisation metrics
        case
            when cast(nb_docks as integer) > 0
            then round(cast(nb_bikes as float) / cast(nb_docks as float) * 100, 2)
            else 0
        end as availability_pct,

        case
            when cast(nb_docks as integer) > 0
            then round(cast(nb_empty_docks as float) / cast(nb_docks as float) * 100, 2)
            else 0
        end as utilisation_pct,

        -- Categorise availability status
        case
            when cast(nb_bikes as integer) = 0 then 'Empty'
            when cast(nb_empty_docks as integer) = 0 then 'Full'
            when cast(nb_bikes as float) / cast(nb_docks as float) > 0.7 then 'High'
            when cast(nb_bikes as float) / cast(nb_docks as float) > 0.3 then 'Medium'
            else 'Low'
        end as availability_status,

        -- Boolean flags
        installed = 'true' as is_installed,
        locked = 'true' as is_locked,
        temporary = 'true' as is_temporary,

        -- Convert install_date from Unix timestamp (milliseconds) to date
        case
            when install_date != '' and install_date is not null
            then to_timestamp(cast(install_date as bigint) / 1000)
            else null
        end as install_timestamp,

        dbt_updated_at
    from bike_points
    where nb_docks is not null
      and nb_docks != ''
)

select * from availability_metrics
