{{
    config(
        materialized='table'
    )
}}

with bike_points as (
    select * from {{ ref('stg_tfl__bike_points') }}
    where ingested_at >= current_timestamp - interval '24 hours'
),

hourly_aggregated as (
    select
        id as bike_point_id,
        common_name,
        terminal_name,
        lat,
        lon,

        -- Aggregate to hourly buckets
        date_trunc('hour', ingested_at) as hour_timestamp,


        -- Cast to proper types and aggregate by hour
        round(avg(cast(nb_bikes as integer)), 1) as avg_bikes_available,
        round(avg(cast(nb_standard_bikes as integer)), 1) as avg_standard_bikes_available,
        round(avg(cast(nb_ebikes as integer)), 1) as avg_ebikes_available,
        round(avg(cast(nb_empty_docks as integer)), 1) as avg_empty_docks,
        round(avg(cast(nb_docks as integer)), 1) as avg_total_docks,

        -- Agregate utilisation metrics
        case
            when avg(cast(nb_docks as integer)) > 0
            then round(avg(cast(nb_bikes as float)) / avg(cast(nb_docks as float)) * 100, 2)
            else 0
        end as avg_availability_pct,

        case
            when avg(cast(nb_docks as integer)) > 0
            then round(avg(cast(nb_empty_docks as float)) / avg(cast(nb_docks as float)) * 100, 2)
            else 0
        end as avg_utilisation_pct,

        -- Categorise availability status
        case
            when avg(cast(nb_bikes as float)) = 0 then 'Empty'
            when avg(cast(nb_empty_docks as integer)) = 0 then 'Full'
            when avg(cast(nb_bikes as float)) / avg(cast(nb_docks as float)) > 0.7 then 'High'
            when avg(cast(nb_bikes as float)) / avg(cast(nb_docks as float)) > 0.3 then 'Medium'
            else 'Low'
        end as availability_status,

        -- Boolean flags
        max(installed) = 'true' as is_installed,
        max(locked) = 'true' as is_locked,
        max(temporary) = 'true' as is_temporary,

        -- Convert install_date from Unix timestamp (milliseconds) to date
        max(case
            when install_date != '' and install_date is not null
            then to_timestamp(cast(install_date as bigint) / 1000)
            else null
        end) as install_timestamp
    
    from bike_points
    group by
        id,
        common_name,
        terminal_name,
        lat,
        lon,
        date_trunc('hour', ingested_at)

)

select * from hourly_aggregated
order by bike_point_id, hour_timestamp desc
