{{
    config(
        materialized='table'
    )
}}

select * from {{ ref('fct_tfl__bike_points') }}
qualify row_number() over (partition by bike_point_id order by hour_timestamp desc) = 1
