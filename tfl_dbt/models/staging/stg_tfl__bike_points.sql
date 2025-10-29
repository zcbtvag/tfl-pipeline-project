{{
    config(
        materialized='view'
    )
}}

with source as (
    select * from {{ source('raw', 'bike_points') }}
),

-- Extract nested properties from additionalProperties array
flattened as (
    select
        id,
        url,
        "commonName" as common_name,
        "placeType" as place_type,
        lat,
        lon,
        -- Extract values from additionalProperties array
        list_filter("additionalProperties", x -> json_extract_string(x, '$.key') = 'TerminalName')[1]->>'$.value' as terminal_name,
        list_filter("additionalProperties", x -> json_extract_string(x, '$.key') = 'Installed')[1]->>'$.value' as installed,
        list_filter("additionalProperties", x -> json_extract_string(x, '$.key') = 'Locked')[1]->>'$.value' as locked,
        list_filter("additionalProperties", x -> json_extract_string(x, '$.key') = 'InstallDate')[1]->>'$.value' as install_date,
        list_filter("additionalProperties", x -> json_extract_string(x, '$.key') = 'RemovalDate')[1]->>'$.value' as removal_date,
        list_filter("additionalProperties", x -> json_extract_string(x, '$.key') = 'Temporary')[1]->>'$.value' as temporary,
        list_filter("additionalProperties", x -> json_extract_string(x, '$.key') = 'NbBikes')[1]->>'$.value' as nb_bikes,
        list_filter("additionalProperties", x -> json_extract_string(x, '$.key') = 'NbEmptyDocks')[1]->>'$.value' as nb_empty_docks,
        list_filter("additionalProperties", x -> json_extract_string(x, '$.key') = 'NbDocks')[1]->>'$.value' as nb_docks,
        list_filter("additionalProperties", x -> json_extract_string(x, '$.key') = 'NbStandardBikes')[1]->>'$.value' as nb_standard_bikes,
        list_filter("additionalProperties", x -> json_extract_string(x, '$.key') = 'NbEBikes')[1]->>'$.value' as nb_ebikes,
        ingested_at
        current_timestamp as dbt_updated_at
    from source
)

select * from flattened
