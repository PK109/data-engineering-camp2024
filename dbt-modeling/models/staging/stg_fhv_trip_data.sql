with 

source as (

    select * from {{ source('staging', 'fhv_trip_data') }}

),

renamed as (

    select
        {{ dbt_utils.generate_surrogate_key(['dispatching_base_num', 'pickup_datetime']) }} as tripid,    
        dispatching_base_num,
        pickup_datetime,
        cast(dropoff_datetime as timestamp) as dropoff_datetime,
        {{ dbt.safe_cast("pulocationid", api.Column.translate_type("integer")) }} as pulocationid,
        {{ dbt.safe_cast("dolocationid", api.Column.translate_type("integer")) }} as dolocationid,
        {{ dbt.safe_cast("sr_flag", api.Column.translate_type("integer")) }} as sr_flag,
        affiliated_base_number

    from source

)

select * from renamed
where cast( {{ dbt.date_trunc("year", 'pickup_datetime') }} as date) =  {{dbt.date("2019","01","01")}}

-- dbt build --select <model_name> --vars '{'is_test_run': 'false'}'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}