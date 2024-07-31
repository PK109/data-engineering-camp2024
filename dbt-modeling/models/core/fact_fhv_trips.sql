{{
    config(
        materialized='table'
    )

}}


with dim_zones as(
            select * from {{ref('dim_zones')}}
            where borough != 'Unknown'
        )
        
select
    tripid,
    dispatching_base_num,	
    pickup_datetime,	
    dropoff_datetime,	
    pulocationid,		
    dolocationid,		
    sr_flag,	
    affiliated_base_number,
    d1.borough as pickup_borough,
    d2.borough as dropoff_borough
from
    {{ref('stg_fhv_trip_data')}} f
inner join dim_zones d1
    on f.pulocationid = d1.locationid
inner join dim_zones d2
    on f.dolocationid = d2.locationid
