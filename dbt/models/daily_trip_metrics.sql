select
    event_date,
    count(*) as trip_count,
    count(*) filter (where fare_amount > 0) as paid_trip_count,
    coalesce(sum(fare_amount) filter (where fare_amount > 0), 0) as total_fare,
    sum(trip_distance) as total_distance
from {{ ref('stg_trips') }}
group by event_date
