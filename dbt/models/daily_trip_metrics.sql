select
    event_date,
    count(*) as trip_count,
    count(*) filter (where fare_amount > 0) as paid_trip_count,
    coalesce(sum(fare_amount) filter (where fare_amount > 0), 0) as total_fare,
    round(avg(fare_amount) filter (where fare_amount > 0), 2) as avg_paid_fare,
    sum(trip_distance) as total_distance,
    round(avg(trip_distance)::numeric, 2) as avg_trip_distance
from {{ ref('stg_trips') }}
group by event_date
