-- One row per stable_id. Curated is already deduplicated by the upsert; the
-- DISTINCT ON keeps metrics correct even if a duplicate ever slips through.
select distinct on (stable_id)
    stable_id,
    pickup_ts,
    dropoff_ts,
    pickup_ts::date as event_date,
    trip_distance,
    pu_location_id,
    fare_amount,
    total_amount
from
{% if var('use_fixtures') %}
    {{ ref('fixture_trips') }}
{% else %}
    {{ source('tlc', 'curated_trips') }}
{% endif %}
order by stable_id, pickup_ts
