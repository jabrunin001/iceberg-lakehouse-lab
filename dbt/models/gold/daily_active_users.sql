select
    cast(event_ts as date)  as activity_date,
    count(distinct user_id) as active_users,
    count(*)                as total_events
from {{ ref('fact_playback_event') }}
group by cast(event_ts as date)
