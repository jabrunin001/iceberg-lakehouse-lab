select
    event_id,
    cast(user_id as bigint)       as user_id,
    cast(movie_id as bigint)      as movie_id,
    event_type,
    event_ts,
    device,
    cast(position_seconds as int) as position_seconds
from demo.bronze.playback_events
where event_id is not null
