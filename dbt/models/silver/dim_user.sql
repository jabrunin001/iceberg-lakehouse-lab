with rating_users as (
    select distinct cast(userId as bigint) as user_id from demo.bronze.ratings
),
event_users as (
    select distinct user_id from demo.bronze.playback_events
)
select user_id from rating_users
union
select user_id from event_users
