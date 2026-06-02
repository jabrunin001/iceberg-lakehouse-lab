with plays as (
    select
        movie_id,
        count(*)                                                as play_events,
        count(distinct user_id)                                 as unique_viewers,
        sum(case when event_type = 'complete' then 1 else 0 end) as completes
    from {{ ref('fact_playback_event') }}
    group by movie_id
),
ratings as (
    select movie_id, avg(rating) as avg_rating, count(*) as rating_count
    from {{ ref('fact_rating') }}
    group by movie_id
)
select
    m.movie_id,
    m.title,
    coalesce(p.play_events, 0)    as play_events,
    coalesce(p.unique_viewers, 0) as unique_viewers,
    case when coalesce(p.play_events, 0) = 0 then 0.0
         else cast(p.completes as double) / p.play_events end as completion_rate,
    r.avg_rating,
    coalesce(r.rating_count, 0)   as rating_count
from {{ ref('dim_movie') }} m
left join plays   p on m.movie_id = p.movie_id
left join ratings r on m.movie_id = r.movie_id
