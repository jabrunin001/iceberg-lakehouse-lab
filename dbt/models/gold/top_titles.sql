select movie_id, title, play_events, unique_viewers, completion_rate, avg_rating
from {{ ref('movie_engagement') }}
where play_events > 0
order by unique_viewers desc, play_events desc
limit 100
