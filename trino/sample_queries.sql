-- Top 10 most-watched titles by unique viewers
SELECT title, unique_viewers, completion_rate, avg_rating
FROM demo.gold.movie_engagement
WHERE play_events > 0
ORDER BY unique_viewers DESC
LIMIT 10;

-- Daily active users trend
SELECT activity_date, active_users, total_events
FROM demo.gold.daily_active_users
ORDER BY activity_date;

-- Time-travel: inspect snapshot history of the events table
SELECT snapshot_id, committed_at, operation
FROM demo.bronze."playback_events$snapshots"
ORDER BY committed_at;
