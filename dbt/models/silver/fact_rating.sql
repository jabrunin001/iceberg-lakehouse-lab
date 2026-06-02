select
    cast(userId as bigint)                      as user_id,
    cast(movieId as bigint)                     as movie_id,
    cast(rating as double)                      as rating,
    cast(from_unixtime(timestamp) as timestamp) as rated_at
from demo.bronze.ratings
where rating is not null
