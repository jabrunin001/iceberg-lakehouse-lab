with src as (select * from demo.bronze.movies)
select
    cast(movieId as bigint)                                     as movie_id,
    title,
    -- last parenthesised 4-digit group is the release year
    try_cast(regexp_extract(title, '\\((\\d{4})\\)', 1) as int) as release_year,
    split(genres, '\\|')                                        as genres
from src
where movieId is not null
