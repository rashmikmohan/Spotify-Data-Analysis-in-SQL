DROP TABLE IF EXISTS spotify_staging;
CREATE TABLE spotify_staging (
    artist VARCHAR(MAX),
    track VARCHAR(MAX),
    album VARCHAR(MAX),
    album_type VARCHAR(MAX),
    danceability VARCHAR(MAX),
    energy VARCHAR(MAX),
    loudness VARCHAR(MAX),
    speechiness VARCHAR(MAX),
    acousticness VARCHAR(MAX),
    instrumentalness VARCHAR(MAX),
    liveness VARCHAR(MAX),
    valence VARCHAR(MAX),
    tempo VARCHAR(MAX),
    duration_min VARCHAR(MAX),
    title VARCHAR(MAX),
    channel VARCHAR(MAX),
    views VARCHAR(MAX),
    likes VARCHAR(MAX),
    comments VARCHAR(MAX),
    licensed VARCHAR(MAX),
    official_video VARCHAR(MAX),
    stream VARCHAR(MAX),
    energy_liveness VARCHAR(MAX),
    most_played_on VARCHAR(MAX)
);


BULK INSERT spotify_staging
FROM 'C:\Hackforge Project\Spotify\cleaned_dataset.csv'
WITH (
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n', -- or '0x0d0a' if you tried 0x0a already
	FIRSTROW = 2
);


SELECT max(duration_min) FROM spotify_staging;


-- create table
DROP TABLE IF EXISTS spotify;
CREATE TABLE spotify (
    artist VARCHAR(max),
    track VARCHAR(max),
    album VARCHAR(max),
    album_type VARCHAR(max),
    danceability FLOAT,
    energy FLOAT,
    loudness FLOAT,
    speechiness FLOAT,
    acousticness FLOAT,
    instrumentalness FLOAT,
    liveness FLOAT,
    valence FLOAT,
    tempo FLOAT,
    duration_min FLOAT,
    title VARCHAR(max),
    channel VARCHAR(max),
    views FLOAT,
    likes BIGINT,
    comments BIGINT,
    licensed BIT,
    official_video BIT,
    stream BIGINT,
    energy_liveness FLOAT,
    most_played_on VARCHAR(max)
);

INSERT INTO spotify (
    artist, track, album, album_type,
    danceability, energy, loudness, speechiness, acousticness,
    instrumentalness, liveness, valence, tempo, duration_min,
    title, channel, views, likes, comments,
    licensed, official_video, stream, energy_liveness, most_played_on
)

SELECT
    REPLACE(artist, '"', '') AS artist,
    REPLACE(track, '"', '') AS track,
    REPLACE(album, '"', '') AS album,
    REPLACE(album_type, '"', '') AS album_type,
    TRY_CAST(danceability AS DECIMAL(10, 5)),
    TRY_CAST(energy AS DECIMAL(10, 5)),
    TRY_CAST(loudness AS DECIMAL(10, 5)),
    TRY_CAST(speechiness AS DECIMAL(10, 5)),
    TRY_CAST(acousticness AS DECIMAL(10, 5)),
    TRY_CAST(instrumentalness AS DECIMAL(10, 5)),
    TRY_CAST(liveness AS DECIMAL(10, 5)),
    TRY_CAST(valence AS DECIMAL(10, 5)),
    TRY_CAST(tempo AS DECIMAL(10, 5)),
    TRY_CAST(duration_min AS DECIMAL(10, 5)),
    REPLACE(title, '"', '') AS title,
    REPLACE(channel, '"', '') AS channel,
    TRY_CAST(views AS FLOAT),
    TRY_CAST(likes AS BIGINT),
    TRY_CAST(comments AS BIGINT),
    CASE WHEN LOWER(licensed) IN ('true', '1') THEN 1 ELSE 0 END,
    CASE WHEN LOWER(official_video) IN ('true', '1') THEN 1 ELSE 0 END,
    TRY_CAST(stream AS BIGINT),
    TRY_CAST(energy_liveness AS DECIMAL(10, 5)),
    trim(REPLACE(most_played_on, '"', '')) AS most_played_on


FROM spotify_staging

;
---

--Easy Level KPIs
--Retrieve the names of all tracks that have more than 1 billion streams.
select track from spotify where stream> 1000000000 --385 Tracks

--List all albums along with their respective artists.
select distinct Album, Artist from spotify
order by 1

--Get the total number of comments for tracks where licensed = TRUE.
select sum(comments) as total_comments from spotify where licensed =1 


--Find all tracks that belong to the album type single.
select  track from spotify where album_type='single'
 
--Count the total number of tracks by each artist.
select artist, count( track) as no_of_tracks from spotify
group by artist
order by  count(track) 

--Medium Level
--Calculate the average danceability of tracks in each album.
select 
album, avg(danceability) as avg_danceability
from spotify
group by album
order by 2 desc

--Find the top 10 tracks with the highest energy values.
select top 10
 track, max(energy) as max_energy
from spotify
group by track
order by 2 desc
--List all tracks along with their views and likes where official_video = TRUE.
select track, sum(views) as total_sum, sum(likes) as total_likes   from spotify where official_video=1
group by track
order by sum(views) desc ,sum(likes) desc

--For each album, calculate the total views of all associated tracks.
select album, track, sum(views) as total_views from spotify
group by album,track
order by 3 desc

--Retrieve the track names that have been streamed on Spotify more than YouTube.
with cte as ( 
select track,
Coalesce(sum(case when most_played_on= 'Spotify' then stream end),0) as spotify_streams,
Coalesce(sum(case when most_played_on= 'Youtube' then stream end),0) as youtube_streams
from 
spotify
group by track
)

select * from cte where spotify_streams>youtube_streams and youtube_streams!=0




--Advanced Level
--Find the top 3 most-viewed tracks for each artist using window functions.
with cte as (select artist, track , sum(views) as total_view,
DENSE_RANK() over (partition by artist order by sum(views) desc) as rn
from spotify 
group by artist, track
 )

select artist, track, total_view from cte where rn<=3

--Write a query to find tracks where the liveness score is above the average.

select track, artist , liveness from spotify where liveness > (select avg(liveness) from spotify )
order by 3 desc


--Use a WITH clause to calculate the difference between the highest and lowest energy values for tracks in each album. --Useful for analyzing whether an album is consistently energetic or has a wide range (e.g. slow  vs. fast dance tracks)

with cte as (
select album,
max(energy) as max_energy,
min(energy) as min_energy
from spotify
group by album )

select album , max_energy-min_energy as energy_difference from cte
order by 2 desc

--Calculate cummulative sum of likes for tracks ordered by no of views using window function

SELECT 
    track,
    views,
    likes,
    SUM(likes) OVER (ORDER BY views) AS cumulative_likes
FROM spotify;

-- Optional - if we want to analyze it by Artist:

SELECT 
    artist,
    track,
    views,
    likes,
    SUM(likes) OVER (PARTITION BY artist ORDER BY views) AS cumulative_likes
FROM spotify
order by ;
-----