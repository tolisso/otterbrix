-- Query 1: Count events by collection type
-- In document_table, JSON paths are automatically extracted as columns
SELECT "commit.collection" AS event, COUNT(*) AS count 
FROM bluesky 
GROUP BY event 
ORDER BY count DESC;

-- Query 2: Count events and unique users by collection
SELECT "commit.collection" AS event, 
       COUNT(*) AS count, 
       COUNT(DISTINCT did) AS users 
FROM bluesky 
WHERE kind = 'commit' 
  AND "commit.operation" = 'create' 
GROUP BY event 
ORDER BY count DESC;

-- Query 3: Event counts by hour of day
SELECT "commit.collection" AS event,
       EXTRACT(HOUR FROM TO_TIMESTAMP(CAST(time_us AS BIGINT) / 1000000)) AS hour_of_day,
       COUNT(*) AS count 
FROM bluesky 
WHERE kind = 'commit' 
  AND "commit.operation" = 'create' 
  AND "commit.collection" IN ('app.bsky.feed.post', 'app.bsky.feed.repost', 'app.bsky.feed.like') 
GROUP BY event, hour_of_day 
ORDER BY hour_of_day, event;

-- Query 4: First 3 users to post
SELECT did AS user_id,
       TO_TIMESTAMP(CAST(MIN(time_us) AS BIGINT) / 1000000) AS first_post_date 
FROM bluesky 
WHERE kind = 'commit' 
  AND "commit.operation" = 'create' 
  AND "commit.collection" = 'app.bsky.feed.post' 
GROUP BY user_id 
ORDER BY first_post_date ASC 
LIMIT 3;

-- Query 5: Top 3 users by activity span
SELECT did AS user_id,
       (CAST(MAX(time_us) AS BIGINT) - CAST(MIN(time_us) AS BIGINT)) / 1000 AS activity_span_ms 
FROM bluesky 
WHERE kind = 'commit' 
  AND "commit.operation" = 'create' 
  AND "commit.collection" = 'app.bsky.feed.post' 
GROUP BY user_id 
ORDER BY activity_span_ms DESC 
LIMIT 3;
