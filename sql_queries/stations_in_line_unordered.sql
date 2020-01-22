/* find all stations that trains of a particular line visited */
WITH cnts as (
SELECT ts.stop_id, COUNT(t.unique_num) as cnt
FROM public."Trains_stopped" AS ts
JOIN public."Train" AS t on (ts.train_unique_num = t.unique_num)
    JOIN public."Trip_update" as tup ON (tup.train_unique_num = t.unique_num)
WHERE t.route_id = '{}' AND tup.direction = '{}' AND ts.stop_time > '{}' AND ts.stop_time < '{}'
GROUP BY ts.stop_id
),
/* plan: find the intersection of all trains that visited these
stations. Then sum up the timestamps of these trains at all stations
and sort by that sum -> order of stations.
Problem: not all of these stations are canonical to the line; some were
visited by a stray train. So we have to filter. */

maxval as (
SELECT MAX(cnts.cnt) as maxval
FROM cnts
),

filtered_stations as (
SELECT * FROM cnts 
WHERE cnt/(SELECT maxval FROM maxval)::float > 0.05
)

SELECT stop_id FROM filtered_stations

/* find trains that visited all of the filtered stations 
ourTrains as (
SELECT t.unique_num, ts.stop_id
FROM public."Trains_stopped" AS ts
JOIN public."Train" AS t on (ts.train_unique_num = t.unique_num)
    JOIN public."Trip_update" as tup ON (tup.train_unique_num = t.unique_num)
WHERE t.route_id = '2' AND tup.direction = 'N'
)

SELECT dist.unique_num, COUNT(dist.stop_id) as cnt FROM (
SELECT DISTINCT oT.unique_num, fs.stop_id, oT.stop_time
 FROM filtered_stations AS fs INNER JOIN ourTrains AS oT
    ON (fs.stop_id = oT.stop_id)
) as dist
GROUP BY unique_num
LIMIT 50


SELECT oT.unique_num, fs.stop_id
FROM filtered_stations AS fs INNER JOIN ourTrains AS oT
    ON (fs.stop_id = oT.stop_id)
WHERE oT.unique_num = '20190710: 02 0755+ FLA/241'
ORDER BY fs.stop_id

GROUP BY oT.unique_num


GROUP BY oT.unique_num
LIMIT 50



SELECT fs.stop_id, SUM(extract(epoch from ts.stop_time)) AS seq
FROM (filtered_stations AS fs 
    LEFT JOIN public."Trains_stopped" AS ts ON (fs.stop_id = ts.stop_id))
GROUP BY fs.stop_id
ORDER BY seq */