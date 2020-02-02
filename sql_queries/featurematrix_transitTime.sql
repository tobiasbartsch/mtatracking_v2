/* we want to create one entry for every train that leaves
our origin station within a specified time window*/
WITH origin AS (
SELECT ts.train_unique_num, stop_time as origin_time
FROM public."Trains_stopped" AS ts
INNER JOIN public."Trip_update" AS tu ON ts.trip_update_id = tu.id
WHERE tu.line_id = '{2}' AND tu.direction = '{3}' AND ts.stop_id = '{0}'
    AND '{4}' < stop_time AND '{5}' > stop_time
ORDER BY stop_time
),

destination AS (
SELECT ts.train_unique_num, stop_time as destination_time
FROM public."Trains_stopped" AS ts
INNER JOIN public."Trip_update" AS tu ON ts.trip_update_id = tu.id
WHERE tu.line_id = '{2}' AND tu.direction = '{3}' AND ts.stop_id = '{1}'
    AND '{4}' < stop_time AND '{5}' > stop_time
ORDER BY stop_time
),

stops_this_line AS (
SELECT stop_id
FROM public."Line" as l INNER JOIN public."Line_stops" As ls
ON l.id = ls.line_id
WHERE l.name = '{2}' AND l.direction = '{3}'
ORDER BY ls.sequence
),

/* get status of stops_this_line at the origin times.
In particular, we want: last stop time, delay sdevs of that last train */
all_stopped_trains_this_line AS (
SELECT stop_id, stop_time, delayed_magnitude
FROM public."Trains_stopped" AS ts 
INNER JOIN public."Trip_update" AS tu ON ts.trip_update_id = tu.id
WHERE tu.line_id = '{2}' AND tu.direction = '{3}' AND ts.stop_id IN (SELECT stop_id FROM stops_this_line)
    AND '{4}' < stop_time AND '{5}' > stop_time
ORDER BY ts.stop_id
),

all_combinations AS (
SELECT stl.stop_id, o.train_unique_num, o.origin_time, o.origin_time - stl.stop_time AS odiff, stl.delayed_magnitude
FROM all_stopped_trains_this_line AS stl CROSS JOIN origin AS o
WHERE o.origin_time - stl.stop_time > interval '0'
),

res AS (
SELECT DISTINCT ON (origin_time, stop_id) origin_time, train_unique_num, stop_id, odiff, delayed_magnitude
FROM all_combinations
ORDER BY origin_time DESC, stop_id, odiff, delayed_magnitude DESC
)

SELECT res.*, destination.destination_time - res.origin_time as transit_time
FROM res INNER JOIN destination ON res.train_unique_num = destination.train_unique_num
ORDER BY delayed_magnitude DESC