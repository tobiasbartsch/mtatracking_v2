/* we want to create one entry for every train that leaves
our origin station */
WITH origin AS (
SELECT ts.train_unique_num, stop_time as origin_time
FROM public."Trains_stopped" AS ts
INNER JOIN public."Trip_update" AS tu ON ts.trip_update_id = tu.id
WHERE tu.line_id = 'Q' AND tu.direction = 'N' AND ts.stop_id = 'D25N'
ORDER BY stop_time
),

stops_this_line AS (
SELECT stop_id
FROM public."Line" as l INNER JOIN public."Line_stops" As ls
ON l.id = ls.line_id
WHERE l.name = 'Q' AND l.direction = 'N'
ORDER BY ls.sequence
),

/* get status of stops_this_line at the origin times.
In particular, we want: last stop time, delay sdevs of that last train */
all_stopped_trains_this_line AS (
SELECT stop_id, stop_time, delayed_magnitude
FROM public."Trains_stopped" AS ts 
INNER JOIN public."Trip_update" AS tu ON ts.trip_update_id = tu.id
WHERE tu.line_id = 'Q' AND tu.direction = 'N' AND ts.stop_id IN (SELECT stop_id FROM stops_this_line)
ORDER BY ts.stop_id
),

all_combinations AS (
SELECT stl.stop_id, o.origin_time, o.origin_time - stl.stop_time AS odiff, stl.delayed_magnitude
FROM all_stopped_trains_this_line AS stl CROSS JOIN origin AS o
WHERE o.origin_time - stl.stop_time > interval '0'
)

SELECT DISTINCT ON (origin_time, stop_id) origin_time, stop_id, odiff, delayed_magnitude
FROM all_combinations
ORDER BY origin_time DESC, stop_id, odiff
LIMIT 100
