/* we want to create one entry for every train that leaves
our origin station within a specified time window
                    line_id,
                    direction,
                    linedef_hour,
                    linedef_day,
                    f_line_id,
                    f_direction
*/

WITH ts_filtered AS (
SELECT * FROM public."Trains_stopped" AS ts
WHERE NOW() - interval '1 hour' < stop_time
),

stops_this_line AS (
SELECT stop_id
FROM public."Line" as l INNER JOIN public."Line_stops" As ls
ON l.id = ls.line_id
WHERE l.name = '{0}' AND l.direction = '{1}'
AND from_hour < '{2}' and to_hour >= '{2}' AND day='{3}'
ORDER BY ls.sequence
),

/* get status of stops_this_line at the origin times.
In particular, we want: last stop time, delay sdevs of that last train */
all_stopped_trains_this_line AS (
SELECT stop_id, tu.id, MAX(stop_time) as stop_time, delayed_magnitude
FROM ts_filtered AS ts 
INNER JOIN public."Trip_update" AS tu ON ts.trip_update_id = tu.id
GROUP BY stop_id, tu.id, delayed_magnitude, tu.line_id, tu.direction
HAVING tu.line_id = '{0}' AND tu.direction = '{1}' AND ts.stop_id IN (SELECT stop_id FROM stops_this_line)
ORDER BY ts.stop_id
),

all_combinations AS (
SELECT stl.stop_id, NOW() as origin_time, NOW() - stl.stop_time AS odiff, stl.delayed_magnitude
FROM all_stopped_trains_this_line AS stl
WHERE NOW() - stl.stop_time > interval '0'
),

res AS (
SELECT DISTINCT ON (origin_time, stop_id) origin_time, stop_id, odiff, delayed_magnitude
FROM all_combinations
ORDER BY origin_time DESC, stop_id, odiff, delayed_magnitude DESC
)

SELECT * FROM res
ORDER BY res.origin_time ASC