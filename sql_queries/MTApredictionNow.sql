/*
line_id
direction
origin_id
destination_id
 */

WITH trains_in_sys AS (
SELECT * FROM public."Train" As t
INNER JOIN public."Trip_update" AS tu ON t.unique_num = tu.train_unique_num
WHERE is_in_system_now = True AND tu.line_id = '{0}' AND tu.direction = '{1}'
),

stu_my_station AS (
SELECT * FROM public."Stop_time_update" AS stu
INNER JOIN trains_in_sys ON trains_in_sys.id = stu.trip_update_id
WHERE stu.stop_id = '{2}'
),

stu_destination_station AS (
SELECT unique_num, stu.effective_timestamp, arrival_time FROM public."Stop_time_update" AS stu
INNER JOIN trains_in_sys ON trains_in_sys.id = stu.trip_update_id
WHERE stu.stop_id = '{3}'
),

closest_train AS (
SELECT unique_num, MIN(NOW() - arrival_time) origin_dt, arrival_time as arrival_time_orig FROM stu_my_station AS stu
GROUP BY unique_num, arrival_time
HAVING MIN(NOW() - arrival_time) > interval '0'
ORDER BY origin_dt ASC
LIMIT 1
)

SELECT *, (SELECT arrival_time_orig FROM closest_train) as orig_time, arrival_time-(SELECT arrival_time_orig FROM closest_train) AS transit_time
FROM stu_destination_station AS stu
WHERE stu.unique_num = (SELECT unique_num FROM closest_train)
ORDER BY stu.effective_timestamp DESC
LIMIT 1