
/* make sure you indexed trip_update_id and stop_id in Stop_time_update, or this will take forever. */
/* variables to be passed into this:
{0}: train_unique_num
{1}: origin stop_id
{2}: destination stop_id

*/
WITH origin_time AS (SELECT max(stop_time) as origin_time FROM public."Trains_stopped"
WHERE train_unique_num = '{0}' and stop_id = '{1}'
),

best_time_diff AS (
SELECT min(abs(stu.effective_timestamp - (SELECT * FROM origin_time))) as td
FROM public."Trip_update" as tu
INNER JOIN public."Stop_time_update"as stu ON stu.trip_update_id = tu.id
WHERE tu.train_unique_num = '{0}' and stop_id = '{2}'
)

SELECT stu.arrival_time as MTA_predicted_arr_time, (SELECT * FROM origin_time) as origin_time, stu.arrival_time - origin_time as MTA_predicted_transit_time
FROM public."Trip_update" as tu
INNER JOIN public."Stop_time_update"as stu ON stu.trip_update_id = tu.id
WHERE tu.train_unique_num = '{0}' and stop_id = '{2}'
AND abs(stu.effective_timestamp - (SELECT * FROM origin_time))  = (SELECT * FROM best_time_diff)
