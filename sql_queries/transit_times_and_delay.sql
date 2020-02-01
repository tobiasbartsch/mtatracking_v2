WITH origin AS (SELECT t.unique_num, ts.stop_id, MAX(ts.stop_time) as stop_time, ts.trip_update_id as tuid
				FROM public."Train" AS t 
					LEFT JOIN public."Trains_stopped" AS ts
					ON t.unique_num = ts.train_unique_num
				GROUP BY t.unique_num, ts.stop_id, ts.trip_update_id
				HAVING ts.stop_id = '{0}' AND t.route_id = '{2}'
				),

destination AS (SELECT t.unique_num, ts.stop_id, MAX(ts.stop_time) as stop_time, ts.trip_update_id as tuid, ts.delayed as delayed
				FROM public."Train" AS t 
					LEFT JOIN public."Trains_stopped" AS ts
					ON t.unique_num = ts.train_unique_num
				GROUP BY t.unique_num, ts.stop_id, ts.trip_update_id, ts.delayed
				HAVING ts.stop_id = '{1}' AND t.route_id = '{2}'
				)

SELECT DISTINCT d.stop_time, (d.stop_time - o.stop_time) AS transit_time, d.delayed AS delayed
FROM origin AS o 
	INNER JOIN destination as d
	ON o.unique_num = d.unique_num AND o.tuid = d.tuid
		WHERE d.stop_time > '{3}'
		AND d.stop_time < '{4}'
	ORDER BY d.stop_time
				/* we have to "group by" to get the max stop time. sometimes trains
				seem to stop several times at the same station -- clearly a glitch
				in the data feed. We assume that the last stop time is the real one */







	