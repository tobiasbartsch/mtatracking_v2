WITH origin AS (SELECT t.unique_num, ts.stop_id, ts.stop_time
				FROM public."Train" AS t 
					LEFT JOIN public."Trains_stopped" AS ts
					ON t.unique_num = ts.train_unique_num
				WHERE ts.stop_id = '{0}' AND t.route_id = '{2}'),

destination AS (SELECT t.unique_num, ts.stop_id, ts.stop_time
				FROM public."Train" AS t 
					LEFT JOIN public."Trains_stopped" AS ts
					ON t.unique_num = ts.train_unique_num
				WHERE ts.stop_id = '{1}' AND t.route_id = '{2}')
SELECT d.stop_time, (d.stop_time - o.stop_time) AS transit_time
FROM origin AS o 
	INNER JOIN destination as d
	ON o.unique_num = d.unique_num
	WHERE d.stop_time > '{3}' AND d.stop_time < '{4}'
	ORDER BY d.stop_time