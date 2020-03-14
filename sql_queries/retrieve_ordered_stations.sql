/* 
0: name
1: direction
2: day
3: hour of day
*/
WITH latest_def AS (
SELECT max(id) as id FROM public."Line"
GROUP BY name, direction
HAVING name = '{0}' AND direction = '{1}'
)

SELECT stop_id
FROM public."Line_stops" AS ls INNER JOIN latest_def AS ld ON ls.line_id = ld.id
WHERE day = '{2}' and from_hour < '{3}' AND '{3}' < to_hour
ORDER BY sequence ASC