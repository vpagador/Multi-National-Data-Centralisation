--9: How quickly is the company making sales?

WITH date_times AS (
SELECT
year,
month,
day,
timestamp,
TO_TIMESTAMP(CONCAT(year, '/', month, '/', day, '/', timestamp), 'YYYY/MM/DD/HH24:MI:ss') as times

			   FROM dim_date_times d
					 JOIN orders_table o
					 ON d.date_uuid = o.date_uuid
					 JOIN dim_store_details s
					 ON o.store_code = s.store_code
			   ORDER BY times DESC),		   	


next_times AS(
SELECT year,
timestamp,
times,
LEAD(times) OVER(ORDER BY times DESC) AS next_times
FROM date_times),

avg_times AS(
SELECT year,
(AVG(times - next_times)) AS avg_times
FROM next_times
GROUP BY year
ORDER BY avg_times DESC)

SELECT year,
-- concat('hours: ', cast(round(avg(EXTRACT(HOUR FROM avg_times))) as text),
-- 	   ', minutes: ', cast(round(avg(EXTRACT(MINUTE FROM avg_times))) as text),
-- 	   ', seconds: ', cast(round(avg(EXTRACT(SECOND FROM avg_times))) as text))
-- 	   as actual_time_taken

	CONCAT('"Hours": ', (EXTRACT(HOUR FROM avg_times)),','
	' "minutes" :', (EXTRACT(MINUTE FROM avg_times)),','
    ' "seconds" :', ROUND(EXTRACT(SECOND FROM avg_times)),','
     ' "milliseconds" :', ROUND((EXTRACT( SECOND FROM avg_times)- FLOOR(EXTRACT(SECOND FROM avg_times)))*100))
	
   as actual_time_taken


FROM avg_times
GROUP BY year, avg_times
ORDER BY avg_times DESC
LIMIT 5;

