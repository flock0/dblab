SELECT  COUNT(*) AS cnt
FROM store_sales
INNER JOIN household_demographics ON ss_hdemo_sk = hd_demo_sk 
INNER JOIN store ON ss_store_sk = s_store_sk
INNER JOIN time_dim ON ss_sold_time_sk = t_time_sk
WHERE t_hour = 8
    AND t_minute >= 30
    AND hd_dep_count = 5
    AND s_store_name = 'ese'
ORDER BY COUNT(*)
LIMIT 100;