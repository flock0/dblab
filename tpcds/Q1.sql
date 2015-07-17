SELECT  COUNT(*) AS cnt
FROM store_sales
INNER JOIN household_demographics ON store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk 
INNER JOIN store ON store_sales.ss_store_sk = store.s_store_sk
INNER JOIN time_dim ON store_sales.ss_sold_time_sk = time_dim.t_time_sk
WHERE time_dim.t_hour = 8
    AND time_dim.t_minute >= 30
    AND household_demographics.hd_dep_count = 5
    AND store.s_store_name = 'ese'
ORDER BY COUNT(*)
LIMIT 100;