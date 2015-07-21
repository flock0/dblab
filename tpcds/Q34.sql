
SELECT  
    SUM(ss_net_profit) AS total_sum
   ,s_state
   ,s_county
   ,grouping(s_state)+grouping(s_county) AS lochierarchy
   ,rank() over (
 	partition by grouping(s_state)+grouping(s_county),
 	CASE WHEN grouping(s_county) = 0 THEN s_state END 
 	ORDER BY SUM(ss_net_profit) desc) AS rank_within_parent
 from
    store_sales
   ,date_dim       d1
   ,store
 WHERE
    d1.d_month_seq between 1217 AND 1217+11
 AND d1.d_date_sk = ss_sold_date_sk
 AND s_store_sk  = ss_store_sk
 AND s_state in
             ( SELECT s_state
               FROM  (SELECT s_state AS s_state,
 			    rank() over ( partition by s_state ORDER BY SUM(ss_net_profit) desc) AS ranking
                      FROM   store_sales, store, date_dim
                      WHERE  d_month_seq between 1217 AND 1217+11
 			    AND d_date_sk = ss_sold_date_sk
 			    AND s_store_sk  = ss_store_sk
                      GROUP BY s_state
                     ) tmp1 
               WHERE ranking <= 5
             )
 GROUP BY rollup(s_state,s_county)
 ORDER BY
   lochierarchy desc
  ,CASE WHEN lochierarchy = 0 THEN s_state END
  ,rank_within_parent
 LIMIT 100;