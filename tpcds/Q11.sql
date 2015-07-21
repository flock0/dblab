
SELECT   
    SUM(ws_net_paid) AS total_sum
   ,i_category
   ,i_class
   ,grouping(i_category)+grouping(i_class) AS lochierarchy
   ,rank() over (
 	partition by grouping(i_category)+grouping(i_class),
 	CASE WHEN grouping(i_class) = 0 THEN i_category END 
 	ORDER BY SUM(ws_net_paid) desc) AS rank_within_parent
 from
    web_sales
   ,date_dim       d1
   ,item
 WHERE
    d1.d_month_seq between 1211 AND 1211+11
 AND d1.d_date_sk = ws_sold_date_sk
 AND i_item_sk  = ws_item_sk
 GROUP BY rollup(i_category,i_class)
 ORDER BY
   lochierarchy desc,
   CASE WHEN lochierarchy = 0 THEN i_category END,
   rank_within_parent
 LIMIT 100;