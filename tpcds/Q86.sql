
SELECT  
   w_state
  ,i_item_id
  ,SUM(CASE WHEN (cast(d_date AS date) < cast ('2000-04-07' AS date)) 
 		then cs_sales_price - coalesce(cr_refunded_cash,0) ELSE 0 END) AS sales_before
  ,SUM(CASE WHEN (cast(d_date AS date) >= cast ('2000-04-07' AS date)) 
 		then cs_sales_price - coalesce(cr_refunded_cash,0) ELSE 0 END) AS sales_after
 from
   catalog_sales LEFT OUTER JOIN catalog_returns on
       (cs_order_number = cr_order_number 
        AND cs_item_sk = cr_item_sk)
  ,warehouse 
  ,item
  ,date_dim
 WHERE
     i_current_price between 0.99 AND 1.49
 AND i_item_sk          = cs_item_sk
 AND cs_warehouse_sk    = w_warehouse_sk 
 AND cs_sold_date_sk    = d_date_sk
 AND d_date between (cast ('2000-04-07' AS date) - 30 days)
                AND (cast ('2000-04-07' AS date) + 30 days) 
 GROUP BY
    w_state,i_item_id
 ORDER BY w_state,i_item_id
LIMIT 100;
