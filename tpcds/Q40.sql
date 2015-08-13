
SELECT  
   w_state
  ,i_item_id
  ,SUM(CASE WHEN (cast(d_date AS date) < cast ('1998-04-08' AS date)) 
 		then cs_sales_price - COALESCE(cr_refunded_cash,0) ELSE 0 END) AS sales_before
  ,SUM(CASE WHEN (cast(d_date AS date) >= cast ('1998-04-08' AS date)) 
 		then cs_sales_price - COALESCE(cr_refunded_cash,0) ELSE 0 END) AS sales_after
 FROM
   catalog_sales left outer join catalog_returns on
       (cs_order_number = cr_order_number 
        AND cs_item_sk = cr_item_sk)
  ,warehouse 
  ,item
  ,date_dim
 WHERE
     i_current_price BETWEEN 0.99 AND 1.49
 AND i_item_sk          = cs_item_sk
 AND cs_warehouse_sk    = w_warehouse_sk 
 AND cs_sold_date_sk    = d_date_sk
 AND d_date BETWEEN (cast ('1998-04-08' AS date) - 30 days)
                AND (cast ('1998-04-08' AS date) + 30 days) 
 GROUP BY
    w_state,i_item_id
 ORDER BY w_state,i_item_id
LIMIT 100;


