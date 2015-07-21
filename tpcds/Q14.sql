
SELECT  *
 FROM(SELECT w_warehouse_name
            ,i_item_id
            ,SUM(CASE WHEN (cast(d_date AS date) < cast ('2000-04-19' AS date))
	                THEN inv_quantity_on_hAND 
                      ELSE 0 END) AS inv_before
            ,SUM(CASE WHEN (cast(d_date AS date) >= cast ('2000-04-19' AS date))
                      THEN inv_quantity_on_hAND 
                      ELSE 0 END) AS inv_after
   FROM inventory
       ,warehouse
       ,item
       ,date_dim
   WHERE i_current_price between 0.99 AND 1.49
     AND i_item_sk          = inv_item_sk
     AND inv_warehouse_sk   = w_warehouse_sk
     AND inv_date_sk    = d_date_sk
     AND d_date between (cast ('2000-04-19' AS date) - 30 days)
                    AND (cast ('2000-04-19' AS date) + 30 days)
   GROUP BY w_warehouse_name, i_item_id) x
 WHERE (CASE WHEN inv_before > 0 
             THEN inv_after / inv_before 
             ELSE NULL
             END) between 2.0/3.0 AND 3.0/2.0
 ORDER BY w_warehouse_name
         ,i_item_id
 LIMIT 100;
