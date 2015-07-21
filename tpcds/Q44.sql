
SELECT  
   SUM(ws_ext_discount_amt)  AS "Excess Discount Amount" 
FROM 
    web_sales 
   ,item 
   ,date_dim
WHERE
i_manufact_id = 73
AND i_item_sk = ws_item_sk 
AND d_date between '2002-02-17' AND 
        (cast('2002-02-17' AS date) + 90 days)
AND d_date_sk = ws_sold_date_sk 
AND ws_ext_discount_amt  
     > ( 
         SELECT 
            1.3 * AVG(ws_ext_discount_amt) 
         FROM 
            web_sales 
           ,date_dim
         WHERE 
              ws_item_sk = i_item_sk 
          AND d_date between '2002-02-17' AND
                             (cast('2002-02-17' AS date) + 90 days)
          AND d_date_sk = ws_sold_date_sk 
      ) 
ORDER BY SUM(ws_ext_discount_amt)
LIMIT 100;