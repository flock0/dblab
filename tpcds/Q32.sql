
SELECT  SUM(cs_ext_discount_amt)  AS "excess discount amount" 
FROM 
   catalog_sales 
   ,item 
   ,date_dim
WHERE
i_manufact_id = 269
AND i_item_sk = cs_item_sk 
AND d_date BETWEEN '1998-03-18' AND 
        (cast('1998-03-18' AS date) + 90 days)
AND d_date_sk = cs_sold_date_sk 
AND cs_ext_discount_amt  
     > 5
LIMIT 100;


