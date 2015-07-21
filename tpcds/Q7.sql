
SELECT  SUM(cs_ext_discount_amt)  AS "excess discount amount" 
FROM 
   catalog_sales 
   ,item 
   ,date_dim
WHERE
i_manufact_id = 291
AND i_item_sk = cs_item_sk 
AND d_date BETWEEN '2000-03-22' AND 
        (cast('2000-03-22' AS date) + 90 days)
AND d_date_sk = cs_sold_date_sk 
AND cs_ext_discount_amt  
     > ( 
         SELECT 
            1.3 * AVG(cs_ext_discount_amt) 
         FROM 
            catalog_sales 
           ,date_dim
         WHERE 
              cs_item_sk = i_item_sk 
          AND d_date BETWEEN '2000-03-22' AND
                             (cast('2000-03-22' AS date) + 90 days)
          AND d_date_sk = cs_sold_date_sk 
      ) 
LIMIT 100;
