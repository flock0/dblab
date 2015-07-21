
SELECT CASE WHEN (SELECT COUNT(*) 
                  FROM store_sales 
                  WHERE ss_quantity between 1 AND 20) > 26229
            THEN (SELECT AVG(ss_ext_sales_price) 
                  FROM store_sales 
                  WHERE ss_quantity between 1 AND 20) 
            ELSE (SELECT AVG(ss_net_paid)
                  FROM store_sales
                  WHERE ss_quantity between 1 AND 20) END bucket1 ,
       CASE WHEN (SELECT COUNT(*)
                  FROM store_sales
                  WHERE ss_quantity between 21 AND 40) > 34549
            THEN (SELECT AVG(ss_ext_sales_price)
                  FROM store_sales
                  WHERE ss_quantity between 21 AND 40) 
            ELSE (SELECT AVG(ss_net_paid)
                  FROM store_sales
                  WHERE ss_quantity between 21 AND 40) END bucket2,
       CASE WHEN (SELECT COUNT(*)
                  FROM store_sales
                  WHERE ss_quantity between 41 AND 60) > 28389
            THEN (SELECT AVG(ss_ext_sales_price)
                  FROM store_sales
                  WHERE ss_quantity between 41 AND 60)
            ELSE (SELECT AVG(ss_net_paid)
                  FROM store_sales
                  WHERE ss_quantity between 41 AND 60) END bucket3,
       CASE WHEN (SELECT COUNT(*)
                  FROM store_sales
                  WHERE ss_quantity between 61 AND 80) > 21637
            THEN (SELECT AVG(ss_ext_sales_price)
                  FROM store_sales
                  WHERE ss_quantity between 61 AND 80)
            ELSE (SELECT AVG(ss_net_paid)
                  FROM store_sales
                  WHERE ss_quantity between 61 AND 80) END bucket4,
       CASE WHEN (SELECT COUNT(*)
                  FROM store_sales
                  WHERE ss_quantity between 81 AND 100) > 23773
            THEN (SELECT AVG(ss_ext_sales_price)
                  FROM store_sales
                  WHERE ss_quantity between 81 AND 100)
            ELSE (SELECT AVG(ss_net_paid)
                  FROM store_sales
                  WHERE ss_quantity between 81 AND 100) END bucket5
FROM reason
WHERE r_reason_sk = 1
;
