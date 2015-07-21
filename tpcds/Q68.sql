
WITH frequent_ss_items AS 
 (SELECT substr(i_item_desc,1,30) itemdesc,i_item_sk item_sk,d_date solddate,COUNT(*) cnt
  FROM store_sales
      ,date_dim 
      ,item
  WHERE ss_sold_date_sk = d_date_sk
    AND ss_item_sk = i_item_sk 
    AND d_year in (1998,1998+1,1998+2,1998+3)
  GROUP BY substr(i_item_desc,1,30),i_item_sk,d_date
  having COUNT(*) >4),
 max_store_sales AS
 (SELECT max(csales) tpcds_cmax 
  FROM (SELECT c_customer_sk,SUM(ss_quantity*ss_sales_price) csales
        FROM store_sales
            ,customer
            ,date_dim 
        WHERE ss_customer_sk = c_customer_sk
         AND ss_sold_date_sk = d_date_sk
         AND d_year in (1998,1998+1,1998+2,1998+3) 
        GROUP BY c_customer_sk)),
 best_ss_customer AS
 (SELECT c_customer_sk,SUM(ss_quantity*ss_sales_price) ssales
  FROM store_sales
      ,customer
  WHERE ss_customer_sk = c_customer_sk
  GROUP BY c_customer_sk
  having SUM(ss_quantity*ss_sales_price) > (95/100.0) * (select
  *
from
 max_store_sales))
  SELECT  SUM(sales)
 FROM (SELECT cs_quantity*cs_list_price sales
       FROM catalog_sales
           ,date_dim 
       WHERE d_year = 1998 
         AND d_moy = 4 
         AND cs_sold_date_sk = d_date_sk 
         AND cs_item_sk in (SELECT item_sk FROM frequent_ss_items)
         AND cs_bill_customer_sk in (SELECT c_customer_sk FROM best_ss_customer)
      union all
      SELECT ws_quantity*ws_list_price sales
       FROM web_sales 
           ,date_dim 
       WHERE d_year = 1998 
         AND d_moy = 4 
         AND ws_sold_date_sk = d_date_sk 
         AND ws_item_sk in (SELECT item_sk FROM frequent_ss_items)
         AND ws_bill_customer_sk in (SELECT c_customer_sk FROM best_ss_customer)) 
 LIMIT 100;
WITH frequent_ss_items AS
 (SELECT substr(i_item_desc,1,30) itemdesc,i_item_sk item_sk,d_date solddate,COUNT(*) cnt
  FROM store_sales
      ,date_dim
      ,item
  WHERE ss_sold_date_sk = d_date_sk
    AND ss_item_sk = i_item_sk
    AND d_year in (1998,1998 + 1,1998 + 2,1998 + 3)
  GROUP BY substr(i_item_desc,1,30),i_item_sk,d_date
  having COUNT(*) >4),
 max_store_sales AS
 (SELECT max(csales) tpcds_cmax
  FROM (SELECT c_customer_sk,SUM(ss_quantity*ss_sales_price) csales
        FROM store_sales
            ,customer
            ,date_dim 
        WHERE ss_customer_sk = c_customer_sk
         AND ss_sold_date_sk = d_date_sk
         AND d_year in (1998,1998+1,1998+2,1998+3)
        GROUP BY c_customer_sk)),
 best_ss_customer AS
 (SELECT c_customer_sk,SUM(ss_quantity*ss_sales_price) ssales
  FROM store_sales
      ,customer
  WHERE ss_customer_sk = c_customer_sk
  GROUP BY c_customer_sk
  having SUM(ss_quantity*ss_sales_price) > (95/100.0) * (select
  *
 FROM max_store_sales))
  SELECT  c_last_name,c_first_name,sales
 FROM (SELECT c_last_name,c_first_name,SUM(cs_quantity*cs_list_price) sales
        FROM catalog_sales
            ,customer
            ,date_dim 
        WHERE d_year = 1998 
         AND d_moy = 4 
         AND cs_sold_date_sk = d_date_sk 
         AND cs_item_sk in (SELECT item_sk FROM frequent_ss_items)
         AND cs_bill_customer_sk in (SELECT c_customer_sk FROM best_ss_customer)
         AND cs_bill_customer_sk = c_customer_sk 
       GROUP BY c_last_name,c_first_name
      union all
      SELECT c_last_name,c_first_name,SUM(ws_quantity*ws_list_price) sales
       FROM web_sales
           ,customer
           ,date_dim 
       WHERE d_year = 1998 
         AND d_moy = 4 
         AND ws_sold_date_sk = d_date_sk 
         AND ws_item_sk in (SELECT item_sk FROM frequent_ss_items)
         AND ws_bill_customer_sk in (SELECT c_customer_sk FROM best_ss_customer)
         AND ws_bill_customer_sk = c_customer_sk
       GROUP BY c_last_name,c_first_name) 
     ORDER BY c_last_name,c_first_name,sales
  LIMIT 100;
