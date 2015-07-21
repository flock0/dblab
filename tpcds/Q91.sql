
SELECT AVG(ss_quantity)
       ,AVG(ss_ext_sales_price)
       ,AVG(ss_ext_wholesale_cost)
       ,SUM(ss_ext_wholesale_cost)
 FROM store_sales
     ,store
     ,customer_demographics
     ,household_demographics
     ,customer_address
     ,date_dim
 WHERE s_store_sk = ss_store_sk
 AND  ss_sold_date_sk = d_date_sk AND d_year = 2001
 AND((ss_hdemo_sk=hd_demo_sk
  AND cd_demo_sk = ss_cdemo_sk
  AND cd_marital_status = 'W'
  AND cd_education_status = 'Secondary'
  AND ss_sales_price between 100.00 AND 150.00
  AND hd_dep_count = 3   
     )or
     (ss_hdemo_sk=hd_demo_sk
  AND cd_demo_sk = ss_cdemo_sk
  AND cd_marital_status = 'U'
  AND cd_education_status = 'Advanced Degree'
  AND ss_sales_price between 50.00 AND 100.00   
  AND hd_dep_count = 1
     ) or 
     (ss_hdemo_sk=hd_demo_sk
  AND cd_demo_sk = ss_cdemo_sk
  AND cd_marital_status = 'D'
  AND cd_education_status = '2 yr Degree'
  AND ss_sales_price between 150.00 AND 200.00 
  AND hd_dep_count = 1  
     ))
 AND((ss_addr_sk = ca_address_sk
  AND ca_country = 'United States'
  AND ca_state in ('KY', 'AL', 'MO')
  AND ss_net_profit between 100 AND 200  
     ) or
     (ss_addr_sk = ca_address_sk
  AND ca_country = 'United States'
  AND ca_state in ('AR', 'KS', 'TX')
  AND ss_net_profit between 150 AND 300  
     ) or
     (ss_addr_sk = ca_address_sk
  AND ca_country = 'United States'
  AND ca_state in ('WA', 'ND', 'ID')
  AND ss_net_profit between 50 AND 250  
     ))
;
