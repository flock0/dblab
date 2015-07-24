
SELECT  i_item_id,
        ca_country,
        ca_state, 
        ca_county,
        AVG( cast(cs_quantity AS numeric(12,2))) agg1,
        AVG( cast(cs_list_price AS numeric(12,2))) agg2,
        AVG( cast(cs_coupon_amt AS numeric(12,2))) agg3,
        AVG( cast(cs_sales_price AS numeric(12,2))) agg4,
        AVG( cast(cs_net_profit AS numeric(12,2))) agg5,
        AVG( cast(c_birth_year AS numeric(12,2))) agg6,
        AVG( cast(cd1.cd_dep_count AS numeric(12,2))) agg7
 FROM catalog_sales, customer_demographics cd1, 
      customer_demographics cd2, customer, customer_address, date_dim, item
 WHERE cs_sold_date_sk = d_date_sk AND
       cs_item_sk = i_item_sk AND
       cs_bill_cdemo_sk = cd1.cd_demo_sk AND
       cs_bill_customer_sk = c_customer_sk AND
       cd1.cd_gender = 'M' AND 
       cd1.cd_education_status = 'College' AND
       c_current_cdemo_sk = cd2.cd_demo_sk AND
       c_current_addr_sk = ca_address_sk AND
       c_birth_month IN (9,5,12,4,1,10) AND
       d_year = 2001 AND
       ca_state IN ('ND','WI','AL'
                   ,'NC','OK','MS','TN')
 GROUP BY rollup (i_item_id, ca_country, ca_state, ca_county)
 ORDER BY ca_country,
        ca_state, 
        ca_county,
	i_item_id
 LIMIT 100;


