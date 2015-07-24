
SELECT  i_item_id,
        s_state, grouping(s_state) g_state,
        AVG(ss_quantity) agg1,
        AVG(ss_list_price) agg2,
        AVG(ss_coupon_amt) agg3,
        AVG(ss_sales_price) agg4
 FROM store_sales, customer_demographics, date_dim, store, item
 WHERE ss_sold_date_sk = d_date_sk AND
       ss_item_sk = i_item_sk AND
       ss_store_sk = s_store_sk AND
       ss_cdemo_sk = cd_demo_sk AND
       cd_gender = 'F' AND
       cd_marital_status = 'W' AND
       cd_education_status = 'Primary' AND
       d_year = 1998 AND
       s_state IN ('TN','TN', 'TN', 'TN', 'TN', 'TN')
 GROUP BY rollup (i_item_id, s_state)
 ORDER BY i_item_id
         ,s_state
 LIMIT 100;


