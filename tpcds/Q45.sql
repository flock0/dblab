
SELECT  dt.d_year 
       ,item.i_brand_id brand_id 
       ,item.i_brAND brand
       ,SUM(ss_ext_discount_amt) sum_agg
 FROM  date_dim dt 
      ,store_sales
      ,item
 WHERE dt.d_date_sk = store_sales.ss_sold_date_sk
   AND store_sales.ss_item_sk = item.i_item_sk
   AND item.i_manufact_id = 922
   AND dt.d_moy=12
 GROUP BY dt.d_year
      ,item.i_brand
      ,item.i_brand_id
 ORDER BY dt.d_year
         ,sum_agg desc
         ,brand_id
 LIMIT 100;
