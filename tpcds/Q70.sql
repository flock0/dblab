
WITH v1 AS(
 SELECT i_category, i_brand,
        cc_name,
        d_year, d_moy,
        SUM(cs_sales_price) sum_sales,
        AVG(SUM(cs_sales_price)) over
          (partition by i_category, i_brand,
                     cc_name, d_year)
          avg_monthly_sales,
        rank() over
          (partition by i_category, i_brand,
                     cc_name
           ORDER BY d_year, d_moy) rn
 FROM item, catalog_sales, date_dim, call_center
 WHERE cs_item_sk = i_item_sk AND
       cs_sold_date_sk = d_date_sk AND
       cc_call_center_sk= cs_call_center_sk AND
       (
         d_year = 2000 or
         ( d_year = 2000-1 AND d_moy =12) or
         ( d_year = 2000+1 AND d_moy =1)
       )
 GROUP BY i_category, i_brand,
          cc_name , d_year, d_moy),
 v2 AS(
 SELECT v1.cc_name
        ,v1.d_year, v1.d_moy
        ,v1.avg_monthly_sales
        ,v1.sum_sales, v1_lag.sum_sales psum, v1_lead.sum_sales nsum
 FROM v1, v1 v1_lag, v1 v1_lead
 WHERE v1.i_category = v1_lag.i_category AND
       v1.i_category = v1_lead.i_category AND
       v1.i_brAND = v1_lag.i_brAND AND
       v1.i_brAND = v1_lead.i_brAND AND
       v1. cc_name = v1_lag. cc_name AND
       v1. cc_name = v1_lead. cc_name AND
       v1.rn = v1_lag.rn + 1 AND
       v1.rn = v1_lead.rn - 1)
  SELECT  *
 FROM v2
 WHERE  d_year = 2000 AND
        avg_monthly_sales > 0 AND
        CASE WHEN avg_monthly_sales > 0 THEN abs(sum_sales - avg_monthly_sales) / avg_monthly_sales ELSE NULL END > 0.1
 ORDER BY sum_sales - avg_monthly_sales, 3
 LIMIT 100;