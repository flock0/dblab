
SELECT  * 
FROM (SELECT i_manager_id
             ,SUM(ss_sales_price) sum_sales
             ,AVG(SUM(ss_sales_price)) over (partition by i_manager_id) avg_monthly_sales
      FROM item
          ,store_sales
          ,date_dim
          ,store
      WHERE ss_item_sk = i_item_sk
        AND ss_sold_date_sk = d_date_sk
        AND ss_store_sk = s_store_sk
        AND d_month_seq in (1200,1200+1,1200+2,1200+3,1200+4,1200+5,1200+6,1200+7,1200+8,1200+9,1200+10,1200+11)
        AND ((    i_category in ('Books','Children','Electronics')
              AND i_class in ('personal','portable','reference','self-help')
              AND i_brAND in ('scholaramalgamalg #14','scholaramalgamalg #7',
		                  'exportiunivamalg #9','scholaramalgamalg #9'))
           or(    i_category in ('Women','Music','Men')
              AND i_class in ('accessories','classical','fragrances','pants')
              AND i_brAND in ('amalgimporto #1','edu packscholar #1','exportiimporto #1',
		                 'importoamalg #1')))
GROUP BY i_manager_id, d_moy) tmp1
WHERE CASE WHEN avg_monthly_sales > 0 THEN abs (sum_sales - avg_monthly_sales) / avg_monthly_sales ELSE NULL END > 0.1
ORDER BY i_manager_id
        ,avg_monthly_sales
        ,sum_sales
LIMIT 100;
