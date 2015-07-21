
SELECT  i_item_desc
      ,w_warehouse_name
      ,d1.d_week_seq
      ,SUM(CASE WHEN p_promo_sk IS NULL THEN 1 ELSE 0 END) no_promo
      ,SUM(CASE WHEN p_promo_sk IS NOT NULL THEN 1 ELSE 0 END) promo
      ,COUNT(*) total_cnt
FROM catalog_sales
join inventory on (cs_item_sk = inv_item_sk)
join warehouse on (w_warehouse_sk=inv_warehouse_sk)
join item on (i_item_sk = cs_item_sk)
join customer_demographics on (cs_bill_cdemo_sk = cd_demo_sk)
join household_demographics on (cs_bill_hdemo_sk = hd_demo_sk)
join date_dim d1 on (cs_sold_date_sk = d1.d_date_sk)
join date_dim d2 on (inv_date_sk = d2.d_date_sk)
join date_dim d3 on (cs_ship_date_sk = d3.d_date_sk)
LEFT OUTER JOIN promotion on (cs_promo_sk=p_promo_sk)
LEFT OUTER JOIN catalog_returns on (cr_item_sk = cs_item_sk AND cr_order_number = cs_order_number)
WHERE d1.d_week_seq = d2.d_week_seq
  AND inv_quantity_on_hAND < cs_quantity 
  AND d3.d_date > d1.d_date + 5
  AND hd_buy_potential = '1001-5000'
  AND d1.d_year = 1999
  AND cd_marital_status = 'D'
GROUP BY i_item_desc,w_warehouse_name,d1.d_week_seq
ORDER BY total_cnt desc, i_item_desc, w_warehouse_name, d_week_seq
LIMIT 100;
