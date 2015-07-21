
WITH ss AS (
 SELECT i_item_id,SUM(ss_ext_sales_price) total_sales
 from
 	store_sales,
 	date_dim,
         customer_address,
         item
 WHERE i_item_id in (select
     i_item_id
FROM item
WHERE i_color in ('navajo','khaki','firebrick'))
 AND     ss_item_sk              = i_item_sk
 AND     ss_sold_date_sk         = d_date_sk
 AND     d_year                  = 2000
 AND     d_moy                   = 4
 AND     ss_addr_sk              = ca_address_sk
 AND     ca_gmt_offset           = -5 
 GROUP BY i_item_id),
 cs AS (
 SELECT i_item_id,SUM(cs_ext_sales_price) total_sales
 from
 	catalog_sales,
 	date_dim,
         customer_address,
         item
 WHERE
         i_item_id               in (select
  i_item_id
FROM item
WHERE i_color in ('navajo','khaki','firebrick'))
 AND     cs_item_sk              = i_item_sk
 AND     cs_sold_date_sk         = d_date_sk
 AND     d_year                  = 2000
 AND     d_moy                   = 4
 AND     cs_bill_addr_sk         = ca_address_sk
 AND     ca_gmt_offset           = -5 
 GROUP BY i_item_id),
 ws AS (
 SELECT i_item_id,SUM(ws_ext_sales_price) total_sales
 from
 	web_sales,
 	date_dim,
         customer_address,
         item
 WHERE
         i_item_id               in (select
  i_item_id
FROM item
WHERE i_color in ('navajo','khaki','firebrick'))
 AND     ws_item_sk              = i_item_sk
 AND     ws_sold_date_sk         = d_date_sk
 AND     d_year                  = 2000
 AND     d_moy                   = 4
 AND     ws_bill_addr_sk         = ca_address_sk
 AND     ca_gmt_offset           = -5
 GROUP BY i_item_id)
  SELECT  i_item_id ,SUM(total_sales) total_sales
 FROM  (SELECT * FROM ss 
        union all
        SELECT * FROM cs 
        union all
        SELECT * FROM ws) tmp1
 GROUP BY i_item_id
 ORDER BY total_sales
 LIMIT 100;
