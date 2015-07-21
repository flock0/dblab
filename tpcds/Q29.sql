
WITH ss AS (
 select
          i_item_id,SUM(ss_ext_sales_price) total_sales
 from
 	store_sales,
 	date_dim,
         customer_address,
         item
 WHERE
         i_item_id in (select
  i_item_id
from
 item
WHERE i_category in ('Music'))
 AND     ss_item_sk              = i_item_sk
 AND     ss_sold_date_sk         = d_date_sk
 AND     d_year                  = 1998
 AND     d_moy                   = 9
 AND     ss_addr_sk              = ca_address_sk
 AND     ca_gmt_offset           = -6 
 GROUP BY i_item_id),
 cs AS (
 select
          i_item_id,SUM(cs_ext_sales_price) total_sales
 from
 	catalog_sales,
 	date_dim,
         customer_address,
         item
 WHERE
         i_item_id               in (select
  i_item_id
from
 item
WHERE i_category in ('Music'))
 AND     cs_item_sk              = i_item_sk
 AND     cs_sold_date_sk         = d_date_sk
 AND     d_year                  = 1998
 AND     d_moy                   = 9
 AND     cs_bill_addr_sk         = ca_address_sk
 AND     ca_gmt_offset           = -6 
 GROUP BY i_item_id),
 ws AS (
 select
          i_item_id,SUM(ws_ext_sales_price) total_sales
 from
 	web_sales,
 	date_dim,
         customer_address,
         item
 WHERE
         i_item_id               in (select
  i_item_id
from
 item
WHERE i_category in ('Music'))
 AND     ws_item_sk              = i_item_sk
 AND     ws_sold_date_sk         = d_date_sk
 AND     d_year                  = 1998
 AND     d_moy                   = 9
 AND     ws_bill_addr_sk         = ca_address_sk
 AND     ca_gmt_offset           = -6
 GROUP BY i_item_id)
  SELECT   
  i_item_id
,SUM(total_sales) total_sales
 FROM  (SELECT * FROM ss 
        union all
        SELECT * FROM cs 
        union all
        SELECT * FROM ws) tmp1
 GROUP BY i_item_id
 ORDER BY i_item_id
      ,total_sales
 LIMIT 100;
