
WITH ss AS (
 select
          i_manufact_id,SUM(ss_ext_sales_price) total_sales
 from
 	store_sales,
 	date_dim,
         customer_address,
         item
 WHERE
         i_manufact_id in (select
  i_manufact_id
from
 item
WHERE i_category in ('Jewelry'))
 AND     ss_item_sk              = i_item_sk
 AND     ss_sold_date_sk         = d_date_sk
 AND     d_year                  = 2000
 AND     d_moy                   = 7
 AND     ss_addr_sk              = ca_address_sk
 AND     ca_gmt_offset           = -5 
 GROUP BY i_manufact_id),
 cs AS (
 select
          i_manufact_id,SUM(cs_ext_sales_price) total_sales
 from
 	catalog_sales,
 	date_dim,
         customer_address,
         item
 WHERE
         i_manufact_id               in (select
  i_manufact_id
from
 item
WHERE i_category in ('Jewelry'))
 AND     cs_item_sk              = i_item_sk
 AND     cs_sold_date_sk         = d_date_sk
 AND     d_year                  = 2000
 AND     d_moy                   = 7
 AND     cs_bill_addr_sk         = ca_address_sk
 AND     ca_gmt_offset           = -5 
 GROUP BY i_manufact_id),
 ws AS (
 select
          i_manufact_id,SUM(ws_ext_sales_price) total_sales
 from
 	web_sales,
 	date_dim,
         customer_address,
         item
 WHERE
         i_manufact_id               in (select
  i_manufact_id
from
 item
WHERE i_category in ('Jewelry'))
 AND     ws_item_sk              = i_item_sk
 AND     ws_sold_date_sk         = d_date_sk
 AND     d_year                  = 2000
 AND     d_moy                   = 7
 AND     ws_bill_addr_sk         = ca_address_sk
 AND     ca_gmt_offset           = -5
 GROUP BY i_manufact_id)
  SELECT  i_manufact_id ,SUM(total_sales) total_sales
 FROM  (SELECT * FROM ss 
        union all
        SELECT * FROM cs 
        union all
        SELECT * FROM ws) tmp1
 GROUP BY i_manufact_id
 ORDER BY total_sales
LIMIT 100;
