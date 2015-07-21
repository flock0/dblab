
SELECT  i_item_id
       ,i_item_desc
       ,i_current_price
 FROM item, inventory, date_dim, store_sales
 WHERE i_current_price between 31 AND 31+30
 AND inv_item_sk = i_item_sk
 AND d_date_sk=inv_date_sk
 AND d_date between cast('2001-04-27' AS date) AND (cast('2001-04-27' AS date) +  60 days)
 AND i_manufact_id in (200,1,170,198)
 AND inv_quantity_on_hAND between 100 AND 500
 AND ss_item_sk = i_item_sk
 GROUP BY i_item_id,i_item_desc,i_current_price
 ORDER BY i_item_id
 LIMIT 100;
