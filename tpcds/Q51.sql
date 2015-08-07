
WITH web_v1 AS (
SELECT
  ws_item_sk item_sk, d_date,
  SUM(SUM(ws_sales_price))
      over (partition by ws_item_sk ORDER BY d_date rows BETWEEN unbounded preceding AND current row) cume_sales
FROM web_sales
    ,date_dim
WHERE ws_sold_date_sk=d_date_sk
  AND d_month_seq BETWEEN 1212 AND 1212+11
  AND ws_item_sk is not NULL
GROUP BY ws_item_sk, d_date),
store_v1 AS (
SELECT
  ss_item_sk item_sk, d_date,
  SUM(SUM(ss_sales_price))
      over (partition by ss_item_sk ORDER BY d_date rows BETWEEN unbounded preceding AND current row) cume_sales
FROM store_sales
    ,date_dim
WHERE ss_sold_date_sk=d_date_sk
  AND d_month_seq BETWEEN 1212 AND 1212+11
  AND ss_item_sk is not NULL
GROUP BY ss_item_sk, d_date)
 SELECT  *
FROM (SELECT item_sk
     ,d_date
     ,web_sales
     ,store_sales
     ,MAX(web_sales)
         over (partition by item_sk ORDER BY d_date rows BETWEEN unbounded preceding AND current row) web_cumulative
     ,MAX(store_sales)
         over (partition by item_sk ORDER BY d_date rows BETWEEN unbounded preceding AND current row) store_cumulative
     FROM (SELECT CASE WHEN web.item_sk is not null THEN web.item_sk ELSE store.item_sk END item_sk
                 ,CASE WHEN web.d_date is not null THEN web.d_date ELSE store.d_date END d_date
                 ,web.cume_sales web_sales
                 ,store.cume_sales store_sales
           FROM web_v1 web full outer join store_v1 store on (web.item_sk = store.item_sk
                                                          AND web.d_date = store.d_date)
          )x )y
WHERE web_cumulative > store_cumulative
ORDER BY item_sk
        ,d_date
LIMIT 100;


