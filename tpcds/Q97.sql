
WITH ssci AS (
SELECT ss_customer_sk customer_sk
      ,ss_item_sk item_sk
FROM store_sales,date_dim
WHERE ss_sold_date_sk = d_date_sk
  AND d_month_seq BETWEEN 1212 AND 1212 + 11
GROUP BY ss_customer_sk
        ,ss_item_sk),
csci AS(
 SELECT cs_bill_customer_sk customer_sk
      ,cs_item_sk item_sk
FROM catalog_sales,date_dim
WHERE cs_sold_date_sk = d_date_sk
  AND d_month_seq BETWEEN 1212 AND 1212 + 11
GROUP BY cs_bill_customer_sk
        ,cs_item_sk)
 SELECT  SUM(CASE WHEN ssci.customer_sk is not null AND csci.customer_sk is null THEN 1 ELSE 0 END) store_only
      ,SUM(CASE WHEN ssci.customer_sk is null AND csci.customer_sk is not null THEN 1 ELSE 0 END) catalog_only
      ,SUM(CASE WHEN ssci.customer_sk is not null AND csci.customer_sk is not null THEN 1 ELSE 0 END) store_and_catalog
FROM ssci full outer join csci on (ssci.customer_sk=csci.customer_sk
                               AND ssci.item_sk = csci.item_sk)
LIMIT 100;


