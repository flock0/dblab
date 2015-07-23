
SELECT 
  s_store_name,
  i_item_desc,
  sc.revenue,
  i_current_price,
  i_wholesale_cost,
  i_brand
FROM store
INNER JOIN 
  (SELECT  ss_store_sk, ss_item_sk, SUM(ss_sales_price) AS revenue
   FROM store_sales INNER JOIN date_dim ON ss_sold_date_sk = d_date_sk
   WHERE d_month_seq BETWEEN 1178 AND 1178+11
   GROUP BY ss_store_sk, ss_item_sk) sc ON s_store_sk = sc.ss_store_sk
INNER JOIN
  (SELECT ss_store_sk, AVG(revenue) AS ave
   FROM
      (SELECT  ss_store_sk, ss_item_sk, SUM(ss_sales_price) AS revenue
       FROM store_sales INNER JOIN date_dim ON ss_sold_date_sk = d_date_sk 
       WHERE d_month_seq BETWEEN 1178 AND 1178+11
       GROUP BY ss_store_sk, ss_item_sk) sa
   GROUP BY ss_store_sk) sb ON sb.ss_store_sk = sc.ss_store_sk
INNER JOIN item ON i_item_sk = sc.ss_item_sk
WHERE sc.revenue <= 0.1 * sb.ave
ORDER BY s_store_name, i_item_desc
LIMIT 100;
