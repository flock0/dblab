
SELECT  * FROM 
(SELECT i_manufact_id,
SUM(ss_sales_price) sum_sales,
AVG(SUM(ss_sales_price)) over (partition by i_manufact_id) avg_quarterly_sales
FROM item, store_sales, date_dim, store
WHERE ss_item_sk = i_item_sk AND
ss_sold_date_sk = d_date_sk AND
ss_store_sk = s_store_sk AND
d_month_seq in (1205,1205+1,1205+2,1205+3,1205+4,1205+5,1205+6,1205+7,1205+8,1205+9,1205+10,1205+11) AND
((i_category in ('Books','Children','Electronics') AND
i_class in ('personal','portable','reference','self-help') AND
i_brAND in ('scholaramalgamalg #14','scholaramalgamalg #7',
		'exportiunivamalg #9','scholaramalgamalg #9'))
or(i_category in ('Women','Music','Men') AND
i_class in ('accessories','classical','fragrances','pants') AND
i_brAND in ('amalgimporto #1','edu packscholar #1','exportiimporto #1',
		'importoamalg #1')))
GROUP BY i_manufact_id, d_qoy ) tmp1
WHERE CASE WHEN avg_quarterly_sales > 0 
	then abs (sum_sales - avg_quarterly_sales)/ avg_quarterly_sales 
	else NULL END > 0.1
ORDER BY avg_quarterly_sales,
	 sum_sales,
	 i_manufact_id
LIMIT 100;
