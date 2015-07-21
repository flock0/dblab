
SELECT  dt.d_year
 	,item.i_category_id
 	,item.i_category
 	,SUM(ss_ext_sales_price)
 FROM 	date_dim dt
 	,store_sales
 	,item
 WHERE dt.d_date_sk = store_sales.ss_sold_date_sk
 	AND store_sales.ss_item_sk = item.i_item_sk
 	AND item.i_manager_id = 1  	
 	AND dt.d_moy=12
 	AND dt.d_year=2000
 GROUP BY 	dt.d_year
 		,item.i_category_id
 		,item.i_category
 ORDER BY       SUM(ss_ext_sales_price) desc,dt.d_year
 		,item.i_category_id
 		,item.i_category
LIMIT 100 ;
