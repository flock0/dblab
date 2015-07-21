
SELECT  i_item_id
      ,i_item_desc 
      ,i_category 
      ,i_class 
      ,i_current_price
      ,SUM(ws_ext_sales_price) AS itemrevenue 
      ,SUM(ws_ext_sales_price)*100/SUM(SUM(ws_ext_sales_price)) over
          (partition by i_class) AS revenueratio
from	
	web_sales
    	,item 
    	,date_dim
WHERE 
	ws_item_sk = i_item_sk 
  	AND i_category in ('Music', 'Books', 'Children')
  	AND ws_sold_date_sk = d_date_sk
	AND d_date between cast('2002-05-01' AS date) 
				AND (cast('2002-05-01' AS date) + 30 days)
GROUP BY 
	i_item_id
        ,i_item_desc 
        ,i_category
        ,i_class
        ,i_current_price
ORDER BY 
	i_category
        ,i_class
        ,i_item_id
        ,i_item_desc
        ,revenueratio
LIMIT 100;
