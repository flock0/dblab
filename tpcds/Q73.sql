
SELECT c_last_name
       ,c_first_name
       ,c_salutation
       ,c_preferred_cust_flag
       ,ss_ticket_number
       ,cnt from
   (SELECT ss_ticket_number
          ,ss_customer_sk
          ,COUNT(*) cnt
    FROM store_sales,date_dim,store,household_demographics
    WHERE store_sales.ss_sold_date_sk = date_dim.d_date_sk
    AND store_sales.ss_store_sk = store.s_store_sk  
    AND store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
    AND (date_dim.d_dom BETWEEN 1 AND 3 or date_dim.d_dom BETWEEN 25 AND 28)
    AND (household_demographics.hd_buy_potential = '1001-5000' or
         household_demographics.hd_buy_potential = '5001-10000')
    AND household_demographics.hd_vehicle_count > 0
    AND (CASE WHEN household_demographics.hd_vehicle_count > 0 
	then household_demographics.hd_dep_count/ household_demographics.hd_vehicle_count 
	else NULL 
	end)  > 1.2
    AND date_dim.d_year in (1998,1998+1,1998+2)
    AND store.s_county in ('Williamson County','Williamson County','Williamson County','Williamson County',
                           'Williamson County','Williamson County','Williamson County','Williamson County')
    GROUP BY ss_ticket_number,ss_customer_sk) dn,customer
    WHERE ss_customer_sk = c_customer_sk
      AND cnt BETWEEN 15 AND 20
    ORDER BY c_last_name,c_first_name,c_salutation,c_preferred_cust_flag desc;
