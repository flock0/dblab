
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
    AND date_dim.d_dom between 1 AND 2 
    AND (household_demographics.hd_buy_potential = '501-1000' or
         household_demographics.hd_buy_potential = '5001-10000')
    AND household_demographics.hd_vehicle_count > 0
    AND CASE WHEN household_demographics.hd_vehicle_count > 0 THEN 
             household_demographics.hd_dep_count/ household_demographics.hd_vehicle_count ELSE NULL END > 1
    AND date_dim.d_year in (1999,1999+1,1999+2)
    AND store.s_county in ('Williamson County','Williamson County','Williamson County','Williamson County')
    GROUP BY ss_ticket_number,ss_customer_sk) dj,customer
    WHERE ss_customer_sk = c_customer_sk
      AND cnt between 1 AND 5
    ORDER BY cnt desc, c_last_name ASc;
