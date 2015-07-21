
SELECT  *
FROM (SELECT AVG(ss_list_price) B1_LP
            ,COUNT(ss_list_price) B1_CNT
            ,COUNT(distinct ss_list_price) B1_CNTD
      FROM store_sales
      WHERE ss_quantity between 0 AND 5
        AND (ss_list_price between 53 AND 53+10 
             or ss_coupon_amt between 7032 AND 7032+1000
             or ss_wholesale_cost between 54 AND 54+20)) B1,
     (SELECT AVG(ss_list_price) B2_LP
            ,COUNT(ss_list_price) B2_CNT
            ,COUNT(distinct ss_list_price) B2_CNTD
      FROM store_sales
      WHERE ss_quantity between 6 AND 10
        AND (ss_list_price between 137 AND 137+10
          or ss_coupon_amt between 4205 AND 4205+1000
          or ss_wholesale_cost between 48 AND 48+20)) B2,
     (SELECT AVG(ss_list_price) B3_LP
            ,COUNT(ss_list_price) B3_CNT
            ,COUNT(distinct ss_list_price) B3_CNTD
      FROM store_sales
      WHERE ss_quantity between 11 AND 15
        AND (ss_list_price between 151 AND 151+10
          or ss_coupon_amt between 762 AND 762+1000
          or ss_wholesale_cost between 18 AND 18+20)) B3,
     (SELECT AVG(ss_list_price) B4_LP
            ,COUNT(ss_list_price) B4_CNT
            ,COUNT(distinct ss_list_price) B4_CNTD
      FROM store_sales
      WHERE ss_quantity between 16 AND 20
        AND (ss_list_price between 10 AND 10+10
          or ss_coupon_amt between 13315 AND 13315+1000
          or ss_wholesale_cost between 40 AND 40+20)) B4,
     (SELECT AVG(ss_list_price) B5_LP
            ,COUNT(ss_list_price) B5_CNT
            ,COUNT(distinct ss_list_price) B5_CNTD
      FROM store_sales
      WHERE ss_quantity between 21 AND 25
        AND (ss_list_price between 106 AND 106+10
          or ss_coupon_amt between 6051 AND 6051+1000
          or ss_wholesale_cost between 21 AND 21+20)) B5,
     (SELECT AVG(ss_list_price) B6_LP
            ,COUNT(ss_list_price) B6_CNT
            ,COUNT(distinct ss_list_price) B6_CNTD
      FROM store_sales
      WHERE ss_quantity between 26 AND 30
        AND (ss_list_price between 21 AND 21+10
          or ss_coupon_amt between 441 AND 441+1000
          or ss_wholesale_cost between 52 AND 52+20)) B6
LIMIT 100;
