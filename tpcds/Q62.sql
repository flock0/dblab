
SELECT  distinct(i_product_name)
 FROM item i1
 WHERE i_manufact_id BETWEEN 682 AND 682+40 
   AND (SELECT COUNT(*) AS item_cnt
        FROM item
        WHERE (i_manufact = i1.i_manufact AND
        ((i_category = 'Women' AND 
        (i_color = 'mint' or i_color = 'gainsboro') AND 
        (i_units = 'Lb' or i_units = 'Dram') AND
        (i_size = 'large' or i_size = 'petite')
        ) or
        (i_category = 'Women' AND
        (i_color = 'olive' or i_color = 'steel') AND
        (i_units = 'Unknown' or i_units = 'Cup') AND
        (i_size = 'extra large' or i_size = 'N/A')
        ) or
        (i_category = 'Men' AND
        (i_color = 'hot' or i_color = 'black') AND
        (i_units = 'Box' or i_units = 'Case') AND
        (i_size = 'medium' or i_size = 'economy')
        ) or
        (i_category = 'Men' AND
        (i_color = 'chiffon' or i_color = 'yellow') AND
        (i_units = 'Dozen' or i_units = 'Pallet') AND
        (i_size = 'large' or i_size = 'petite')
        ))) or
       (i_manufact = i1.i_manufact AND
        ((i_category = 'Women' AND 
        (i_color = 'cornflower' or i_color = 'peach') AND 
        (i_units = 'Bundle' or i_units = 'Tsp') AND
        (i_size = 'large' or i_size = 'petite')
        ) or
        (i_category = 'Women' AND
        (i_color = 'dark' or i_color = 'lime') AND
        (i_units = 'Tbl' or i_units = 'Bunch') AND
        (i_size = 'extra large' or i_size = 'N/A')
        ) or
        (i_category = 'Men' AND
        (i_color = 'cyan' or i_color = 'ivory') AND
        (i_units = 'Ounce' or i_units = 'Pound') AND
        (i_size = 'medium' or i_size = 'economy')
        ) or
        (i_category = 'Men' AND
        (i_color = 'snow' or i_color = 'sandy') AND
        (i_units = 'Oz' or i_units = 'Ton') AND
        (i_size = 'large' or i_size = 'petite')
        )))) > 0
 ORDER BY i_product_name
 LIMIT 100;
