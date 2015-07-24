
SELECT  DISTINCT(i_product_name)
 FROM item i1
 WHERE i_manufact_id BETWEEN 742 AND 742+40 
   AND (SELECT COUNT(*) AS item_cnt
        FROM item
        WHERE (i_manufact = i1.i_manufact AND
        ((i_category = 'Women' AND 
        (i_colOR = 'orchid' OR i_colOR = 'papaya') AND 
        (i_units = 'Pound' OR i_units = 'Lb') AND
        (i_size = 'petite' OR i_size = 'medium')
        ) OR
        (i_category = 'Women' AND
        (i_colOR = 'burlywood' OR i_colOR = 'navy') AND
        (i_units = 'Bundle' OR i_units = 'Each') AND
        (i_size = 'N/A' OR i_size = 'extra large')
        ) OR
        (i_category = 'Men' AND
        (i_colOR = 'bisque' OR i_colOR = 'azure') AND
        (i_units = 'N/A' OR i_units = 'Tsp') AND
        (i_size = 'small' OR i_size = 'large')
        ) OR
        (i_category = 'Men' AND
        (i_colOR = 'chocolate' OR i_colOR = 'cornflower') AND
        (i_units = 'Bunch' OR i_units = 'Gross') AND
        (i_size = 'petite' OR i_size = 'medium')
        ))) OR
       (i_manufact = i1.i_manufact AND
        ((i_category = 'Women' AND 
        (i_colOR = 'salmon' OR i_colOR = 'midnight') AND 
        (i_units = 'Oz' OR i_units = 'Box') AND
        (i_size = 'petite' OR i_size = 'medium')
        ) OR
        (i_category = 'Women' AND
        (i_colOR = 'snow' OR i_colOR = 'steel') AND
        (i_units = 'Carton' OR i_units = 'Tbl') AND
        (i_size = 'N/A' OR i_size = 'extra large')
        ) OR
        (i_category = 'Men' AND
        (i_colOR = 'purple' OR i_colOR = 'gainsboro') AND
        (i_units = 'Dram' OR i_units = 'Unknown') AND
        (i_size = 'small' OR i_size = 'large')
        ) OR
        (i_category = 'Men' AND
        (i_colOR = 'metallic' OR i_colOR = 'forest') AND
        (i_units = 'Gram' OR i_units = 'Ounce') AND
        (i_size = 'petite' OR i_size = 'medium')
        )))) > 0
 ORDER BY i_product_name
 LIMIT 100;


