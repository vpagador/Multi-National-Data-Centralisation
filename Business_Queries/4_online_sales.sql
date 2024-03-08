--4: How many sales are coming from online?

SELECT 	
    COUNT(*) AS number_of_sales,
    SUM(orders_table.product_quantity)
    AS product_quantity_count,
		CASE 
		WHEN 
            dim_store_details.store_type = 'Web Portal' 
        THEN
		'online'
		ELSE'offline'
		END
FROM 
    orders_table
LEFT JOIN 
    dim_store_details ON orders_table.store_code = dim_store_details.store_code
LEFT JOIN
    dim_products ON dim_products.product_code = orders_table.product_code
GROUP BY 
		CASE 
		WHEN dim_store_details.store_type = 'Web Portal' THEN
		'online'
		ELSE'offline'
		END

