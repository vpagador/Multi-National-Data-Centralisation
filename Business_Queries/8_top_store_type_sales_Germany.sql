--8: Which store type is selling the most in Germany?

SELECT 
    ROUND(CAST(SUM(dim_products.product_price * orders_table.product_quantity) AS NUMERIC), 2) as total_sales, dim_store_details.store_type, 
    dim_store_details.country_code 
    FROM 
        orders_table
    INNER JOIN 
        dim_products on orders_table.product_code = dim_products.product_code 
    INNER JOIN 
        dim_store_details on orders_table.store_code = dim_store_details.store_code 
    WHERE 
        dim_store_details.country_code = 'DE'
    GROUP BY 
        store_type, country_code
    ORDER BY 
        total_sales 

