--6: Which month in each year produced the highest cost of sales?

SELECT 
    ROUND(CAST(SUM(dim_products.product_price * orders_table.product_quantity) AS NUMERIC), 2) as total_sales, dim_date_times.year, dim_date_times.month 
    FROM 
        orders_table

    LEFT JOIN 
        dim_date_times ON orders_table.date_uuid  = dim_date_times.date_uuid
    LEFT JOIN 
        dim_products ON orders_table.product_code = dim_products.product_code
    GROUP BY 
        dim_date_times.year, dim_date_times.month
    ORDER BY 
        total_sales DESC LIMIT 10

