--5: What percentage of sales come from each store? 

SELECT 
    dim_store_details.store_type, 
    SUM(orders_table.product_quantity*dim_products.product_price)
    AS total_sales,
    COUNT( * ) / CAST((SELECT COUNT( * ) FROM orders_table) AS NUMERIC) * 100 as "percentage_total(%)"
    FROM 
        orders_table
    LEFT JOIN 
        dim_store_details ON orders_table.store_code = dim_store_details.store_code
    LEFT JOIN
        dim_products ON dim_products.product_code = orders_table.product_code
    GROUP BY 
        dim_store_details.store_type 

