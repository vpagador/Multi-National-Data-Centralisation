--Milestone 4

--Task 1: How Many Stores does the business have in which countries?

SELECT 
	country_code,COUNT(country_code) 
    FROM 
        dim_store_details
	GROUP BY 
        country_code
	ORDER BY 
        country_code ASC

--Task 2: Which locations currently have the most stores?

SELECT 
    locality, COUNT(locality) 
    FROM 
        dim_store_details
    GROUP BY 
        locality
    ORDER BY 
        count DESC 
    LIMIT 7

--Task 3: Which months produce the highest cost of sales?

SELECT 	
    ROUND(CAST(SUM(orders_table.product_quantity*dim_products.product_price)AS NUMERIC),2) 
    AS sales,dim_date_times.month	
    FROM 
        orders_table
    INNER JOIN 
        dim_date_times ON dim_date_times.date_uuid = orders_table.date_uuid
    INNER JOIN 
        dim_products ON dim_products.product_code = orders_table.product_code
    GROUP BY 
        (dim_date_times.month)

--Task 4: How many sales are coming from online?

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

--Task 5: What percentage of sales come from each store? 

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

--Task 6: Which month in each year produced the highest cost of sales?

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

--Task 7: What is our staff headcount, divided by country?

SELECT 
    SUM(CAST(dim_store_details.staff_numbers AS NUMERIC)) as staff_numbers, dim_store_details.country_code 
    FROM 
        dim_store_details
    GROUP BY 
        country_code
    ORDER BY 
        staff_numbers DESC

--Task 8: Which store type is selling the most in Germany?

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

--Task 9: How quickly is the company making sales?

WITH cte AS(
    SELECT 
        CAST(CONCAT(year, '-', month, '-', day, ' ', timestamp) AS TIMESTAMP) as datetimes, year 
        FROM 
            dim_date_times
    ORDER BY 
        datetimes DESC), 
        cte2 AS
        (SELECT 
        year, 
        datetimes, 
        LEAD(datetimes, 1) OVER (ORDER BY datetimes DESC) AS time_difference 
        FROM cte) 
    SELECT year, AVG((datetimes - time_difference)) AS actual_time_taken 
    FROM cte2
GROUP BY 
        year
ORDER BY 
        actual_time_taken DESC
SELECT * 
    FROM 
        dim_date_details  
SELECT * 
    FROM 
        orders_table
INNER JOIN 
    dim_products ON dim_products.product_code = orders_table.product_code
SELECT 
    *, ts.sales * 100 /sum(sales)
FROM 
    (SELECT 
        dim_store.store_type,
           SUM(products.product_price * orders_table.product_quantity) AS sales
           FROM 
            orders_table AS orders_table
           INNER JOIN 
            dim_store_details ON orders_table.store_code = dim_store_details.store_code
           INNER JOIN 
            dim_products ON orders_table.product_code = dim_products.product_code
           GROUP BY 
            store_type
) as ts.sales

SELECT 
    ts.store_type, sum(ts.ts_sales) 
    FROM (
        SELECT 
            dim_store_details.store_type,
        SUM(dim_products.product_price * orders_table.product_quantity) AS ts_sales
        FROM 
            orders_table
        INNER JOIN  
        dim_store_details ON orders_table.store_code = dim_store.store_code
        INNER JOIN 
            dim_products ON orders_table.product_code = dim_products.product_code
        GROUP BY 
            dim_store_details.store_type
        ) AS ts

with cte as 
    (SELECT 
        dim_store_details.store_type, 
        SUM(dim_products.product_price * orders_table.product_quantity) AS sales
        FROM 
            orders_table AS orders_table
        INNER JOIN 
            dim_store_details ON orders_table.store_code = dim_store_details.store_code
        INNER JOIN 
            dim_products ON orders_table.product_code = dim_products.product_code
        GROUP BY 
            dim_store_details.store_type)
        SELECT 
            cte.store_type, cte.sales / (SELECT sum(cte.sales) FROM cte)
        FROM 
            cte
