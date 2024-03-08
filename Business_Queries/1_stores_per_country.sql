--1: How Many Stores does the business have in which countries?

SELECT 
	country_code,COUNT(country_code) 
    FROM 
        dim_store_details
	GROUP BY 
        country_code
	ORDER BY 
        country_code ASC

