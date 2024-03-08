--7: What is our staff headcount, divided by country?

SELECT 
    SUM(CAST(dim_store_details.staff_numbers AS NUMERIC)) as staff_numbers, dim_store_details.country_code 
    FROM 
        dim_store_details
    GROUP BY 
        country_code
    ORDER BY 
        staff_numbers DESC
