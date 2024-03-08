--2: Which locations currently have the most stores?

SELECT 
    locality, COUNT(locality) 
    FROM 
        dim_store_details
    GROUP BY 
        locality
    ORDER BY 
        count DESC 
    LIMIT 7

