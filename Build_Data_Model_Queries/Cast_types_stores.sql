
--Modify dim_store_details and cast types to dim_store_details

--Update webstore row where there is a NULL

UPDATE dim_store_details
SET latitude = NULL
WHERE latitude = 'N/A';
UPDATE dim_store_details
SET longitude = NULL
WHERE longitude = 'N/A';

--Cast types to dim_store_details - to do this, ensure that web portal row is 'N/A' for VARCHAR and NULL for INT/FLOAT

ALTER TABLE dim_store_details
ALTER COLUMN longitude TYPE FLOAT USING longitude::DOUBLE PRECISION,       
ALTER COLUMN locality  TYPE VARCHAR(255),           
ALTER COLUMN store_code TYPE VARCHAR(12),             
ALTER COLUMN staff_numbers TYPE SMALLINT USING staff_numbers::SMALLINT,     
ALTER COLUMN opening_date TYPE DATE,      
ALTER COLUMN store_type TYPE VARCHAR(255),  
ALTER COLUMN latitude TYPE FLOAT USING latitude::DOUBLE PRECISION,   
ALTER COLUMN country_code TYPE VARCHAR(2),  
ALTER COLUMN continent TYPE VARCHAR(255);   

