--Make changes to the dim_products for delivery team and cast types to dim_products

--Remove £ sign from product_price

UPDATE dim_products SET product_price = REPLACE(product_price, '£', '');

--Add new column weight_class

ALTER TABLE dim_products
ADD COLUMN weight_class VARCHAR(15);
UPDATE dim_products
SET weight_class =

	CASE  
		WHEN weight < 2 THEN '<2'
		WHEN weight BETWEEN 2 AND 39 THEN '>=2 - < 40'
		WHEN weight BETWEEN 40 AND 139 THEN '>= 40 - < 140'
		WHEN weight >= 140 THEN '=> 140'
	END;

--Update the dim_products with the required data types

ALTER TABLE dim_products
	RENAME COLUMN removed TO still_available;

ALTER TABLE dim_products
ALTER COLUMN product_price TYPE FLOAT USING product_price::DOUBLE PRECISION,
ALTER COLUMN weight TYPE FLOAT USING weight::DOUBLE PRECISION, 
ALTER COLUMN category TYPE VARCHAR(18),
ALTER COLUMN "EAN" TYPE VARCHAR(17),
ALTER COLUMN date_added TYPE DATE,
ALTER COLUMN uuid TYPE UUID USING uuid::UUID, 
ALTER COLUMN still_available TYPE BOOL USING (still_available='still_available'),
ALTER COLUMN product_code TYPE VARCHAR(11);
