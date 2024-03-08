--Cast types to dim_users

ALTER TABLE dim_users
ALTER COLUMN first_name TYPE VARCHAR(225),
ALTER COLUMN last_name TYPE VARCHAR(225),
ALTER COLUMN date_of_birth TYPE DATE,
ALTER COLUMN country_code TYPE VARCHAR(2),
ALTER COLUMN user_uuid TYPE UUID USING user_uuid::UUID,
ALTER COLUMN  join_date TYPE DATE ;
