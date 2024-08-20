--function for cleaning DOB(null values and >100 and >0)
CREATE OR REPLACE FUNCTION cleanse_dob(customer_since DATE, date_of_birth DATE)
RETURNS DATE
LANGUAGE SQL
AS
$$
  CASE 
    -- If date_of_birth is NULL, replace it with customer_since - 18 years
    WHEN date_of_birth IS NULL THEN DATEADD(YEAR, -18, customer_since)
    
    -- If age is greater than 100 years or less than 0, replace with customer_since - 18 years
    WHEN DATEDIFF(YEAR, date_of_birth, CURRENT_DATE) > 100 
         OR DATEDIFF(YEAR, date_of_birth, CURRENT_DATE) < 0 THEN DATEADD(YEAR, -18, customer_since)
    
    -- Otherwise, keep the original date_of_birth
    ELSE date_of_birth
  END
$$;


SELECT 
    customer_id,
    first_name,
    last_name,
    cleanse_dob(customer_since, date_of_birth) AS cleansed_dob
FROM 
    customer;


grant USAGE on FUNCTION cleanse_dob(DATE, DATE) to role PC_DBT_ROLE;


--UDF for cleanse first_name and last_name

CREATE OR REPLACE FUNCTION transform_first_name(first_name STRING)
RETURNS STRING
LANGUAGE SQL
AS
$$
    REGEXP_REPLACE(first_name, '[^a-zA-Z0-9]', '')
$$;


CREATE OR REPLACE FUNCTION transform_last_name(last_name STRING)
RETURNS STRING
LANGUAGE SQL
AS
$$
  REPLACE(last_name, ' ', '')
$$;

SELECT
  customer_id,
  transform_first_name(first_name) AS first_name_cleaned,
  transform_first_name(last_name) AS last_name_cleaned
FROM
  customer;

GRANT USAGE ON FUNCTION transform_first_name(STRING) TO ROLE PC_DBT_ROLE;
GRANT USAGE ON FUNCTION transform_last_name(STRING) TO ROLE PC_DBT_ROLE;


CREATE OR REPLACE PROCEDURE merge_cleansed_customer_data()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    -- Create a temporary table with cleansed data using UDFs
    CREATE OR REPLACE TEMPORARY TABLE customer_temp AS
    SELECT
        customer_id,
        transform_first_name(first_name) AS first_name,
        transform_last_name(last_name) AS last_name,
        cleanse_dob(customer_since, date_of_birth) AS date_of_birth,
        gender,
        email,
        phone_number,
        address,
        city,
        country,
        occupation,
        income_bracket,
        customer_since
    FROM
        customer;

    -- Merge the cleansed data into the original customer table
    MERGE INTO customer AS target
    USING customer_temp AS source
    ON target.customer_id = source.customer_id
    WHEN MATCHED THEN
        UPDATE SET
            target.first_name = source.first_name,
            target.last_name = source.last_name,
            target.date_of_birth = source.date_of_birth,
            target.gender = source.gender,
            target.email = source.email,
            target.phone_number = source.phone_number,
            target.address = source.address,
            target.city = source.city,
            target.country = source.country,
            target.occupation = source.occupation,
            target.income_bracket = source.income_bracket,
            target.customer_since = source.customer_since
    WHEN NOT MATCHED THEN
        INSERT (customer_id, first_name, last_name, date_of_birth, gender, email, phone_number, address, city, country, occupation, income_bracket, customer_since)
        VALUES (source.customer_id, source.first_name, source.last_name, source.date_of_birth, source.gender, source.email, source.phone_number, source.address, source.city, source.country, source.occupation, source.income_bracket, source.customer_since);

    RETURN 'Merge operation completed successfully.';
END;
$$;

CALL merge_cleansed_customer_data();


select * from customer;