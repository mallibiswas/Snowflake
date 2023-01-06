{{ config(materialized='table') }}

SELECT 'test_cc3_merchant_id'::VARCHAR(16777216) AS merchant_id,
     'test_cc3@test.com'::VARCHAR(16777216) AS primary_email,
     'test_cc3_customer_id'::VARCHAR(16777216) AS id,
     'test_cc3_fullname'::VARCHAR(16777216) AS "name"
UNION ALL SELECT 'test_cc4_merchant_id'::VARCHAR(16777216) AS merchant_id,
     'test_cc4@test.com'::VARCHAR(16777216) AS primary_email,
     'test_cc4_customer_id'::VARCHAR(16777216) AS id,
     'test_cc4_fullname'::VARCHAR(16777216) AS "name"
UNION ALL SELECT 'test_cc5_merchant_id'::VARCHAR(16777216) AS merchant_id,
     'test_cc5@test.com'::VARCHAR(16777216) AS primary_email,
     'test_cc5_customer_id'::VARCHAR(16777216) AS id,
     'test_cc5_fullname'::VARCHAR(16777216) AS "name"
