{{ config(materialized='table') }}

SELECT 'test_cc1@test.com'::VARCHAR(16777216) AS email, 
    array_construct('test_cc1@test.com')::VARIANT AS emails, 
    'test_cc1_bid'::VARCHAR(16777216) AS business_id, 
    'test_cc1_fullname'::VARCHAR(16777216) AS fullname,
    '25'::VARCHAR(16777216) AS age,
    'M'::VARCHAR(16777216) AS gender,
    '50000'::VARCHAR(16777216) AS income,
    current_timestamp()::TIMESTAMP_NTZ(9) AS created

UNION ALL SELECT 'test_cc2@test.com'::VARCHAR(16777216) AS email, 
    array_construct('test_cc2@test.com')::VARIANT AS emails, 
    'test_cc2_bid'::VARCHAR(16777216) AS business_id, 
    'test_cc2_fullname'::VARCHAR(16777216) AS fullname,
    '26'::VARCHAR(16777216) AS age,
    'F'::VARCHAR(16777216) AS gender,
    '60000'::VARCHAR(16777216) AS income,
    current_timestamp()::TIMESTAMP_NTZ(9) AS created

UNION ALL SELECT 'test_cc3@test.com'::VARCHAR(16777216) AS email, 
    array_construct('test_cc3@test.com')::VARIANT AS emails, 
    'test_cc3_bid'::VARCHAR(16777216) AS business_id, 
    'test_cc3_fullname'::VARCHAR(16777216) AS fullname,
    '27'::VARCHAR(16777216) AS age,
    'M'::VARCHAR(16777216) AS gender,
    '70000'::VARCHAR(16777216) AS income,
    current_timestamp()::TIMESTAMP_NTZ(9) AS created

UNION ALL SELECT 'test_cc4@test.com'::VARCHAR(16777216) AS email, 
    array_construct('test_cc4@test.com')::VARIANT AS emails, 
    'test_cc4_bid'::VARCHAR(16777216) AS business_id, 
    'test_cc4_fullname'::VARCHAR(16777216) AS fullname,
    '28'::VARCHAR(16777216) AS age,
    'F'::VARCHAR(16777216) AS gender,
    '80000'::VARCHAR(16777216) AS income,
    current_timestamp()::TIMESTAMP_NTZ(9) AS created
