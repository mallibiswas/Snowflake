{{ config(materialized='table') }}

SELECT 'test_cc1_bid'::VARCHAR(16777216) AS business_id, 
    'FALSE'::BOOLEAN AS test_business
UNION ALL SELECT 'test_cc2_bid'::VARCHAR(16777216) AS business_id, 
    'FALSE'::BOOLEAN AS test_business
UNION ALL SELECT 'test_cc3_bid'::VARCHAR(16777216) AS business_id, 
    'FALSE'::BOOLEAN AS test_business
UNION ALL SELECT 'test_cc4_bid'::VARCHAR(16777216) AS business_id, 
    'FALSE'::BOOLEAN AS test_business
UNION ALL SELECT 'test_cc5_bid'::VARCHAR(16777216) AS business_id, 
    'FALSE'::BOOLEAN AS test_business
