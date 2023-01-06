{{ config(materialized='table') }}

SELECT 'test_cc1_bid'::VARCHAR(16777216) AS business_id,
    'test_cc1_business_name'::VARCHAR(16777216) AS business_name,
    'test_cc1_parent_bid'::VARCHAR(16777216) AS parent_id,
    'test_cc1_parent_name'::VARCHAR(16777216) AS parent_name
UNION ALL SELECT 'test_cc2_bid'::VARCHAR(16777216) AS business_id,
    'test_cc2_business_name'::VARCHAR(16777216) AS business_name,
    'test_cc2_parent_bid'::VARCHAR(16777216) AS parent_id,
    'test_cc2_parent_name'::VARCHAR(16777216) AS parent_name
UNION ALL SELECT 'test_cc3_bid'::VARCHAR(16777216) AS business_id,
    'test_cc3_business_name'::VARCHAR(16777216) AS business_name,
    'test_cc3_parent_bid'::VARCHAR(16777216) AS parent_id,
    'test_cc3_parent_name'::VARCHAR(16777216) AS parent_name
UNION ALL SELECT 'test_cc4_bid'::VARCHAR(16777216) AS business_id,
    'test_cc4_business_name'::VARCHAR(16777216) AS business_name,
    'test_cc4_parent_bid'::VARCHAR(16777216) AS parent_id,
    'test_cc4_parent_name'::VARCHAR(16777216) AS parent_name
UNION ALL SELECT 'test_cc5_bid'::VARCHAR(16777216) AS business_id,
    'test_cc5_business_name'::VARCHAR(16777216) AS business_name,
    'test_cc5_parent_bid'::VARCHAR(16777216) AS parent_id,
    'test_cc5_parent_name'::VARCHAR(16777216) AS parent_name
