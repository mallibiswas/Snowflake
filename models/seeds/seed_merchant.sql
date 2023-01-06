{{ config(materialized='table') }}

SELECT 'test_cc1_merchant_id'::VARCHAR(16777216) AS id,
    'test_cc1_bid'::VARCHAR(16777216) AS zenreach_bid
UNION ALL SELECT 'test_cc2_merchant_id'::VARCHAR(16777216) AS id,
    'test_cc2_bid'::VARCHAR(16777216) AS zenreach_bid
UNION ALL SELECT 'test_cc3_merchant_id'::VARCHAR(16777216) AS id,
    'test_cc3_bid'::VARCHAR(16777216) AS zenreach_bid
UNION ALL SELECT 'test_cc4_merchant_id'::VARCHAR(16777216) AS id,
    'test_cc4_bid'::VARCHAR(16777216) AS zenreach_bid
UNION ALL SELECT 'test_cc5_merchant_id'::VARCHAR(16777216) AS id,
    'test_cc5_bid'::VARCHAR(16777216) AS zenreach_bid
