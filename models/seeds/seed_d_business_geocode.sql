{{ config(materialized='table') }}

SELECT 'test_cc1_bid'::VARCHAR(16777216) AS business_id,
    '-8'::NUMBER(28,10) AS timezone_utc_offset,
    '20'::NUMBER(28,10) AS latitude,
    '100'::NUMBER(28,10) AS longitude,
    'test_cc1_bid_zip'::VARCHAR(16777216) AS zip
UNION ALL SELECT 'test_cc2_bid'::VARCHAR(16777216) AS business_id,
    '-8'::NUMBER(28,10) AS timezone_utc_offset,
    '20'::NUMBER(28,10) AS latitude,
    '100'::NUMBER(28,10) AS longitude,
    'test_cc2_bid_zip'::VARCHAR(16777216) AS zip
UNION ALL SELECT 'test_cc3_bid'::VARCHAR(16777216) AS business_id,
    '-8'::NUMBER(28,10) AS timezone_utc_offset,
    '20'::NUMBER(28,10) AS latitude,
    '100'::NUMBER(28,10) AS longitude,
    'test_cc3_bid_zip'::VARCHAR(16777216) AS zip
UNION ALL SELECT 'test_cc4_bid'::VARCHAR(16777216) AS business_id,
    '-8'::NUMBER(28,10) AS timezone_utc_offset,
    '20'::NUMBER(28,10) AS latitude,
    '100'::NUMBER(28,10) AS longitude,
    'test_cc4_bid_zip'::VARCHAR(16777216) AS zip
UNION ALL SELECT 'test_cc5_bid'::VARCHAR(16777216) AS business_id,
    '-8'::NUMBER(28,10) AS timezone_utc_offset,
    '20'::NUMBER(28,10) AS latitude,
    '100'::NUMBER(28,10) AS longitude,
    'test_cc5_bid_zip'::VARCHAR(16777216) AS zip
