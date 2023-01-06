{{ config(materialized='table') }}

SELECT 'test_cc3_merchant_id'::VARCHAR(16777216) AS merchant_id,
    'test_cc3_customer_id'::VARCHAR(16777216) AS customer_id,
    dateadd(hour, -35, current_timestamp())::TIMESTAMP_NTZ(9) AS foreign_created_time,
    '500'::NUMBER(38,0) AS total,
    'test_cc3_purchase_id'::VARCHAR(16777216) AS id,
    'COMPLETED_STATUS'::VARCHAR(16777216) AS "status"
UNION ALL SELECT 'test_cc4_merchant_id'::VARCHAR(16777216) AS merchant_id,
    'test_cc4_customer_id'::VARCHAR(16777216) AS customer_id,
    dateadd(hour, -35, current_timestamp())::TIMESTAMP_NTZ(9) AS foreign_created_time,
    '600'::NUMBER(38,0) AS total,
    'test_cc4_purchase_id'::VARCHAR(16777216) AS id,
    'COMPLETED_STATUS'::VARCHAR(16777216) AS "status"
UNION ALL SELECT 'test_cc5_merchant_id'::VARCHAR(16777216) AS merchant_id,
    'test_cc5_customer_id'::VARCHAR(16777216) AS customer_id,
    dateadd(hour, -35, current_timestamp())::TIMESTAMP_NTZ(9) AS foreign_created_time,
    '700'::NUMBER(38,0) AS total,
    'test_cc5_purchase_id'::VARCHAR(16777216) AS id,
    'COMPLETED_STATUS'::VARCHAR(16777216) AS "status"


