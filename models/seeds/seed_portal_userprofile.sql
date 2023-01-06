{{ config(materialized='table') }}

SELECT 'test_cc1@test.com'::VARCHAR(16777216) AS email, 
    'TRUE'::BOOLEAN AS email_is_valid,
    '1'::NUMBER(5,2) AS email_score,
    'sunset'::VARCHAR(16777216) AS email_reason,
    'test_cc1_userprofile_id'::VARCHAR(16777216) AS userprofile_id,
    current_timestamp()::TIMESTAMP_NTZ(9) AS date_added

UNION ALL SELECT 'test_cc2@test.com'::VARCHAR(16777216) AS email, 
    'TRUE'::BOOLEAN AS email_is_valid,
    '1'::NUMBER(5,2) AS email_score,
    'sunset'::VARCHAR(16777216) AS email_reason,
    'test_cc2_userprofile_id'::VARCHAR(16777216) AS userprofile_id,
    current_timestamp()::TIMESTAMP_NTZ(9) AS date_added

UNION ALL SELECT 'test_cc3@test.com'::VARCHAR(16777216) AS email, 
    'TRUE'::BOOLEAN AS email_is_valid,
    '1'::NUMBER(5,2) AS email_score,
    'sunset'::VARCHAR(16777216) AS email_reason,
    'test_cc2_userprofile_id'::VARCHAR(16777216) AS userprofile_id,
    current_timestamp()::TIMESTAMP_NTZ(9) AS date_added

UNION ALL SELECT 'test_cc4@test.com'::VARCHAR(16777216) AS email, 
    'TRUE'::BOOLEAN AS email_is_valid,
    '1'::NUMBER(5,2) AS email_score,
    'sunset'::VARCHAR(16777216) AS email_reason,
    'test_cc4_userprofile_id'::VARCHAR(16777216) AS userprofile_id,
    current_timestamp()::TIMESTAMP_NTZ(9) AS date_added
