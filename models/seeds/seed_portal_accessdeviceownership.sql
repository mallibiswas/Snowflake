{{ config(materialized='table') }}

SELECT 'cc1_accessdevice_id_1'::VARCHAR(16777216) AS accessdevice_id,
    'test_cc1_userprofile_id'::VARCHAR(16777216) AS userprofile_id,
    dateadd(hour, -1, current_timestamp())::TIMESTAMP_NTZ(9) AS created
UNION ALL SELECT 'cc2_accessdevice_id_1'::VARCHAR(16777216) AS accessdevice_id,
    'test_cc2_userprofile_id'::VARCHAR(16777216) AS userprofile_id,
    dateadd(hour, -1, current_timestamp())::TIMESTAMP_NTZ(9) AS created
