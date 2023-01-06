{{ config(materialized='table') }}

SELECT 'cc1_accessdevice_id_1'::VARCHAR(16777216) AS accessdevice_id,
    'test_cc1_client_mac'::VARCHAR(16777216) AS mac
UNION ALL SELECT 'cc2_accessdevice_id_1'::VARCHAR(16777216) AS accessdevice_id,
    'test_cc2_client_mac'::VARCHAR(16777216) AS mac
UNION ALL SELECT 'cc3_accessdevice_id_1'::VARCHAR(16777216) AS accessdevice_id,
    'test_cc3_client_mac'::VARCHAR(16777216) AS mac
