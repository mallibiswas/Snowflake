SELECT COUNT(*) FROM {{ref('mart_merged_presence_pos__wifi_pos_presence')}} HAVING COUNT(*) != 5
UNION ALL SELECT COUNT(*) FROM (SELECT * FROM {{ref('mart_merged_presence_pos__wifi_pos_presence')}} WHERE 
    custom_conversion_type = 'CC1' 
    AND business_id = 'test_cc1_bid' 
    AND email_1 = 'test_cc1@test.com' ) 
HAVING COUNT(*) != 1
UNION ALL SELECT COUNT(*) FROM (SELECT * FROM {{ref('mart_merged_presence_pos__wifi_pos_presence')}} WHERE 
    custom_conversion_type = 'CC2' 
    AND business_id = 'test_cc2_bid' 
    AND email_1 = 'test_cc2@test.com' ) 
HAVING COUNT(*) != 1
UNION ALL SELECT COUNT(*) FROM (SELECT * FROM {{ref('mart_merged_presence_pos__wifi_pos_presence')}} WHERE 
    custom_conversion_type = 'CC3' 
    AND business_id = 'test_cc3_bid' 
    AND email_1 = 'test_cc3@test.com' ) 
HAVING COUNT(*) != 1
UNION ALL SELECT COUNT(*) FROM (SELECT * FROM {{ref('mart_merged_presence_pos__wifi_pos_presence')}} WHERE 
    custom_conversion_type = 'CC4' 
    AND business_id = 'test_cc4_bid' 
    AND email_1 = 'test_cc4@test.com' ) 
HAVING COUNT(*) != 1
UNION ALL SELECT COUNT(*) FROM (SELECT * FROM {{ref('mart_merged_presence_pos__wifi_pos_presence')}} WHERE 
    custom_conversion_type = 'CC5' 
    AND business_id = 'test_cc5_bid' 
    AND email_1 = 'test_cc5@test.com' ) 
HAVING COUNT(*) != 1
