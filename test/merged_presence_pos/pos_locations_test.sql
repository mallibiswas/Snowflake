SELECT COUNT(*) FROM {{ref('mart_merged_presence_pos__pos_locations')}} HAVING COUNT(*) != 5
UNION ALL SELECT COUNT(*) FROM (SELECT business_id FROM {{ref('mart_merged_presence_pos__pos_locations')}}  WHERE business_id = 'test_cc1_bid') HAVING COUNT(*) != 1
UNION ALL SELECT COUNT(*) FROM (SELECT business_id FROM {{ref('mart_merged_presence_pos__pos_locations')}}  WHERE business_id = 'test_cc2_bid') HAVING COUNT(*) != 1
UNION ALL SELECT COUNT(*) FROM (SELECT business_id FROM {{ref('mart_merged_presence_pos__pos_locations')}}  WHERE business_id = 'test_cc3_bid') HAVING COUNT(*) != 1
UNION ALL SELECT COUNT(*) FROM (SELECT business_id FROM {{ref('mart_merged_presence_pos__pos_locations')}}  WHERE business_id = 'test_cc4_bid') HAVING COUNT(*) != 1
UNION ALL SELECT COUNT(*) FROM (SELECT business_id FROM {{ref('mart_merged_presence_pos__pos_locations')}}  WHERE business_id = 'test_cc5_bid') HAVING COUNT(*) != 1
