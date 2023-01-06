SELECT COUNT(*) FROM {{ref('mart_merged_presence_pos__temporospatial_contact_profiles')}} HAVING COUNT(*) != 1
UNION ALL SELECT COUNT(*) FROM (SELECT * FROM {{ref('mart_merged_presence_pos__temporospatial_contact_profiles')}}  WHERE sighting_ids = 'test_cc3_bid_sighting_1' AND pos_customer_id = 'test_cc3_customer_id') HAVING COUNT(*) != 1
