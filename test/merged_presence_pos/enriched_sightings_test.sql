SELECT COUNT(*) FROM {{ref('mart_merged_presence_pos__enriched_sightings')}} HAVING COUNT(*) != 3
UNION ALL SELECT COUNT(*) FROM (SELECT business_id FROM {{ref('mart_merged_presence_pos__enriched_sightings')}}  WHERE sighting_id = 'test_cc1_bid_sighting_1') HAVING COUNT(*) != 1
UNION ALL SELECT COUNT(*) FROM (SELECT business_id FROM {{ref('mart_merged_presence_pos__enriched_sightings')}}  WHERE sighting_id = 'test_cc2_bid_sighting_1') HAVING COUNT(*) != 1
UNION ALL SELECT COUNT(*) FROM (SELECT business_id FROM {{ref('mart_merged_presence_pos__enriched_sightings')}}  WHERE sighting_id = 'test_cc3_bid_sighting_1') HAVING COUNT(*) != 1
