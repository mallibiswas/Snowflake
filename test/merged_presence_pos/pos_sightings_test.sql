SELECT COUNT(*) FROM {{ref('mart_merged_presence_pos__pos_sightings')}} HAVING COUNT(*) != 3
UNION ALL SELECT COUNT(*) FROM (SELECT sighting_id FROM {{ref('mart_merged_presence_pos__pos_sightings')}}  WHERE sighting_id = 'test_cc1_bid_sighting_1') HAVING COUNT(*) != 1
UNION ALL SELECT COUNT(*) FROM (SELECT sighting_id FROM {{ref('mart_merged_presence_pos__pos_sightings')}}  WHERE sighting_id = 'test_cc1_bid_sighting_1') HAVING COUNT(*) != 1
UNION ALL SELECT COUNT(*) FROM (SELECT sighting_id FROM {{ref('mart_merged_presence_pos__pos_sightings')}}  WHERE sighting_id = 'test_cc1_bid_sighting_1') HAVING COUNT(*) != 1
