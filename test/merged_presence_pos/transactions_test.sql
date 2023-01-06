SELECT COUNT(*) FROM {{ref('mart_merged_presence_pos__transactions')}} HAVING COUNT(*) != 3
UNION ALL SELECT COUNT(*) FROM (SELECT pos_purchase_id FROM {{ref('mart_merged_presence_pos__transactions')}}  WHERE pos_purchase_id = 'test_cc3_purchase_id') HAVING COUNT(*) != 1
UNION ALL SELECT COUNT(*) FROM (SELECT pos_purchase_id FROM {{ref('mart_merged_presence_pos__transactions')}}  WHERE pos_purchase_id = 'test_cc4_purchase_id') HAVING COUNT(*) != 1
UNION ALL SELECT COUNT(*) FROM (SELECT pos_purchase_id FROM {{ref('mart_merged_presence_pos__transactions')}}  WHERE pos_purchase_id = 'test_cc5_purchase_id') HAVING COUNT(*) != 1
