SELECT COUNT(*) FROM {{ref('mart_merged_presence_pos__pos_customers')}} HAVING COUNT(*) != 3
UNION ALL SELECT COUNT(*) FROM (SELECT * FROM {{ref('mart_merged_presence_pos__pos_customers')}}  WHERE business_id = 'test_cc3_bid' AND pos_customer_id = 'test_cc3_customer_id') HAVING COUNT(*) != 1
UNION ALL SELECT COUNT(*) FROM (SELECT * FROM {{ref('mart_merged_presence_pos__pos_customers')}}  WHERE business_id = 'test_cc4_bid' AND pos_customer_id = 'test_cc4_customer_id') HAVING COUNT(*) != 1
UNION ALL SELECT COUNT(*) FROM (SELECT * FROM {{ref('mart_merged_presence_pos__pos_customers')}}  WHERE business_id = 'test_cc5_bid' AND pos_customer_id = 'test_cc5_customer_id') HAVING COUNT(*) != 1
