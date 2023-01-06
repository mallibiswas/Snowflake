SELECT COUNT(*) FROM {{ref('mart_merged_presence_pos__crm_contact_profiles')}} HAVING COUNT(*) != 1
UNION ALL SELECT COUNT(*) FROM (SELECT * FROM {{ref('mart_merged_presence_pos__crm_contact_profiles')}}  WHERE business_id = 'test_cc4_bid' AND pos_customer_id = 'test_cc4_customer_id') HAVING COUNT(*) != 1
