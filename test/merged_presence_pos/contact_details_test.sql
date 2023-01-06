SELECT COUNT(*) FROM {{ref('mart_merged_presence_pos__contact_details')}} HAVING COUNT(*) != 4
UNION ALL SELECT COUNT(*) FROM (SELECT contact FROM {{ref('mart_merged_presence_pos__contact_details')}}  WHERE contact = 'test_cc1@test.com') HAVING COUNT(*) != 1
UNION ALL SELECT COUNT(*) FROM (SELECT contact FROM {{ref('mart_merged_presence_pos__contact_details')}}  WHERE contact = 'test_cc2@test.com') HAVING COUNT(*) != 1
UNION ALL SELECT COUNT(*) FROM (SELECT contact FROM {{ref('mart_merged_presence_pos__contact_details')}}  WHERE contact = 'test_cc3@test.com') HAVING COUNT(*) != 1
UNION ALL SELECT COUNT(*) FROM (SELECT contact FROM {{ref('mart_merged_presence_pos__contact_details')}}  WHERE contact = 'test_cc4@test.com') HAVING COUNT(*) != 1
