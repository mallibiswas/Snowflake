SELECT COUNT(*) FROM {{ref('mart_merged_presence_pos__local_crm_contacts')}} HAVING COUNT(*) != 20
