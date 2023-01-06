SELECT  $1                  AS id
       ,$2                  AS business_entity_id
       ,$3                  AS cloud_network_id
       ,$4                  AS meraki_network_id
       ,current_timestamp() AS asof_date
FROM {{ most_recent_s3_file_name ('AMS_ROUTERS','AMS_ROUTERS_S3_STAGE','.*network_assignment.csv') }}
