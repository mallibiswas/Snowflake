SELECT  $1                  AS id
       ,$2                  AS router_id
       ,$3                  AS mac
       ,$4::timestamp       AS created_in_crm
       ,$5::timestamp       AS created
       ,$6::timestamp       AS updated
       ,current_timestamp() AS asof_date
FROM {{ most_recent_s3_file_name ('AMS_ROUTERS','AMS_ROUTERS_S3_STAGE','.*logical_router.csv') }}
