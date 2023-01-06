SELECT  $1                  AS id
       ,$2                  AS account_id
       ,$3                  AS mac_start
       ,$4                  AS mac_end
       ,$5                  AS router_type
       ,$6::timestamp       AS created
       ,$7::timestamp       AS updated
       ,$8::timestamp       AS deleted
       ,$9                  AS node_id
       ,$10                 AS serial_number
       ,current_timestamp() AS asof_date
FROM {{ most_recent_s3_file_name ('AMS_ROUTERS','AMS_ROUTERS_S3_STAGE','.*router.csv') }}
