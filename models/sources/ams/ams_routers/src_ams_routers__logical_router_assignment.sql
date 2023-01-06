SELECT  $1                  AS id
       ,$2                  AS logical_router_id
       ,$3                  AS router_assignment_id
       ,$4::timestamp       AS assigned_in_crm
       ,$5::timestamp       AS unassigned_in_crm
       ,$6::timestamp       AS created
       ,$7::timestamp       AS updated
       ,current_timestamp() AS asof_date
FROM {{ most_recent_s3_file_name ('AMS_ROUTERS','AMS_ROUTERS_S3_STAGE','.*logical_router_assignment.csv') }}
