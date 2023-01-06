select $1:business_id:"$oid"::string as business_id,
       {{ parse_json_date('$1:created') }}   as created,
       $1:_id:"$oid"::string          as target_id,
       $1:post_data::variant  as post_data,
       current_date           as asof_date
from {{ most_recent_s3_file_name('_STAGE', 'S3_MONGO_STAGE', '.*/smbsite_target.json') }}
