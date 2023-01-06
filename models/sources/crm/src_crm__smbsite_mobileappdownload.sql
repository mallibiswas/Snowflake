select $1:_id:"$oid"::string                   as mobileappdownload_id,
       {{ parse_json_date('$1:banner_dismissed') }}   as banner_dismissed,
       {{ parse_json_date('$1:banner_first_seen') }}  as banner_first_seen,
       $1:engagement_type::string      as engagement_type,
       $1:user_id:"$oid"::string         as user_id,
       $1:view_type::string            as view_type,
       current_date                    as asof_date
from {{ most_recent_s3_file_name('_STAGE', 'S3_MONGO_STAGE', '.*/smbsite_modileappdownload.json') }}
