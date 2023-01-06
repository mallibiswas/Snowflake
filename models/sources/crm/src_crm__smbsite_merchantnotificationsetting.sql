select $1:_id:"$oid"::string                       as merchantnotificationsetting_id,
       $1:userprofile_id:"$oid"::string           as userprofile_id,
       $1:business_id:"$oid"::string              as business_id,
       {{ parse_json_date('$1:date_added') }}             as date_added,
       $1:email::string                   as email,
       $1:reputation_notification::variant as reputation_notification,
       {{ parse_json_date('$1:updated') }}                as updated,
       current_date                        as asof_date
from {{ most_recent_s3_file_name('_STAGE', 'S3_MONGO_STAGE', '.*/smbsite_merchantnotificationsetting.json') }}
