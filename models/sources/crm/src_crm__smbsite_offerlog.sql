select $1:_id:"$oid"::string             as offerlog_id,
       $1:business_id:"$oid"::string    as business_id,
       $1:userprofile_id:"$oid"::string as userprofile_id,
       $1:code::string           as offer_code,
       {{ parse_json_date('$1:created') }}      as created,
       {{ parse_json_date('$1:expiration') }}   as expiration,
       $1:offer_id:"$oid"::string       as offer_id,
       {{ parse_json_date('$1:opened') }}       as opened,
       {{ parse_json_date('$1:redeemed') }}     as redeemed,
       $1:redirect_id:"$oid"::string    as redirect_id,
       $1:test_user::boolean     as test_user,
       current_date              as asof_date
from {{ most_recent_s3_file_name('_STAGE', 'S3_MONGO_STAGE', '.*/smbsite_offerlog.json') }}
