select $1:_id:"$oid"::string            as offerlogdetail_id,
       $1:event::string         as event,
       $1:is_error::boolean     as is_error,
       $1:offer_log_id::string  as offerlog_id,
       $1:response::string      as response,
       $1:sms_blast_id::string  as sms_blast_id,
       {{ parse_json_date('$1:timestamp') }}  as timestamp,
       $1:url::string          as url,
       current_date             as asof_date
from {{ most_recent_s3_file_name('_STAGE', 'S3_MONGO_STAGE', '.*/smbsite_offerlogdetail.json') }}
