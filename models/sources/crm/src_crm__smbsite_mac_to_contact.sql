select $1:_id:"$oid"::string           as mac_to_contact_id,
       $1:account_id:"$oid"::string   as account_id,
       $1:contact_id:"$oid"::string   as contact_id,
       {{ parse_json_date('$1:last_seen') }}  as last_seen,
       $1:location_id:"$oid"::string  as business_id,
       lower($1:mac::string)   as client_mac,
       current_date            as asof_date
from {{ most_recent_s3_file_name('_STAGE', 'S3_MONGO_STAGE', '.*/smbsite_mac_to_contact.json') }}
