select $1:_id:"$oid"::string                   as offer_id,
       $1:business_id:"$oid"::string          as business_id,
       $1:email_blast_id:"$oid"::string       as email_blast_id,
       {{ parse_json_date('$1:expiration') }}         as expiration,
       {{ parse_json_date('$1:future_expiration') }}  as future_expiration,
       $1:logo_id::string              as logo_id,
       $1:require_mac::string          as require_mac,
       $1:test::string                 as free_hug,
       $1:title::string                as title,
       $1:trigger_id:"$oid"::string           as trigger_id,
       current_date                    as asof_date
from {{ most_recent_s3_file_name('_STAGE', 'S3_MONGO_STAGE', '.*/smbsite_offer.json') }}
