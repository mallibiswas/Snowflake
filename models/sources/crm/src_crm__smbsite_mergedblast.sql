select $1:_id:"$oid"::string             as mergedblast_id,
       $1:business_id:"$oid"::string    as business_id,
       {{ parse_json_date('$1:created') }}      as created,
       {{ parse_json_date('$1:deleted') }}      as deleted,
       $1:draft::boolean         as draft,
       $1:email_blast_id:"$oid"::string as email_blast_id,
       {{ parse_json_date('$1:scheduled') }}    as scheduled,
       $1:sms_blast_id::string   as sms_blast_id,
       {{ parse_json_integer('$1:sort_key') }}      as sort_key,
       $1:target::variant        as target,
       current_date              as asof_date
from {{ most_recent_s3_file_name('_STAGE', 'S3_MONGO_STAGE', '.*/smbsite_mergedblast.json') }}
