SELECT $1:_id:"$oid"::STRING        AS GDPR_CONTACT_BLACKLIST_ID,
       $1:contact_id:"$oid"::STRING AS CONTACT_ID,
       $1:account_id:"$oid"::STRING AS ACCOUNT_ID,
       current_date         AS ASOF_DATE
FROM {{ most_recent_s3_file_name('_STAGE', 'S3_MONGO_STAGE', '.*/gdpr_contact_blacklist.json') }}
