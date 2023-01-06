SELECT $1:_id:"$oid"::STRING   AS BLACKLISTEDEMAIL_ID,
       $1:email::STRING       AS EMAIL,
       $1:occurrences::INTEGER AS OCCURRENCES,
       current_date    AS ASOF_DATE
FROM  {{ most_recent_s3_file_name('_STAGE', 'S3_MONGO_STAGE', '.*/smbsite_blacklistedemail.json') }}