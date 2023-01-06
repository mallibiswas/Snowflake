SELECT $1:_id:"$oid"::STRING            AS BLACKLISTEDEMAIL_ID,
       $1:blast_id::STRING              AS BLAST_ID,
       $1:business_id:"$oid"::STRING    AS BUSINESS_ID,
       $1:userprofile_id:"$oid"::STRING AS USERPROFILE_ID,
       $1:long_url::STRING              AS LONG_URL,
       $1:short_url::STRING             AS SHORT_URL,
       $1:click_time:"$date"::DATETIME  AS CLICK_TIME,
       current_date             AS ASOF_DATE
FROM {{ most_recent_s3_file_name('_STAGE', 'S3_MONGO_STAGE', '.*/smbsite_clickevent.json') }}
