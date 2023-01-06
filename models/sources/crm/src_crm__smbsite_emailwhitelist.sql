SELECT $1:_id:"$oid"::STRING AS EMAILWHITELIST_ID,
       $1:email::STRING     AS EMAIL,
       current_date  AS ASOF_DATE
FROM {{ most_recent_s3_file_name('_STAGE', 'S3_MONGO_STAGE', '.*/smbsite_emailwhitelist.json') }}
