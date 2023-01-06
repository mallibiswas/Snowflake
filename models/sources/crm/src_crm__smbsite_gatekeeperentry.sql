SELECT $1:_id:"$oid"::STRING           AS GATEKEEPERENTRY_ID,
       $1:business_id:"$oid"::STRING   AS BUSINESS_ID,
       $1:gatekeeper_id:"$oid"::STRING AS GATEKEEPR_ID,
       $1:created:"$date"::DATETIME    AS CREATED,
       $1:removed:"$date"::DATETIME    AS REMOVED,
       current_date            AS ASOF_DATE
FROM {{ most_recent_s3_file_name('_STAGE', 'S3_MONGO_STAGE', '.*/smbsite_gatekeeperentry.json') }}
