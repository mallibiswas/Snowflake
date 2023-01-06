SELECT $1:_id:"$oid"::STRING         AS ACTIVITYLOG_ID,
       $1:business_id:"$oid"::STRING AS BUSINESS_ID,
       $1:user_id:"$oid"::STRING     AS USER_ID,
       $1:activity::STRING           AS ACTIVITY,
       $1:is_staff::BOOLEAN          AS IS_STAFF,
       $1:created:"$date"::DATETIME  AS CREATED,
       $1:latest:"$date"::DATETIME   AS LATEST,
       current_date          AS ASOF_DATE
FROM {{ most_recent_s3_file_name('_STAGE', 'S3_MONGO_STAGE', '.*/smbsite_activitylog.json') }}
