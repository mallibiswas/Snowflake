SELECT $1:_id:"$oid"::STRING     AS ENGAGEMENTLOG_ID,
       $1:sent:"$date"::DATETIME AS SENT,
       $1:device_id::STRING      AS DEVICE_ID,
       current_date      AS ASOF_DATE
FROM {{ most_recent_s3_file_name('_STAGE', 'S3_MONGO_STAGE', '.*/mobile_engagementlog.json') }}
