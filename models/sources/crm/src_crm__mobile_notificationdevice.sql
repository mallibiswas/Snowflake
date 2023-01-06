SELECT $1:_id:"$oid"::STRING             AS NOTIFICATIONDEVICE_ID,
       $1:business_id:"$oid"::STRING     AS BUSINESS_ID,
       $1:device_id::STRING              AS DEVICE_ID,
       $1:created:"$date"::DATETIME      AS CREATED,
       $1:archived:"$date"::DATETIME     AS ARCHIVED,
       $1:last_updated:"$date"::DATETIME AS LAST_UPDATED,
       current_date              AS ASOF_DATE
FROM {{ most_recent_s3_file_name('_STAGE', 'S3_MONGO_STAGE', '.*/mobile_notificationdevice.json') }}
