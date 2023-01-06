SELECT $1:_id:"$oid"::STRING        AS DEVICEREGISTRATIONLOG_ID,
       $1:created:"$date"::DATETIME AS CREATED,
       $1:device_id::STRING         AS DEVICE_ID,
       $1:login_email::STRING       AS LOGIN_EMAIL,
       $1:push_id::STRING           AS PUSH_ID,
       current_date         AS ASOF_DATE
FROM {{ most_recent_s3_file_name('_STAGE', 'S3_MONGO_STAGE', '.*/mobile_deviceregistrationlog.json') }}
