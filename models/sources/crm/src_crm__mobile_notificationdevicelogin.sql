SELECT $1:_id:"$oid"::STRING           AS NOTIFICATIONDEVICELOGIN_ID,
       $1:device_id::STRING            AS DEVICE_ID,
       $1:archived:"$date"::DATETIME   AS ARCHIVED,
       $1:last_login:"$date"::DATETIME AS LAST_LOGIN,
       current_date            AS ASOF_DATE
FROM {{ most_recent_s3_file_name('_STAGE', 'S3_MONGO_STAGE', '.*/mobile_notificationdevicelogin.json') }}
