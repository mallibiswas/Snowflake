SELECT $1:_id:"$oid"::STRING           AS CLICKLOG_ID,
       $1:message_id:"$oid"::STRING    AS MESSAGE_ID,
       $1:messagelog_id:"$oid"::STRING AS MESSAGELOG_ID,
       $1:timestamp:"$date"::DATETIME AS TIMESTAMP,
       $1:client_os::STRING            AS CLIENT_OS,
       $1:client_type::STRING          AS CLIENT_TYPE,
       $1:device_type::STRING          AS DEVICE_TYPE,
       $1:link_class::STRING           AS LINK_CLASS,
       $1:url::STRING                 AS URL,
       $1:user_agent::STRING           AS USER_AGENT,
       current_date            AS ASOF_DATE
FROM {{ most_recent_s3_file_name('_STAGE', 'S3_MONGO_STAGE', '.*/smbsite_clicklog.json') }}
