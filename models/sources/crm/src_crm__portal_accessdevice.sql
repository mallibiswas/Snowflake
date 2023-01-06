SELECT $1:_id:"$oid"::STRING           AS ACCESSDEVICE_ID,
       $1:cookie_key::STRING           AS COOKIE_KEY,
       $1:date_added:"$date"::DATETIME AS DATE_ADDED,
       $1:last_seen:"$date"::DATETIME  AS LAST_SEEN,
       $1:last_seen_ip::STRING         AS LAST_SEEN_IP,
       lower($1:mac::STRING)           AS MAC,
       current_date            AS ASOF_DATE
FROM {{ most_recent_s3_file_name('_STAGE', 'S3_MONGO_STAGE', '.*/portal_accessdevice.json') }}
