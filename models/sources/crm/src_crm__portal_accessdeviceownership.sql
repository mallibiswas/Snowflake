SELECT $1:_id:"$oid"::STRING               AS ACCESSDEVICEOWNERSHIP_ID,
       $1:accessdevice_id:"$oid"::STRING   AS ACCESSDEVICE_ID,
       $1:created:"$date"::DATETIME        AS CREATED,
       $1:last_confirmed:"$date"::DATETIME AS LAST_CONFIRMED,
       $1:userprofile_id:"$oid"::STRING    AS USERPROFILE_ID,
       current_date                AS ASOF_DATE
FROM {{ most_recent_s3_file_name('_STAGE', 'S3_MONGO_STAGE', '.*/portal_accessdeviceownership.json') }}
