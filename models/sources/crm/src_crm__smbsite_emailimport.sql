SELECT $1:_id:"$oid"::STRING            AS EMAILIMPORT_ID,
       $1:business_id:"$oid"::STRING    AS BUSINESS_ID,
       $1:created:"$date"::DATETIME     AS CREATED,
       $1:error::STRING                 AS ERROR,
       $1:file                         AS FILE,
       $1:uploaded_by_id:"$oid"::STRING AS UPLOADED_BY_ID,
       current_date             AS ASOF_DATE
FROM {{ most_recent_s3_file_name('_STAGE', 'S3_MONGO_STAGE', '.*/smbsite_emailimport.json') }}
