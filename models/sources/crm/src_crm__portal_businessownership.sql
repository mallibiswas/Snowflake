SELECT $1:_id:"$oid"::STRING            AS BUSINESSOWNERSHIP_ID,
       $1:business_id:"$oid"::STRING    AS BUSINESS_ID,
       $1:userprofile_id:"$oid"::STRING AS USERPROFILE_ID,
       $1:create:"$date"::DATETIME      AS CREATED,
       $1:updated:"$date"::DATETIME     AS UPDATED,
       $1:role_ids::VARIANT             AS ROLE_IDS,
       current_date             AS ASOF_DATE
FROM {{ most_recent_s3_file_name('_STAGE', 'S3_MONGO_STAGE', '.*/portal_businessownership.json') }}
