SELECT $1:_id:"$oid"::STRING              AS BUSINESSRELATINSHIP_ID,
       $1:business_id:"$oid"::STRING      AS BUSINESS_ID,
       $1:created:"$date"::DATETIME       AS CREATED,
       $1:importer_id:"$oid"::STRING      AS IMPORTER_ID,
       $1:is_employee::BOOLEAN            AS IS_EMPLOYEE,
       $1:userprofile_id:"$oid"::STRING   AS USERPROFILE_ID,
       $1:login_count::INTEGER            AS LOGIN_COUNT,
       $1:last_updated:"$date"::TIMESTAMP AS LAST_UPDATED,
       $1:last_login:"$date"::TIMESTAMP   AS LAST_LOGIN,
       $1:contact_allowed::BOOLEAN        AS CONTACT_ALLOWED,
       current_date               AS ASOF_DATE
FROM {{ most_recent_s3_file_name('_STAGE', 'S3_MONGO_STAGE', '.*/portal_businessrelationship.json') }}
