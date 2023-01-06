SELECT $1:_id:"$oid"::STRING                AS PORTAL_TOSCONSENT_ID,
       $1:business_id:"$oid"::STRING        AS BUSINESS_ID,
       $1:userprofile_id:"$oid"::STRING     AS USERPROFILE_ID,
       {{ string_to_MAC('$1:client_mac') }} AS CLIENT_MAC,
       $1:created:"$date"::DATETIME         AS CREATED,
       current_date                 AS ASOF_DATE
FROM {{ most_recent_s3_file_name('_STAGE', 'S3_MONGO_STAGE', '.*/portal_tosconsent.json') }}