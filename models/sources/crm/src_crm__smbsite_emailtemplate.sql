SELECT $1:_id:"$oid"::STRING          AS EMAILTEMPLATE_ID,
       $1:business_id:"$oid"::STRING  AS BUSINESS_ID,
       $1:created:"$date"::DATETIME   AS CREATED,
       $1:complete::BOOLEAN           AS COMPLETE,
       try_parse_json($1:layout)::VARIANT AS LAYOUT,
       $1:subject::STRING             AS SUBJECT,
       $1:updated:"$date"::DATETIME   AS UPDATED,
       current_date           AS ASOF_DATE
FROM {{ most_recent_s3_file_name('_STAGE', 'S3_MONGO_STAGE', '.*/smbsite_emailtemplate.json') }}
