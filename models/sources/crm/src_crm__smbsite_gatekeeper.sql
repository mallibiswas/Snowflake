SELECT $1:_id:"$oid"::STRING  AS GATEKEEPER_ID,
       $1:description::STRING AS DESCRIPTION,
       $1:name::STRING       AS NAME,
       current_date   AS ASOF_DATE
FROM {{ most_recent_s3_file_name('_STAGE', 'S3_MONGO_STAGE', '.*/smbsite_gatekeeper.json') }}
