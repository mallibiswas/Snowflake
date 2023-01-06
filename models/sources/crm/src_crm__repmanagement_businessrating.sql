SELECT $1:_id:"$oid"::STRING               AS REPMANAGEMENT_BUSINESSRATING_ID,
       $1:user_id:"$oid"::STRING           AS USER_ID,
       $1:business_id:"$oid"::STRING       AS BUSINESS_ID,
       $1:rating::FLOAT                    AS RATING,
       $1:created:"$date"::DATETIME        AS CREATED,
       $1:updated:"$date"::DATETIME        AS UPDATED,
       $1:rating_updated:"$date"::DATETIME AS RATING_UPDATED,
       current_date                AS ASOF_DATE
FROM {{ most_recent_s3_file_name('_STAGE', 'S3_MONGO_STAGE', '.*/repmanagement_businessrating.json') }}