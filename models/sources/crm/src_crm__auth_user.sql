SELECT $1:_id:"$oid"::STRING            AS AUTH_USER_ID,
       $1:email::STRING                AS EMAIL,
       $1:date_joined:"$date"::DATETIME AS DATE_JOINED,
       $1:username::STRING              AS USERNAME,
       $1:is_active::BOOLEAN            AS IS_ACTIVE,
       $1:last_login:"$date"::DATETIME  AS LAST_LOGIN,
       current_date             AS ASOF_DATE
FROM {{ most_recent_s3_file_name('_STAGE', 'S3_MONGO_STAGE', '.*/auth_user.json') }}
