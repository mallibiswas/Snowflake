SELECT $1:_id:"$oid"::STRING            AS ACCOUNTTUTORIALCOMPLETION_ID,
       $1:business_id:"$oid"::STRING    AS BUSINESS_ID,
       $1:userprofile_id:"$oid"::STRING AS USERPROFILE_ID,
       $1:completed:"$date"::DATETIME   AS COMPLETED,
       $1:extra_args::VARIANT           AS EXTRA_ARGS,
       $1:tutorial::STRING              AS TUTORIAL,
       current_date             AS ASOF_DATE
FROM {{ most_recent_s3_file_name('_STAGE', 'S3_MONGO_STAGE', '.*/models_accounttutorialcompletion.json') }}
