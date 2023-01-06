SELECT $1:_id:"$oid"::STRING          AS ONBOARDINGWIZARDPAGECOMPLETIONS_ID,
       $1:business_id:"$oid"::STRING  AS BUSINESS_ID,
       $1:completed:"$date"::DATETIME AS COMPLETED,
       $1:page::STRING                AS PAGE,
       current_date           AS ASOF_DATE
FROM {{ most_recent_s3_file_name('_STAGE', 'S3_MONGO_STAGE', '.*/models_onboardingwizardpagecompletions.json') }}
