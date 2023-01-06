SELECT $1:_id:"$oid"::STRING           AS MESSAGELOG_ID,
       $1:business_id:"$oid"::STRING   AS BUSINESS_ID,
       $1:timestamp:"$date"::DATETIME AS TIMESTAMP,
       $1:zenboost_sent::INTEGER       AS ZENBOOST_SENT,
       $1:period::INTEGER              AS PERIOD,
       $1:sent::INTEGER                AS SENT,
       current_date            AS ASOF_DATE
FROM {{ most_recent_s3_file_name('_STAGE', 'S3_MONGO_STAGE', '.*/analytics_messagelogstats.json') }}
