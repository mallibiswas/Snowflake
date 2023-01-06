SELECT $1:_id:"$oid"::STRING           AS ANALYTICS_AGGREGATESTATS_ID,
       $1:business_id:"$oid"::STRING   AS BUSINESS_ID,
       $1:updated:"$date"::DATETIME    AS UPDATED,
       $1:created:"$date"::DATETIME    AS CREATED,
       $1:timestamp:"$date"::DATETIME AS TIMESTAMP,
       $1:reach::INTEGER               AS REACH,
       $1:period::INTEGER              AS PERIOD,
       current_date            AS ASOF_DATE
FROM {{ most_recent_s3_file_name('_STAGE', 'S3_MONGO_STAGE', '.*/analytics_aggregatestats.json') }}
