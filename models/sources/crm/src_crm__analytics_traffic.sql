SELECT $1:_id:"$oid"::STRING           AS ANALYTICS_TRAFFIC_ID,
       $1:business_id:"$oid"::STRING   AS BUSINESS_ID,
       $1:updated:"$date"::DATETIME    AS UPDATED,
       $1:timestamp:"$date"::DATETIME AS TIMESTAMP,
       $1:avg_visit_duration::INTEGER  AS AVG_VISIT_DURATION,
       $1:period::INTEGER              AS PERIOD,
       $1:visitors::INTEGER            AS VISITORS,
       $1:new_visitors::INTEGER        AS NEW_VISITORS,
       $1:repeat_visitors::INTEGER     AS REPEAT_VISITORS,
       $1:passersby::INTEGER           AS PASSERSBY,
       $1:converted_visitors::INTEGER  AS CONVERTED_VISITORS,
       current_date            AS ASOF_DATE
FROM {{ most_recent_s3_file_name('_STAGE', 'S3_MONGO_STAGE', '.*/analytics_traffic.json') }}
