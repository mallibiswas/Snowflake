SELECT $1:_id:"$oid"::STRING          AS BILLINGANDSUBSCRIPTIONPREFS_ID,
       $1:business_id:"$oid"::STRING  AS BUSINESS_ID,
       $1:created:"$date"::DATETIME   AS CREATED,
       $1:enrichment_enabled::BOOLEAN AS ENRICHMENT_ENABLED,
       current_date           AS ASOF_DATE
FROM {{ most_recent_s3_file_name('_STAGE', 'S3_MONGO_STAGE', '.*/models_billingandsubscriptionprefs.json') }}
