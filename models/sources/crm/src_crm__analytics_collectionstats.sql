SELECT $1:_id:"$oid"::STRING                AS COLLECTION_ID,
       $1:business_id:"$oid"::STRING        AS BUSINESS_ID,
       $1:date:"$date"::DATETIME           AS DATE,
       $1:phones::INTEGER                   AS PHONES,
       $1:valid_emails::INTEGER             AS VALID_EMAILS,
       $1:mailgun_corrected_emails::INTEGER AS MAILGUN_CORRECTED_EMAILS,
       $1:wifast_corrected_emails::INTEGER  AS WIFAST_CORRECTED_EMAILS,
       $1:likes::INTEGER                    AS LIKES,
       $1:follows::INTEGER                  AS FOLLOWS,
       $1:invalid_emails::INTEGER           AS INVALID_EMAILS,
       $1:zenboost_network::INTEGER         AS ZENBOOST_NETWORK,
       $1:emails::INTEGER                   AS EMAILS,
       current_date                 AS ASOF_DATE
FROM {{ most_recent_s3_file_name('_STAGE', 'S3_MONGO_STAGE', '.*/analytics_collectionstats.json') }}
