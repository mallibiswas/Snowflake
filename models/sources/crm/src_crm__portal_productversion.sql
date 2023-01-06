SELECT $1:_id:"$oid"::STRING           AS PRODUCTVERSION_ID,
       $1:product_id:"$oid"::STRING    AS PRODUCT_ID,
       $1:date_added:"$date"::DATETIME AS DATE_ADDED,
       $1:name::INTEGER               AS NAME,
       $1:label::STRING                AS LABEL,
       current_date            AS ASOF_DATE
FROM {{ most_recent_s3_file_name('_STAGE', 'S3_MONGO_STAGE', '.*/portal_productversion.json') }}
