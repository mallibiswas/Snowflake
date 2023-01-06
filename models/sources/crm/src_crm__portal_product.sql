SELECT $1:_id:"$oid"::STRING           AS PORTAL_PRODUCT_ID,
       $1:date_added:"$date"::DATETIME AS DATE_ADDED,
       $1:name::STRING                AS NAME,
       current_date            AS ASOF_DATE
FROM {{ most_recent_s3_file_name('_STAGE', 'S3_MONGO_STAGE', '.*/portal_product.json') }}
