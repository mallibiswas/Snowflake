SELECT $1:_id:"$oid"::STRING         AS BUSINESSBRANDING_ID,
       $1:business_id:"$oid"::STRING AS BUSINESS_ID,
       $1:created:"$date"::DATETIME  AS CREATED,
       $1:button_color::STRING       AS BUTTON_COLOR,
       $1:font::STRING               AS FONT,
       $1:logo_id::STRING            AS LOGO_ID,
       $1:tone::STRING               AS TONE,
       current_date          AS ASOF_DATE
FROM {{ most_recent_s3_file_name('_STAGE', 'S3_MONGO_STAGE', '.*/models_businessbranding.json') }}
