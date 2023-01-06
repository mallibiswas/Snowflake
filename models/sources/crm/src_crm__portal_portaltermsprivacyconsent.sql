SELECT $1:_id:"$oid"::STRING                   AS TERMSPRIVACYCONSENT_ID,
       $1:business_id:"$oid"::STRING           AS BUSINESS_ID,
       $1:userprofile_id:"$oid"::STRING        AS USERPROFILE_ID,
       $1:terms_privacy_bundle_version::STRING AS TERMS_PRIVACY_BUNDLE_VERSION,
       $1:created:"$date"::DATETIME            AS CREATED,
       $1:consent_time:"$date"::DATETIME       AS CONSENT_TIME,
       $1:userprofile_email::STRING            AS USERPROFILE_EMAIL,
       {{ string_to_MAC('$1:client_mac') }}    AS CLIENT_MAC,
       current_date                    AS ASOF_DATE
FROM {{ most_recent_s3_file_name('_STAGE', 'S3_MONGO_STAGE', '.*/portal_portaltermsprivacyconsent.json') }}
