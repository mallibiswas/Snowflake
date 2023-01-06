SELECT  $1                  AS id
       ,$2                  AS parent_id
       ,$3                  AS name
       ,$4                  AS account_id
       ,$5                  AS crm_id
       ,$6                  AS salesforce_id
       ,$7                  AS type
       ,$8::timestamp       AS created
       ,$9::timestamp       AS updated
       ,current_timestamp() AS asof_date
FROM {{ most_recent_s3_file_name('AMS_PRODUCTLICENSER', 'AMS_PRODUCTLICENSER_S3_STAGE', '.*business_entity.csv') }}