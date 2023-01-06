SELECT  $1                  AS id
       ,$2                  AS name
       ,$3::timestamp       AS created
       ,$4::timestamp       AS updated
       ,current_timestamp() AS asof_date
FROM {{ most_recent_s3_file_name('AMS_ACCOUNTS', 'AMS_ACCOUNTS_S3_STAGE', '.*/salesforce_account.csv') }}
