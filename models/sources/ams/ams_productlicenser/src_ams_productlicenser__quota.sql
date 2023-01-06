SELECT  $1                  AS id
       ,$2::number          AS soft_limit
       ,$3::number          AS hard_limit
       ,$4::float           AS unit_penalty_in_cents
       ,$5                  AS quota_type_id
       ,$6::timestamp       AS created
       ,current_timestamp() AS asof_date
FROM {{ most_recent_s3_file_name('AMS_PRODUCTLICENSER', 'AMS_PRODUCTLICENSER_S3_STAGE', '.*quota.csv') }}