SELECT      $1 as id,
            $2 as license_id,
            $3 as business_id,
            $4::timestamp as created,
            $5::timestamp as updated,
            $6::timestamp as deleted,
            current_timestamp() as asof_date
FROM {{ most_recent_s3_file_name('AMS_PRODUCTLICENSER', 'AMS_PRODUCTLICENSER_S3_STAGE', '.*license_assignment.csv') }}