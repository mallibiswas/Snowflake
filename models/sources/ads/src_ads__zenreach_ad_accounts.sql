

          select
            $1 as ad_account_id,
            $2 as name,
            $3 as platform,
            $4 as account_id,
            $5::boolean as upload_enabled,
            current_timestamp() as asof_date
          FROM {{ most_recent_s3_file_name('ADS', 'ARCHIVER_ADS_S3_STAGE', '.*/nwomni/.*/ad_accounts.csv') }}

