

          select
            $1 as ad_account_id,
            $2 as name,
            $3::boolean as is_zenreach,
            $4::timestamp as created_time,
            $5::timestamp as last_synced,
            current_timestamp() as asof_date
          FROM {{ most_recent_s3_file_name('ADS', 'ARCHIVER_ADS_S3_STAGE', '.*/nwfbcampaignsync/.*/ad_accounts_v3.csv') }}
