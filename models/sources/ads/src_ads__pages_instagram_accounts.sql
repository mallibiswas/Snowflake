

          select
            $1 as pages_instagram_accounts_id,
            $2::integer as page_id,
            $3::integer as instagram_id,
            $4 as instagram_name,
            $5::timestamp as last_synced,
            current_timestamp() as asof_date
          FROM {{ most_recent_s3_file_name('ADS', 'ARCHIVER_ADS_S3_STAGE', '.*/nwfbcampaignsync/.*/pages_instagram_accounts_v3.csv') }}

