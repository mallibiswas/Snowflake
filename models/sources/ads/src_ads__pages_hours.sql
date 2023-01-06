

          select
            $1 as pages_hour_id,
            $2 as page_id,
            $3 as hour_key,
            $4 as hour_value,
            $5::timestamp as last_synced,
            current_timestamp() as asof_date
          FROM {{ most_recent_s3_file_name('ADS', 'ARCHIVER_ADS_S3_STAGE', '.*/nwfbcampaignsync/.*/pages_hours_v3.csv') }}

