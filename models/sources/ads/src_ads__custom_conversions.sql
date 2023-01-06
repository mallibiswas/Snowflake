

          select
            $1 as custom_conversion_id,
            $2 as account_id,
            $3 as custom_event_type,
            $4 as name,
            $5::timestamp as last_synced,
            $6::boolean as is_archived,
            $7 as offline_event_set_id,
            current_timestamp() as asof_date
          FROM {{ most_recent_s3_file_name('ADS', 'ARCHIVER_ADS_S3_STAGE', '.*/nwfbcampaignsync/.*/view_custom_conversions_snowflake.csv') }}

