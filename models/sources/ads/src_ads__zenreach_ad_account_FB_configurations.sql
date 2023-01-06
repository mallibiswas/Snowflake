

          select
            $1 as ad_account_id,
            $2 as facebook_ad_account_id,
            $3 as offline_event_set_id,
            $4 as facebook_page_id,
            $5 as default_attribution_window,
            current_timestamp() as asof_date
          FROM {{ most_recent_s3_file_name('ADS', 'ARCHIVER_ADS_S3_STAGE', '.*/nwomni/.*/ad_account_fb_config.csv') }}

