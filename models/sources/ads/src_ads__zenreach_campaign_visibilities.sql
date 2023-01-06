

          select
            $1 as zenreach_campaign_id,
            $2::boolean as is_visible_on_dashboard,
            $3 as fb_display_attribution_window,
            $4 as LR_DISPLAY_ATTRIBUTION_WINDOW,
            current_timestamp() as asof_date
          FROM {{ most_recent_s3_file_name('ADS', 'ARCHIVER_ADS_S3_STAGE', '.*/nwomni/.*/zenreach_campaign_visibility.csv') }}

