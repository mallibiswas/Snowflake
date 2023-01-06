

          select
            $1 as insights_custom_id,
            $2 as custom_conv_id,
            $3 as campaign_id,
            $4::integer as walkthroughs,
            $5 as insight_type,
            $6 as breakdown_type,
            $7 as breakdown_value,
            $8::timestamp as last_synced,
            $9::integer as walkthroughs1_day,
            $10::integer as walkthroughs7_day,
            $11::integer as walkthroughs28_day,
            current_timestamp() as asof_date
          FROM {{ most_recent_s3_file_name('ADS', 'ARCHIVER_ADS_S3_STAGE', '.*/nwfbcampaignsync/.*/insights_custom_v3.csv') }}

