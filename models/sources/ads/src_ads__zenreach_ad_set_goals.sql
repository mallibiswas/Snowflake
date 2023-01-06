

          select
            $1 as zenreach_campaign_records_id,
            $2 as ad_set_id,
            $3 as goal,
            current_timestamp() as asof_date
          FROM {{ most_recent_s3_file_name('ADS', 'ARCHIVER_ADS_S3_STAGE', '.*/nwomni/.*/zenreach_ad_set_goals.csv') }}

