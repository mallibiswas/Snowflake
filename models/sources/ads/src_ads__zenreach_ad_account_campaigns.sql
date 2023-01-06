

          select
            $1 as ad_account_id,
            $2 as zenreach_campaign_id,
            current_timestamp() as asof_date
          FROM {{ most_recent_s3_file_name('ADS', 'ARCHIVER_ADS_S3_STAGE', '.*/nwomni/.*/ad_account_campaigns.csv') }}

