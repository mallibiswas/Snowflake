

          select
            $1 as ad_id,
            $2 as ad_account_id,
            $3 as campaign_id,
            $4 as ad_set_id,
            $5 as ad_creative_id,
            $6 as name,
            $7::timestamp as created_time,
            $8::timestamp as updated_time,
            $9 as status,
            $10 as effective_status,
            $11::timestamp as last_synced,
            current_timestamp() as asof_date
          FROM {{ most_recent_s3_file_name('ADS', 'ARCHIVER_ADS_S3_STAGE', '.*/nwfbcampaignsync/.*/ads_v3.csv') }}

