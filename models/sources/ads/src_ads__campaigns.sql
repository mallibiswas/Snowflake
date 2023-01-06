

          select
            $1 as campaign_id,
            $2 as ad_account_id,
            $3 as name,
            $4::timestamp as start_time,
            $5::timestamp as stop_time,
            $6::timestamp as created_time,
            $7::timestamp as updated_time,
            $8 as status,
            $9 as effective_status,
            $10 as objective,
            $11::timestamp as last_synced,
            $12 as daily_spend_cents,
            $13 as lifetime_spend_cents,
            current_timestamp() as asof_date
          FROM {{ most_recent_s3_file_name('ADS', 'ARCHIVER_ADS_S3_STAGE', '.*/nwfbcampaignsync/.*/campaigns_v3.csv') }}

