

          select
            $1 as zenreach_campaign_records_id,
            $2 as zenreach_campaign_id,
            $3 as campaign_id,
            $4 as creation_status,
            $5 as platform,
            $6 as platform_account_id,
            $7::integer as daily_budget_cents,
            $8::integer as total_budget_cents,
            current_timestamp() as asof_date
          FROM {{ most_recent_s3_file_name('ADS', 'ARCHIVER_ADS_S3_STAGE', '.*/nwomni/.*/zenreach_campaign_records.csv') }}

