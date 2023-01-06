

          select
            $1 as insight_id,
            $2 as ad_account_id,
            $3 as campaign_id,
            $4 as insight_type,
            $5 as breakdown_type,
            $6 as breakdown_value,
            $7::integer as impressions,
            $8::integer as clicks,
            $9::integer as walkthroughs,
            $10 as engagement,
            $11::integer as spend_cents,
            $12::timestamp as last_synced,
            $13::integer as walkthroughs1_day,
            $14::integer as walkthroughs7_day,
            $15::integer as walkthroughs28_day,
            current_timestamp() as asof_date
          FROM {{ most_recent_s3_file_name('ADS', 'ARCHIVER_ADS_S3_STAGE', '.*/nwfbcampaignsync/.*/insights_v3.csv') }}

