

          select
            $1 as zenreach_campaign_id,
            $2::timestamp as start_time,
            $3::timestamp as end_time,
            $4 as status,
            $5::timestamp as created,
            $6::timestamp as updated,
            $7 as name,
            $8::integer as daily_budget_cents,
            $9 as creation_source,
            $10 as campaign_goal,
            $11::integer as total_budget_cents,
            $12::boolean as is_io_mappable, 
            $13 as io_reason,
            current_timestamp() as asof_date
          FROM {{ most_recent_s3_file_name('ADS', 'ARCHIVER_ADS_S3_STAGE', '.*/nwomni/.*/view_zenreach_campaigns_snowflake.csv') }}

