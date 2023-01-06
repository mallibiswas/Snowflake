

          select
            $1 as creative_id,
            $2 as message_id,
            $3::date as date,
            $4 as device_type,
            $5 as media_type,
            $6 as impressions,
            $7 as clicks,
            $8 as cost_cents,
            $9 as impression_uniques,
            $10 as player_starts,
            $11 as player25perc_complete,
            $12 as player50perc_complete,
            $13 as player75perc_complete,
            $14 as player_completed_views,
            $15 as sampled_tracked_impressions,
            $16 as sampled_viewed_impressions,
            current_timestamp() as asof_date
          FROM {{ most_recent_s3_file_name('ADS', 'ARCHIVER_ADS_S3_STAGE', '.*/nwlrcampaignsync/.*/insights_creatives.csv') }}

