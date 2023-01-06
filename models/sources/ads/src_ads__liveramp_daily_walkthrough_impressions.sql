

          select
            $1 as campaign_name,
            $2 as campaign_id,
            $3 as ad_group_name,
            $4 as ad_group_id,
            $5 as creative_name,
            $6 as creative_id,
            $7 as location_id,
            $8::date as sighting_day,
            $9 as aggregate7day,
            $10 as aggregate14day,
            $11 as aggregate28day,
            $12 as unique7day,
            $13 as unique14day,
            $14 as unique28day,
            current_timestamp() as asof_date
          FROM {{ most_recent_s3_file_name('ADS', 'ARCHIVER_ADS_S3_STAGE', '.*/nwlrcampaignsync/.*/view_impressions_walkthroughs_by_day.csv') }}

