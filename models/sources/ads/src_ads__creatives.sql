

          select
            $1 as ad_creative_id,
            $2 as ad_account_id,
            $3 as name,
            $4 as page_id,
            $5 as object_type,
            $6 as object_permalink_url,
            $7 as instagram_actor_id,
            $8 as instagram_permalink_url,
            $9 as video_id,
            $10 as status,
            $11 as title,
            $12 as body,
            $13 as thumbnail,
            $14 as image,
            $15 as call_to_action_type,
            $16 as link_description,
            $17 as link,
            $18::timestamp as last_synced,
            current_timestamp() as asof_date
          FROM {{ most_recent_s3_file_name('ADS', 'ARCHIVER_ADS_S3_STAGE', '.*/nwfbcampaignsync/.*/ad_creatives_v3.csv') }}

