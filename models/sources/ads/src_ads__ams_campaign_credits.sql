

          select
            $1 as ams_campaign_credits_id,
            $2 as ams_campaign_id,
            $3 as reason,
            $4 as value_type,
            $5::float as value,
            $6::timestamp as start_date,
            $7::timestamp as end_date,
            $8 as notes,
            $9 as username,
            current_timestamp() as asof_date
          FROM {{ most_recent_s3_file_name('ADS', 'ARCHIVER_ADS_S3_STAGE', '.*/nwomni/.*/ams_campaign_credits.csv') }}

