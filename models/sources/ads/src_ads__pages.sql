

          select
            $1 as page_id,
            $2 as name,
            $3 as street,
            $4 as city,
            $5 as state,
            $6 as zip,
            $7 as country,
            $8::float as latitude,
            $9::float as longitude,
            $10::boolean as is_always_open,
            $11::timestamp as last_synced,
            current_timestamp() as asof_date
          FROM {{ most_recent_s3_file_name('ADS', 'ARCHIVER_ADS_S3_STAGE', '.*/nwfbcampaignsync/.*/pages_v3.csv') }}

