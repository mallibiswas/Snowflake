

          select
            $1 as page_vertical_id,
            $2::integer as page_id,
            $3::integer as vertical_id,
            $4 as vertical_name,
            $5::timestamp as last_synced,
            current_timestamp() as asof_date
          FROM {{ most_recent_s3_file_name('ADS', 'ARCHIVER_ADS_S3_STAGE', '.*/nwfbcampaignsync/.*/pages_verticals_v3.csv') }}

