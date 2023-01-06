

          select
            $1 as message_id,
            $2 as url,
            $3::timestamp as processed,
            current_timestamp() as asof_date
          FROM {{ most_recent_s3_file_name('ADS', 'ARCHIVER_ADS_S3_STAGE', '.*/nwlrcampaignsync/.*/processed_exports.csv') }}

