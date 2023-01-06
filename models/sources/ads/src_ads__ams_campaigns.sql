

          select
            $1 as zenreach_campaign_id,
            $2 as ads_io_id,
            $3::timestamp as updated,
            $4 as mapped_by,
            current_timestamp() as asof_date
          FROM {{ most_recent_s3_file_name('ADS', 'ARCHIVER_ADS_S3_STAGE', '.*/nwomni/.*/ams_campaign.csv') }}

