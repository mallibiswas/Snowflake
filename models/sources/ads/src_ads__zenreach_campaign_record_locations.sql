

          select
            $1 as zenreach_campaign_records_id,
            $2 as location_id,
            current_timestamp() as asof_date
          FROM {{ most_recent_s3_file_name('ADS', 'ARCHIVER_ADS_S3_STAGE', '.*/nwomni/.*/zenreach_campaign_records_locations.csv') }}

