

          select
            $1 as uploaded_sightings_id,
            $2 as field,
            $3 as value,
            current_timestamp() as asof_date
          FROM {{ most_recent_s3_file_name('ADS', 'ARCHIVER_ADS_S3_STAGE', '.*/nwfbcampaignsync/.*/uploaded_sightings_custom_data.csv') }}

