

          select
            $1 as uploaded_sightings_id,
            $2 as sighting_id,
            $3 as offline_event_set_id,
            $4 as business_id,
            $5::timestamp as end_time,
            $6::timestamp as uploaded,
            current_timestamp() as asof_date
          FROM {{ most_recent_s3_file_name('ADS', 'ARCHIVER_ADS_S3_STAGE', '.*/nwfbcampaignsync/.*/uploaded_sightings.csv') }}

