

          select
            $1 as uploaded_sightings_id,
            $2 as sighting_id,
            $3 as business_id,
            $4::timestamp as end_time,
            current_timestamp() as asof_date
          FROM {{ most_recent_s3_file_name('ADS', 'ARCHIVER_ADS_S3_STAGE', '.*/liveramp_measurement/.*/uploaded_sightings.csv') }}