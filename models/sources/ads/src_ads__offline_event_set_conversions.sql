

          select
            $1 as offline_event_set_id,
            $2 as location_id,
            $3 as custom_conversion_id,
            current_timestamp() as asof_date
          FROM {{ most_recent_s3_file_name('ADS', 'ARCHIVER_ADS_S3_STAGE', '.*/nwomni/.*/offline_event_set_conversions.csv') }}

