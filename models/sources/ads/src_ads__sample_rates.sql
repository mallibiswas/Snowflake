

          select
            $1 as business_id, 
            $2::number as sample_rate_multiplier, 
            $3::date as day, 
            $4::timestamp_ntz as updated, 
            $5 as source, 
            current_timestamp() as asof_date
          FROM {{ most_recent_s3_file_name('ADS', 'ARCHIVER_ADS_S3_STAGE', '.*/nwsampling/.*/sample_rates.csv') }}

