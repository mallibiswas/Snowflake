

          select
            $1 as campaign_id,
            $2::float as margin_percent,
            $3::timestamp as updated,
            $4 as zenreach_campaign_records_id,
            current_timestamp() as asof_date
          FROM {{ most_recent_s3_file_name('ADS', 'ARCHIVER_ADS_S3_STAGE', '.*/nwomni/.*/view_zenreach_campaign_records_margins.csv') }}

