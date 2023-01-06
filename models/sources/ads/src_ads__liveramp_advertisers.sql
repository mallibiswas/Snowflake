

          select
            $1 as id,
            $2 as advertiser_id,
            $3 as name,
            $4::timestamp as created,
            current_timestamp() as asof_date
          FROM {{ most_recent_s3_file_name('ADS', 'ARCHIVER_ADS_S3_STAGE', '.*/nwlrcampaignsync/.*/advertisers.csv') }}

