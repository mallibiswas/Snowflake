

          select
            $1 as id,
            $2 as campaign_id,
            $3 as advertiser_id,
            $4 as name,
            $5::timestamp as created,
            current_timestamp() as asof_date
          FROM {{ most_recent_s3_file_name('ADS', 'ARCHIVER_ADS_S3_STAGE', '.*/nwlrcampaignsync/.*/campaigns.csv') }}

