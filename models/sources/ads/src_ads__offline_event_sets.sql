

          select
            $1 as offline_event_sets_id,
            $2 as best_account_id,
            split_part($3,' ',1) as business_id,
            replace(replace($3,split_part($3,' ',1)),'_',' ') as business_name,
            $4 as description,
            $5::boolean as is_auto_assign,
            $6::timestamp as creation_time,
            $7::timestamp as event_time_min,
            $8::timestamp as event_time_max,
            $9::timestamp as last_upload_time,
            parse_json($10)::variant as event_stats,
            $11::integer as valids,
            $12::integer as duplicates,
            $13::integer as matches,
            $14::integer as match_rate,
            $15 as last_synced,
            current_timestamp() as asof_date
          FROM {{ most_recent_s3_file_name('ADS', 'ARCHIVER_ADS_S3_STAGE', '.*/nwfbcampaignsync/.*/offline_event_sets_v3.csv') }}

