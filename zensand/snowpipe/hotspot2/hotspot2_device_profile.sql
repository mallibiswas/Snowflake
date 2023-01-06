-------------------------------------------------------------------
-----------------  device_profile table
-------------------------------------------------------------------

-- Create procedure to replace table (need to avoid overlapping the S3 archiver)
create or replace procedure ZENSAND.HOTSPOT2.DEVICE_PROFILE_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENSAND.HOTSPOT2.DEVICE_PROFILE as
          select
            $1::string as profile_id,
	        $2::string as user_agent_id,
	        $3::string as contact_id,
	        $4::timestamp_tz as last_check_in,
	        $5::timestamp_tz as last_log_in,
	        $6::timestamp_tz as install_date,
            current_timestamp() as asof_date
          FROM @ZENSAND.HOTSPOT2.HOTSPOT2_S3_STAGE/${FILE_DATE}/device_profile.csv;`
     }).execute();
$$;


-- Create task to call the procedure (need to avoid overlapping the S3 archiver, S3 is 0, 12 hour)
create or replace task ZENSAND.HOTSPOT2.DEVICE_PROFILE_TASK
    WAREHOUSE = ZENSAND
    SCHEDULE = 'USING CRON 0 6,18 * * * UTC' -- this gives the archiver 6 hours to upload and snowflake 6 hours to download from S3
as 
    -- ensure task uses UTC time
    CALL ZENSAND.HOTSPOT2.DEVICE_PROFILE_PROCEDURE(TO_DATE(CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP())));

alter task ZENSAND.HOTSPOT2.DEVICE_PROFILE_TASK resume;
