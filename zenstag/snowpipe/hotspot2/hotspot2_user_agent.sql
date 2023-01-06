-------------------------------------------------------------------
-----------------  user_agent table
-------------------------------------------------------------------

-- Create procedure to replace table (need to avoid overlapping the S3 archiver)
create or replace procedure ZENSTAG.HOTSPOT2.USER_AGENT_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENSTAG.HOTSPOT2.USER_AGENT as
          select
            $1::string as id,
	        $2::string as os,
	        $3::string as device_type,
	        $4::string as manufacturer,
	        $5::string as software_version,
	        $6::string as hardware_version,
            current_timestamp() as asof_date
          FROM @ZENSTAG.HOTSPOT2.HOTSPOT2_S3_STAGE/${FILE_DATE}/user_agent.csv;`
     }).execute();
$$;


-- Create task to call the procedure (need to avoid overlapping the S3 archiver, S3 is 0, 12 hour)
create or replace task ZENSTAG.HOTSPOT2.USER_AGENT_TASK
    WAREHOUSE = ZENSTAG
    SCHEDULE = 'USING CRON 0 6,18 * * * UTC' -- this gives the archiver 6 hours to upload and snowflake 6 hours to download from S3
as 
    -- ensure task uses UTC time
    CALL ZENSTAG.HOTSPOT2.USER_AGENT_PROCEDURE(TO_DATE(CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP())));

alter task ZENSTAG.HOTSPOT2.USER_AGENT_TASK resume;
