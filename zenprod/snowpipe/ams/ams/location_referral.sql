-------------------------------------------------------------------
----------------- LOCATION_REFERRAL table
-------------------------------------------------------------------

-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENPROD.AMS.LOCATION_REFERRAL_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENPROD.AMS.LOCATION_REFERRAL as
          select
            $1::number as id,
            $2::timestamp as created,
            $3::timestamp as updated,
            $4::number as referrer_id,
            $5::number as location_id,
            $6 as payment_percentage,
            $7::timestamp as referred_date,
            $8::number as duration_in_months,
            $9::timestamp as payment_start_date,
            $10::timestamp as payment_end_date,
            $11::timestamp as termination_date,
            current_timestamp() as asof_date
          FROM @ZENPROD.AMS.AMS_S3_STAGE/${FILE_DATE}/locationreferral.csv;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENPROD.AMS.LOCATION_REFERRAL_TASK
    WAREHOUSE = ZENPROD
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENPROD.AMS.LOCATION_REFERRAL_PROCEDURE(CURRENT_DATE());

alter task ZENPROD.AMS.LOCATION_REFERRAL_TASK resume;