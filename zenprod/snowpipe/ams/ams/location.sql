-------------------------------------------------------------------
----------------- LOCATION table
-------------------------------------------------------------------

-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENPROD.AMS.LOCATION_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENPROD.AMS.LOCATION as
          select
            $1::number as id,
            $2::timestamp as created,
            $3::timestamp as updated,
            $4::number as account_id,
            $5 as name,
            $6 as salesforce_id,
            $7 as location_state,
            $8 as address_line1,
            $9 as address_line2,
            $10 as address_zip,
            $11 as address_city,
            $12 as address_state,
            $13 as address_country,
            $14 as business_profile_id,
            $15::float as latitude,
            $16::float as longitude,
            $17 as installed,
            $18::boolean as disqualified,
            $19::number as billing_account_id,
            $20 as business_unit,
            $21::number as billing_account_v2_id,
            current_timestamp() as asof_date
          FROM @ZENPROD.AMS.AMS_S3_STAGE/${FILE_DATE}/location.csv;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENPROD.AMS.LOCATION_TASK
    WAREHOUSE = ZENPROD
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENPROD.AMS.LOCATION_PROCEDURE(CURRENT_DATE());

alter task ZENPROD.AMS.LOCATION_TASK resume;