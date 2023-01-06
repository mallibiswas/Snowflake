-------------------------------------------------------------------
----------------- MODELS_MESSAGESTATS table
-------------------------------------------------------------------

-- Create procedure to replace table after latest s3 dump
create or replace procedure ZENPROD.CRM.MODELS_MESSAGESTATS_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
snowflake.createStatement({
  sqlText:`
    create or replace transient table ZENPROD.CRM.MODELS_MESSAGESTATS as
    select  
      GET_PATH($1, '_id:$oid')::string as id,
      $1:conversions::integer as conversions,
      $1:delivered::integer as delivered,
      GET_PATH($1, 'updated:$date')::timestamp as updated,
      $1:bounced::integer as bounced,
      $1:clicked::integer as clicked,
      $1:opened::integer as opened,
      $1:cumulative_opened::variant as cumulative_opened,
      $1:cumulative_conversions::variant as cumulative_conversions,
      $1:cumulative_clicks::variant as cumulative_clicks,
      $1:clicks::variant as clicks,
      $1:click_breakdown::variant as click_breakdown,
      $1:cumulative_unsubscribes::variant as cumulatiive_unsuscribes,
      $1:soft_bounced::integer as soft_bounced,
      $1:trackable::integer as trackable,
      GET_PATH($1, 'message_id:$oid')::string as message_id,
      $1:sent::integer as sent,
      $1:unsubscribed::integer as unsubscribed,
      DATEADD(day,-1,current_date()) as asof_date
    FROM @ZENPROD.CRM.MONGO_S3_STAGE/${FILE_DATE}/models_messagestats.json;`
     }).execute();
$$;


-- Create task to call the procedure
create task ZENPROD.CRM.MODELS_MESSAGESTATS_TASK
    WAREHOUSE = ZENPROD
    SCHEDULE = 'USING CRON 0 8 * * * UTC'
as 
    CALL ZENPROD.CRM.MODELS_MESSAGESTATS_PROCEDURE(DATEADD(day, -1, CURRENT_DATE()));

alter task ZENPROD.CRM.MODELS_MESSAGESTATS_TASK resume;
