-------------------------------------------------------------------
----------------- MODELS_MESSAGESTATS table
-------------------------------------------------------------------

-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENSTAG.CRM.MODELS_MESSAGESTATS_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
snowflake.createStatement({
  sqlText:`
    create or replace transient table ZENSTAG.CRM.MODELS_MESSAGESTATS as
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
      current_timestamp() as asof_date
    FROM @ZENSTAG.CRM.MONGO_S3_STAGE/${FILE_DATE}/models_messagestats.json;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENSTAG.CRM.MODELS_MESSAGESTATS_TASK
    WAREHOUSE = ZENSTAG
    SCHEDULE = 'USING CRON 0 20 * * * UTC'
as 
    CALL ZENSTAG.CRM.MODELS_MESSAGESTATS_PROCEDURE(CURRENT_DATE());

alter task ZENSTAG.CRM.MODELS_MESSAGESTATS_TASK resume;
