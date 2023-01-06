--- Procedure to replace tables
create or replace procedure ZENPROD.ADS.SYNC_EVENTS_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
snowflake.createStatement({
	sqlText: `
	create or replace transient table ZENPROD.ADS.SYNC_EVENTS as
		select
			$1 as id,
			$2::timestamp as timestamp,
			$3::text as payload,
			$4::text as synced_by,
			current_timestamp() as asof_date
		FROM @ZENPROD.ADS.ARCHIVER_ADS_S3_STAGE/nwomni/${FILE_DATE}/sync_events.csv;`
}).execute();
$$;
-- Create task to call the procedure (at every 45th minute)
create or replace task ZENPROD.ADS.SYNC_EVENTS_TASK
    WAREHOUSE = ZENPROD
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as
    CALL ZENPROD.ADS.SYNC_EVENTS_PROCEDURE(CURRENT_DATE());

alter task ZENPROD.ADS.SYNC_EVENTS_TASK resume;
