--- Procedure to replace tables
create or replace procedure ZENSTAG.ADS.SYNCED_SOURCES_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
snowflake.createStatement({
	sqlText: `
	create or replace transient table ZENSTAG.ADS.SYNCED_SOURCES as
		select
			$1::text as source,
			current_timestamp() as asof_date
		FROM @ZENSTAG.ADS.ARCHIVER_ADS_S3_STAGE/nwomni/${FILE_DATE}/synced_sources.csv;`
}).execute();
$$;
-- Create task to call the procedure (at every 45th minute)
create or replace task ZENSTAG.ADS.SYNCED_SOURCES_TASK
    WAREHOUSE = ZENSTAG
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as
    CALL ZENSTAG.ADS.SYNCED_SOURCES_PROCEDURE(CURRENT_DATE());

alter task ZENSTAG.ADS.SYNCED_SOURCES_TASK resume;
