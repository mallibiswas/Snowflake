--- Procedure to replace tables
create or replace procedure ZENSTAG.ADS.LR_ATTRIBUTION_WINDOWS_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
snowflake.createStatement({
	sqlText: `
	create or replace transient table ZENSTAG.ADS.LR_ATTRIBUTION_WINDOWS as
		select
			$1::text as days,
			current_timestamp() as asof_date
		FROM @ZENSTAG.ADS.ARCHIVER_ADS_S3_STAGE/nwomni/${FILE_DATE}/lr_attribution_windows.csv;`
}).execute();
$$;
-- Create task to call the procedure (at every 45th minute)
create or replace task ZENSTAG.ADS.LR_ATTRIBUTION_WINDOWS_TASK
    WAREHOUSE = ZENSTAG
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as
    CALL ZENSTAG.ADS.LR_ATTRIBUTION_WINDOWS_PROCEDURE(CURRENT_DATE());

alter task ZENSTAG.ADS.LR_ATTRIBUTION_WINDOWS_TASK resume;
