--- Procedure to replace tables
create or replace procedure ZENSTAG.ADS.INSIGHTS_CAMPAIGNS_LOCATIONS_TTD_METRICS_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
snowflake.createStatement({
	sqlText: `
	create or replace transient table ZENSTAG.ADS.INSIGHTS_CAMPAIGNS_LOCATIONS_TTD_METRICS as
		select
			$1 as id,
			$2 as insights_campaigns_locations_id,
			$3::integer as attribution_window,
			$4::bigint as confirmed_walkthroughs,
			$5::bigint as confirmed_walkthroughs7_day,
			$6::bigint as confirmed_walkthroughs14_day,
			$7::bigint as confirmed_walkthroughs28_day,
			$8::real as sample_rate_multiplier,
			$9::bigint as calculated_walkthroughs,
			$10::bigint as calculated_walkthroughs7_day,
			$11::bigint as calculated_walkthroughs14_day,
			$12::bigint as calculated_walkthroughs28_day,
			current_timestamp() as asof_date
		FROM @ZENSTAG.ADS.ARCHIVER_ADS_S3_STAGE/nwomni/${FILE_DATE}/insights_campaigns_locations_ttd_metrics.csv;`
}).execute();
$$;
-- Create task to call the procedure (at every 45th minute)
create or replace task ZENSTAG.ADS.INSIGHTS_CAMPAIGNS_LOCATIONS_TTD_METRICS_TASK
    WAREHOUSE = ZENSTAG
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as
    CALL ZENSTAG.ADS.INSIGHTS_CAMPAIGNS_LOCATIONS_TTD_METRICS_PROCEDURE(CURRENT_DATE());

alter task ZENSTAG.ADS.INSIGHTS_CAMPAIGNS_LOCATIONS_TTD_METRICS_TASK resume;
