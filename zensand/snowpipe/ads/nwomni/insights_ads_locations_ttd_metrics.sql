--- Procedure to replace tables
create or replace procedure ZENSAND.ADS.INSIGHTS_ADS_LOCATIONS_TTD_METRICS_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
snowflake.createStatement({
	sqlText: `
	create or replace transient table ZENSAND.ADS.INSIGHTS_ADS_LOCATIONS_TTD_METRICS as
		select
			$1 as id,
			$2 as insights_ads_locations_id,
			$3::text as media_type,
			$4::bigint as cost_cents,
			$5::integer as margin,
			$6::bigint as confirmed_walkthroughs,
			$7::bigint as confirmed_walkthroughs7_day,
			$8::bigint as confirmed_walkthroughs14_day,
			$9::bigint as confirmed_walkthroughs28_day,
			$10::real as sample_rate_multiplier,
			$11::bigint as calculated_walkthroughs,
			$12::bigint as calculated_walkthroughs7_day,
			$13::bigint as calculated_walkthroughs14_day,
			$14::bigint as calculated_walkthroughs28_day,
			current_timestamp() as asof_date
		FROM @ZENSAND.ADS.ARCHIVER_ADS_S3_STAGE/nwomni/${FILE_DATE}/insights_ads_locations_ttd_metrics.csv;`
}).execute();
$$;
-- Create task to call the procedure (at every 45th minute)
create or replace task ZENSAND.ADS.INSIGHTS_ADS_LOCATIONS_TTD_METRICS_TASK
    WAREHOUSE = ZENSAND
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as
    CALL ZENSAND.ADS.INSIGHTS_ADS_LOCATIONS_TTD_METRICS_PROCEDURE(CURRENT_DATE());

alter task ZENSAND.ADS.INSIGHTS_ADS_LOCATIONS_TTD_METRICS_TASK resume;
