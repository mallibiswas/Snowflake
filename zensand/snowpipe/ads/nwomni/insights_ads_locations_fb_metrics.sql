--- Procedure to replace tables
create or replace procedure ZENSAND.ADS.INSIGHTS_ADS_LOCATIONS_FB_METRICS_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
snowflake.createStatement({
	sqlText: `
	create or replace transient table ZENSAND.ADS.INSIGHTS_ADS_LOCATIONS_FB_METRICS as
		select
			$1 as id,
			$2 as insights_ads_locations_id,
			$3::integer as margin,
			$4::bigint as confirmed_walkthroughs,
			$5::bigint as confirmed_walkthroughs1_day,
			$6::bigint as confirmed_walkthroughs7_day,
			$7::bigint as confirmed_walkthroughs28_day,
			$8::real as sample_rate_multiplier,
			$9::bigint as calculated_walkthroughs,
			$10::bigint as calculated_walkthroughs1_day,
			$11::bigint as calculated_walkthroughs7_day,
			$12::bigint as calculated_walkthroughs28_day,
			$13::bigint as cpwt1_day,
			$14::bigint as cpwt7_day,
			$15::bigint as cpwt28_day,
			current_timestamp() as asof_date
		FROM @ZENSAND.ADS.ARCHIVER_ADS_S3_STAGE/nwomni/${FILE_DATE}/insights_ads_locations_fb_metrics.csv;`
}).execute();
$$;
-- Create task to call the procedure (at every 45th minute)
create or replace task ZENSAND.ADS.INSIGHTS_ADS_LOCATIONS_FB_METRICS_TASK
    WAREHOUSE = ZENSAND
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as
    CALL ZENSAND.ADS.INSIGHTS_ADS_LOCATIONS_FB_METRICS_PROCEDURE(CURRENT_DATE());

alter task ZENSAND.ADS.INSIGHTS_ADS_LOCATIONS_FB_METRICS_TASK resume;
